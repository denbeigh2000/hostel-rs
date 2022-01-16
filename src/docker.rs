use std::collections::HashMap;
use std::convert::{Infallible, TryInto};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bollard::container::LogOutput;
use bollard::exec::{CreateExecOptions, ResizeExecOptions, StartExecOptions, StartExecResults};
use bollard::Docker;
use futures::future::{ready, FutureExt, Ready};
use futures::{Stream, StreamExt};
use thiserror::Error;
use thrussh::server::{Auth, Handler, Response, Server, Session};
use thrussh::{ChannelId, CryptoVec};
use tokio::io::{AsyncWrite, AsyncWriteExt};

static DEST_CONTAINER: &str = "a0b95114e3b8";

type LogStream = Pin<Box<dyn Stream<Item = Result<LogOutput, bollard::errors::Error>> + Send>>;
type ContainerSink = Pin<Box<dyn AsyncWrite + Send>>;

pub struct SSHSession {
    pub state: ConnectionState,
    pub pty_data: Option<PtyData>,
}

impl SSHSession {
    pub fn set_pty_data(&mut self, data: PtyData) -> Result<(), Infallible> {
        if self.pty_data.is_some() {
            panic!("pty data already set");
        }

        self.pty_data = Some(data);
        Ok(())
    }

    pub fn env(&self) -> Option<Vec<String>> {
        self.pty_data
            .as_ref()
            .map(|p| vec![format!("SHELL={}", p.term)])
    }

    pub fn set_connected(&mut self, exec_id: String, stdin: ContainerSink) {
        match self.state {
            ConnectionState::Connected(_, _) => panic!("already connected"),
            ConnectionState::NotConnected => {
                self.state = ConnectionState::Connected(exec_id, stdin)
            }
        }
    }
}

impl Default for SSHSession {
    fn default() -> Self {
        Self {
            state: ConnectionState::NotConnected,
            pty_data: None,
        }
    }
}

pub enum ConnectionState {
    NotConnected,
    Connected(String, ContainerSink),
}

#[derive(Debug)]
pub struct PtyData {
    height: u16,
    width: u16,
    term: String,
}

impl PtyData {
    pub fn new<S, U1, U2>(term: S, height: U1, width: U2) -> Self
    where
        S: Into<String>,
        U1: TryInto<u16>,
        U1::Error: std::fmt::Debug,
        U2: TryInto<u16>,
        U2::Error: std::fmt::Debug,
    {
        let term = term.into();
        let height = height.try_into().unwrap();
        let width = width.try_into().unwrap();
        Self {
            height,
            width,
            term,
        }
    }
}

#[derive(Debug, Error)]
pub enum HandlerError {
    #[error("unhandled ssh error: {0}")]
    UnhandledSSH(#[from] thrussh::Error),
    #[error("docker error: {0}")]
    Docker(#[from] bollard::errors::Error),
}

pub struct DockerServer {
    client: Arc<Docker>,
}

impl Server for DockerServer {
    type Handler = DockerHandler;

    fn new(&mut self, addr: Option<std::net::SocketAddr>) -> Self::Handler {
        eprintln!("making new handler: {:?}", addr);
        Self::Handler::new(Arc::clone(&self.client))
    }
}

impl DockerServer {
    pub fn new(client: Arc<Docker>) -> Self {
        Self { client }
    }
}

pub struct DockerHandler {
    client: Arc<Docker>,
    sessions: HashMap<ChannelId, SSHSession>,
}

impl DockerHandler {
    pub fn new(client: Arc<Docker>) -> Self {
        let sessions = HashMap::new();
        Self { client, sessions }
    }
}

type BoxedFuture<V> = Pin<Box<dyn Future<Output = V> + Send>>;

impl Handler for DockerHandler {
    type Error = HandlerError;

    type FutureAuth = Ready<Result<(Self, Auth), Self::Error>>;

    type FutureUnit = BoxedFuture<Result<(Self, Session), Self::Error>>;

    type FutureBool = Ready<Result<(Self, Session, bool), Self::Error>>;

    fn channel_open_session(mut self, channel: ChannelId, session: Session) -> Self::FutureUnit {
        eprintln!("creating new session: {:?}", channel);
        if self.sessions.contains_key(&channel) {
            panic!("already active session initialised on {:?}", channel);
        }

        self.sessions.insert(channel, Default::default());
        self.finished(session)
    }

    fn finished_auth(self, auth: Auth) -> Self::FutureAuth {
        ready(Ok((self, auth)))
    }

    fn finished_bool(self, b: bool, session: Session) -> Self::FutureBool {
        ready(Ok((self, session, b)))
    }

    fn finished(self, session: Session) -> Self::FutureUnit {
        ready(Ok((self, session))).boxed()
    }

    fn auth_none(self, user: &str) -> Self::FutureAuth {
        eprintln!("user {} connecting", user);
        self.finished_auth(match user {
            "denbeigh" => Auth::Accept,
            _ => Auth::Reject,
        })
    }

    fn auth_keyboard_interactive(
        self,
        user: &str,
        _submethods: &str,
        _response: Option<Response<'_>>,
    ) -> Self::FutureAuth {
        eprintln!("user {} connecting", user);
        self.finished_auth(match user {
            "denbeigh" => Auth::Accept,
            _ => Auth::Reject,
        })
    }

    fn pty_request(
        mut self,
        channel: ChannelId,
        term: &str,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _modes: &[(thrussh::Pty, u32)],
        session: Session,
    ) -> Self::FutureUnit {
        let ssh_session = self.sessions.get_mut(&channel).expect("uninitialised");
        if let Some(d) = &ssh_session.pty_data {
            // TODO: Error handling
            panic!("pty data already set: {:?}", d);
        }

        ssh_session.pty_data = Some(PtyData::new(term, row_height, col_width));
        self.finished(session)
    }

    fn shell_request(self, channel: ChannelId, session: Session) -> Self::FutureUnit {
        self.handle_shell(channel, session).boxed()
    }

    fn data(self, channel: ChannelId, data: &[u8], session: Session) -> Self::FutureUnit {
        self.handle_data(channel, data.to_vec(), session).boxed()
    }

    fn window_change_request(
        self,
        channel: ChannelId,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        session: Session,
    ) -> Self::FutureUnit {
        self.resize_window(row_height as u16, col_width as u16, channel, session)
            .boxed()
    }

    fn channel_close(mut self, channel: ChannelId, session: Session) -> Self::FutureUnit {
        self.sessions.remove(&channel).unwrap();
        eprintln!("channel closed: {:?}", &channel);

        self.finished(session)
    }
}

async fn handle_exec(
    client: Arc<Docker>,
    exec_id: String,
    mut handle: thrussh::server::Handle,
    mut output: LogStream,
    channel: ChannelId,
) {
    while let Some(e) = output.next().await {
        let e = e.unwrap();

        eprintln!("sending data");

        match e {
            LogOutput::StdErr { message } => {
                let msg = CryptoVec::from_slice(&message);
                handle.extended_data(channel, 1, msg).await.unwrap();
            }
            LogOutput::StdOut { message } => {
                let msg = CryptoVec::from_slice(&message);
                handle.data(channel, msg).await.unwrap();
            }
            _ => (),
        };
    }

    eprintln!("no more data");

    let exec = client.inspect_exec(&exec_id).await.unwrap();
    let code = exec.exit_code.unwrap_or(0);

    handle
        .exit_status_request(channel, code as u32)
        .await
        .unwrap();
    handle.eof(channel).await.unwrap();
    handle.close(channel).await.unwrap();

    eprintln!("eof sent");
}

impl DockerHandler {
    async fn handle_data(
        mut self,
        channel: ChannelId,
        data: Vec<u8>,
        session: Session,
    ) -> Result<(Self, Session), HandlerError> {
        let ssh_session = self
            .sessions
            .get_mut(&channel)
            .expect("data for uninitialised channel?");
        if let ConnectionState::Connected(_, input) = &mut ssh_session.state {
            input.write_all(&data).await.unwrap();
        }

        Ok((self, session))
    }

    async fn resize_window(
        mut self,
        h: u16,
        w: u16,
        channel: ChannelId,
        session: Session,
    ) -> Result<(Self, Session), HandlerError> {
        let ssh_session = self
            .sessions
            .get_mut(&channel)
            .expect("resize window for uninitialised channel");

        match ssh_session.pty_data.as_mut() {
            Some(d) => {
                d.height = h;
                d.width = w;
            }
            None => unreachable!(),
        }

        let id = match &ssh_session.state {
            ConnectionState::NotConnected => {
                panic!("how did we resize before a shell was requested?")
            }
            ConnectionState::Connected(id, _) => id,
        };

        eprintln!("resizing to {}x{}", h, w);

        self.client
            .resize_exec(
                id,
                ResizeExecOptions {
                    height: h,
                    width: w,
                },
            )
            .await
            .unwrap();

        Ok((self, session))
    }

    async fn handle_shell(
        mut self,
        channel: ChannelId,
        session: Session,
    ) -> Result<(Self, Session), HandlerError> {
        // TODO: Why doesn't this work?
        // let containers = self
        //     .client
        //     .list_containers(None::<ListContainersOptions<&str>>)
        //     .await?;

        // let container = containers
        //     .into_iter()
        //     .find(|c| {
        //         c.names
        //             .as_ref()
        //             .unwrap()
        //             .iter()
        //             .find(|n| n.as_str().trim() == "ssh")
        //             .is_some()
        //     })
        //     .unwrap();

        let ssh_session = self.sessions.get_mut(&channel).expect("uninitialised");
        let env = ssh_session.env();
        let request_tty = env.is_some();

        let exec = self
            .client
            .create_exec(
                DEST_CONTAINER,
                CreateExecOptions {
                    attach_stdin: Some(request_tty),
                    attach_stdout: Some(request_tty),
                    attach_stderr: Some(request_tty),
                    tty: Some(request_tty),
                    env,
                    cmd: Some(vec!["bash".to_string()]),
                    ..Default::default()
                },
            )
            .await?;

        eprintln!("created exec {}", &exec.id);

        let exec_hand = self
            .client
            .start_exec(&exec.id, Some(StartExecOptions { detach: false }))
            .await?;

        if let Some(info) = &ssh_session.pty_data {
            self.client
                .resize_exec(
                    &exec.id,
                    ResizeExecOptions {
                        height: info.height,
                        width: info.width,
                    },
                )
                .await
                .unwrap();
        }

        let (output, input) = match exec_hand {
            StartExecResults::Attached { output, input } => (output, input),
            StartExecResults::Detached => panic!("detached?"),
        };

        eprintln!("connected");
        ssh_session.set_connected(exec.id.to_string(), input);

        tokio::spawn(handle_exec(
            Arc::clone(&self.client),
            exec.id,
            session.handle(),
            output,
            channel,
        ));

        Ok((self, session))
    }
}
