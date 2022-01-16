use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bollard::container::{ListContainersOptions, LogOutput};
use bollard::exec::{CreateExecOptions, ResizeExecOptions, StartExecOptions, StartExecResults};
use bollard::Docker;
use futures::future::{ready, FutureExt, Ready};
use futures::{Stream, StreamExt};
use thiserror::Error;
use thrussh::server::{Auth, Handler, Response, Server, Session};
use thrussh::{ChannelId, CryptoVec};
use tokio::io::{AsyncWrite, AsyncWriteExt};

static DEST_CONTAINER: &str = "6f6ecc2fa824";

type LogStream = Pin<Box<dyn Stream<Item = Result<LogOutput, thrussh::Error>> + Send + Sync>>;
type ContainerSink = Pin<Box<dyn AsyncWrite + Send>>;

pub enum ConnectionState {
    NotConnected,
    Connected(String, ContainerSink),
}

#[derive(Debug)]
struct PtyData {
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

    fn new(&mut self, _: Option<std::net::SocketAddr>) -> Self::Handler {
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
    pty_data: Option<PtyData>,
    state: ConnectionState,
}

impl DockerHandler {
    pub fn new(client: Arc<Docker>) -> Self {
        let pty_data = None;
        let state = ConnectionState::NotConnected;
        Self {
            client,
            pty_data,
            state,
        }
    }
}

type BoxedFuture<V> = Pin<Box<dyn Future<Output = V> + Send>>;

impl Handler for DockerHandler {
    type Error = HandlerError;

    type FutureAuth = Ready<Result<(Self, Auth), Self::Error>>;

    type FutureUnit = BoxedFuture<Result<(Self, Session), Self::Error>>;

    type FutureBool = Ready<Result<(Self, Session, bool), Self::Error>>;

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
        if let Some(d) = self.pty_data {
            // TODO: Error handling
            panic!("pty data already set: {:?}", d);
        }

        self.pty_data = Some(PtyData::new(term, row_height, col_width));
        self.finished(session)
    }

    fn shell_request(self, channel: ChannelId, session: Session) -> Self::FutureUnit {
        self.handle_shell(channel, session).boxed()
    }

    fn data(self, channel: ChannelId, data: &[u8], session: Session) -> Self::FutureUnit {
        self.handle_data(data.to_vec(), session).boxed()
    }

    fn window_change_request(
        self,
        channel: ChannelId,
        col_width: u32,
        row_height: u32,
        pix_width: u32,
        pix_height: u32,
        session: Session,
    ) -> Self::FutureUnit {
        self.resize_window(row_height as u16, col_width as u16, session)
            .boxed()
    }
}

impl DockerHandler {
    async fn handle_data(
        mut self,
        data: Vec<u8>,
        session: Session,
    ) -> Result<(Self, Session), HandlerError> {
        if let ConnectionState::Connected(_, input) = &mut self.state {
            input.write_all(&data).await.unwrap();
        }

        Ok((self, session))
    }

    async fn resize_window(
        mut self,
        h: u16,
        w: u16,
        session: Session,
    ) -> Result<(Self, Session), HandlerError> {
        let mut pty_data = self.pty_data.as_mut();
        match pty_data {
            Some(d) => {
                d.height = h;
                d.width = w;
            }
            None => unreachable!(),
        }

        let id = match &self.state {
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

        let request_tty = self.pty_data.is_some();
        let env = self
            .pty_data
            .as_ref()
            .map(|p| vec![format!("SHELL={}", p.term)]);

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

        if let Some(info) = &self.pty_data {
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

        let (mut output, input) = match exec_hand {
            StartExecResults::Attached { output, input } => (output, input),
            StartExecResults::Detached => panic!("detached?"),
        };

        eprintln!("connected");
        self.state = ConnectionState::Connected(exec.id, input);

        let handle = session.handle();
        tokio::spawn(async move {
            let channel = channel;
            let mut handle = handle;
            while let Some(e) = output.next().await {
                let e = e.unwrap();

                eprintln!("sending data");

                match e {
                    LogOutput::StdErr { message } => {
                        let msg = CryptoVec::from_slice(&message);
                        handle.extended_data(channel.clone(), 1, msg).await.unwrap();
                    }
                    LogOutput::StdOut { message } => {
                        let msg = CryptoVec::from_slice(&message);
                        handle.data(channel.clone(), msg).await.unwrap();
                    }
                    _ => (),
                };
            }

            handle.eof(channel);
        });

        Ok((self, session))
    }
}
