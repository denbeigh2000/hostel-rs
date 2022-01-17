use std::collections::HashMap;
use std::convert::{Infallible, TryInto};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bollard::container::{Config, CreateContainerOptions, LogOutput, StartContainerOptions};
use bollard::exec::{CreateExecOptions, ResizeExecOptions, StartExecOptions, StartExecResults};
use bollard::image::CreateImageOptions;
use bollard::Docker;
use futures::future::{ready, FutureExt, Ready};
use futures::{Stream, StreamExt};
use thiserror::Error;
use thrussh::server::{Auth, Handler, Response, Server, Session};
use thrussh::{ChannelId, CryptoVec};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::active::{CountAndResource, Key, Resource, ResourcePool};

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

    pub fn set_connected(&mut self, exec_id: String) {
        match self.state {
            ConnectionState::Connected(_) => panic!("already connected"),
            ConnectionState::NotConnected => self.state = ConnectionState::Connected(exec_id),
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
    Connected(String),
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
    active_resources: Arc<ResourcePool<Username, ContainerId>>,
}

impl Server for DockerServer {
    type Handler = DockerHandler;

    fn new(&mut self, addr: Option<std::net::SocketAddr>) -> Self::Handler {
        eprintln!("making new handler: {:?}", addr);
        Self::Handler::new(Arc::clone(&self.client), Arc::clone(&self.active_resources))
    }
}

impl DockerServer {
    pub fn new(
        client: Arc<Docker>,
        active_resources: Arc<ResourcePool<Username, ContainerId>>,
    ) -> Self {
        Self {
            client,
            active_resources,
        }
    }
}

pub struct Username(String);

impl Key for Username {
    fn category_key(&'_ self) -> &'_ str {
        "user"
    }

    fn key(&'_ self) -> &'_ str {
        self.0.as_str()
    }
}

#[derive(Debug)]
pub struct ContainerId(String);

impl From<String> for ContainerId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl AsRef<str> for ContainerId {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

pub struct DockerHandler {
    client: Arc<Docker>,
    active_resources: Arc<ResourcePool<Username, ContainerId>>,
    sessions: HashMap<ChannelId, SSHSession>,
    username: Option<Username>,
    data_tx: mpsc::Sender<InputItem>,
}

impl DockerHandler {
    pub fn new(
        client: Arc<Docker>,
        active_resources: Arc<ResourcePool<Username, ContainerId>>,
    ) -> Self {
        let sessions = HashMap::new();
        let (data_tx, data_rx) = mpsc::channel(24);
        tokio::spawn(handle_input(data_rx));

        Self {
            client,
            sessions,
            active_resources,
            username: None,
            data_tx,
        }
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

    fn auth_none(mut self, user: &str) -> Self::FutureAuth {
        eprintln!("user {} connecting", user);
        self.username = Some(Username(user.to_string()));
        self.finished_auth(match user {
            "denbeigh" => Auth::Accept,
            _ => Auth::Reject,
        })
    }

    fn auth_keyboard_interactive(
        mut self,
        user: &str,
        _submethods: &str,
        _response: Option<Response<'_>>,
    ) -> Self::FutureAuth {
        eprintln!("user {} connecting", user);
        self.username = Some(Username(user.to_string()));
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
        // NOTE on fixing this problem sending sinks across threads: instead
        // of storing the sinks locally, we need to spawn a new task when we
        // create our exec thawe we pass the rx end of a channel to, then store
        // the tx end in our local map to send data from framework events.
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

enum InputItem {
    Channel(ChannelId, ContainerSink),
    Message(ChannelId, Vec<u8>),
}

async fn handle_input(mut rx: mpsc::Receiver<InputItem>) {
    let mut items: HashMap<ChannelId, ContainerSink> = HashMap::new();

    while let Some(item) = rx.recv().await {
        match item {
            InputItem::Channel(id, sink) => {
                if items.contains_key(&id) {
                    eprintln!("already contains active key: {:?}", &id);
                    continue;
                }

                items.insert(id, sink);
            }
            InputItem::Message(id, data) => match items.get_mut(&id) {
                Some(sink) => sink.write_all(&data).await.unwrap(),
                None => eprintln!("recived data but had no sink for {:?}", id),
            },
        }
    }
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
        if let ConnectionState::Connected(_) = &mut ssh_session.state {
            if let Err(e) = self.data_tx.send(InputItem::Message(channel, data)).await {
                eprintln!("failed to send data: {}", e);
            }
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
            ConnectionState::Connected(id) => id,
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

    async fn create_container(&self) -> Result<ContainerId, Infallible> {
        let mut info = self.client.create_image(
            Some(CreateImageOptions {
                from_image: "ubuntu:latest",
                ..Default::default()
            }),
            None,
            None,
        );

        while let Some(i) = info.next().await {
            let i = i.expect("error pulling");
            match (i.status, i.progress) {
                (None, None) => eprintln!("unknown"),
                (None, Some(p)) => eprintln!("(state unknown) {}", p),
                (Some(s), None) => eprintln!("{}", s),
                (Some(s), Some(p)) => eprintln!("{}: {}", s, p),
            }
        }

        eprintln!("creating container");
        let container = self
            .client
            .create_container(
                None::<CreateContainerOptions<&str>>,
                Config {
                    image: Some("ubuntu:latest"),
                    entrypoint: Some(vec!["sleep", "infinity"]),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        eprintln!("starting container");
        self.client
            .start_container(&container.id, None::<StartContainerOptions<&str>>)
            .await
            .unwrap();

        eprintln!("started container");

        Ok(ContainerId(container.id))
    }

    async fn handle_shell(
        mut self,
        channel: ChannelId,
        session: Session,
    ) -> Result<(Self, Session), HandlerError> {
        let client = self.client.clone();
        let resources = self.active_resources.clone();
        let username = self.username.as_ref().expect("username unset");
        let u = self.username.as_ref().unwrap();
        let id = match resources.join(u).await.unwrap() {
            CountAndResource(_, Resource::Existing(id)) => Some(id),
            CountAndResource(_, Resource::New) => Some(self.create_container().await.unwrap()),
            CountAndResource(_, Resource::Pending) => todo!(),
        };

        eprintln!("started container {:?}", id);

        let ssh_session = self.sessions.get_mut(&channel).expect("uninitialised");
        let env = ssh_session.env();
        let request_tty = env.is_some();

        eprintln!("creating exec");

        let container_id = &id.unwrap();

        let exec = client
            .create_exec(
                container_id.0.as_str(),
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
            .await
            .unwrap();

        eprintln!("created exec {}", &exec.id);

        let exec_hand = client
            .start_exec(&exec.id, Some(StartExecOptions { detach: false }))
            .await?;

        if let Some(info) = &ssh_session.pty_data {
            client
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
        // TODO: we need to create a channel and send input to a spawned task,
        // and keep the rx end of the channel in this map instead.
        ssh_session.set_connected(exec.id.to_string());
        if let Err(e) = self.active_resources.set_id(username, container_id).await {
            eprintln!("failed to set container id: {}", e);
        }
        if let Err(e) = self.data_tx.send(InputItem::Channel(channel, input)).await {
            eprintln!("failed to send channel: {}", e);
        }

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
