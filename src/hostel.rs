use std::collections::HashMap;
use std::convert::{Infallible, TryInto};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bollard::container::{
    Config, CreateContainerOptions, KillContainerOptions, LogOutput, RemoveContainerOptions,
    StartContainerOptions,
};
use bollard::exec::{CreateExecOptions, ResizeExecOptions, StartExecOptions, StartExecResults};
use bollard::image::CreateImageOptions;
use bollard::Docker;
use futures::future::{ready, FutureExt, Ready};
use futures::{Stream, StreamExt};
use thiserror::Error;
use thrussh::server::{Auth, Handler, Response, Session};
use thrussh::{ChannelId, CryptoVec};
use thrussh_keys::key::PublicKey;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, watch};

use crate::active::{CountAndResource, Key, PoolError, PoolState, Resource, ResourcePool};
use crate::config::{self, KeyLoadError};
use crate::utils::ssh::clone_key;

type LogStream = Pin<Box<dyn Stream<Item = Result<LogOutput, bollard::errors::Error>> + Send>>;
type ContainerSink = Pin<Box<dyn AsyncWrite + Send>>;

pub struct SSHSession {
    pub state: ConnectionState,
    pub pty_data: Option<PtyData>,
}

pub trait Server: thrussh::server::Server {
    // TODO: Counting/logging functionality should probably be moved inside the
    // "server" type, and it should just expose a oneshot that is resolved when
    // it is safe to shut down
    fn remaining_connections(&self) -> watch::Receiver<usize>;
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

    pub fn set_connected(&mut self, container_id: ContainerId, exec_id: ExecId) {
        match self.state {
            ConnectionState::Connected(_, _) => panic!("already connected"),
            ConnectionState::NotConnected => {
                self.state = ConnectionState::Connected(container_id, exec_id)
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
    Connected(ContainerId, ExecId),
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
    #[error("validating public key: {0}")]
    PubkeyAuth(#[from] KeyLoadError),
    #[error("handling shell request")]
    Shell(#[from] ShellHandlerError),
}

pub struct HostelServer {
    client: Arc<Docker>,
    config: Arc<config::MetaConfig>,
    connected: Arc<AtomicUsize>,
    connected_tx: Arc<watch::Sender<usize>>,
    connected_rx: watch::Receiver<usize>,
    active_resources: Arc<ResourcePool<Username, ContainerId>>,
}

impl thrussh::server::Server for HostelServer {
    type Handler = HostelHandler;

    fn new(&mut self, addr: Option<std::net::SocketAddr>) -> Self::Handler {
        log::debug!("making new handler: {:?}", addr);
        Self::Handler::new(
            Arc::clone(&self.client),
            Arc::clone(&self.config),
            Arc::clone(&self.connected),
            Arc::clone(&self.connected_tx),
            Arc::clone(&self.active_resources),
        )
    }
}

impl Server for HostelServer {
    fn remaining_connections(&self) -> watch::Receiver<usize> {
        self.connected_rx.clone()
    }
}

impl HostelServer {
    pub fn new(
        client: Arc<Docker>,
        config: config::MetaConfig,
        active_resources: Arc<ResourcePool<Username, ContainerId>>,
    ) -> Self {
        let (connected_tx, connected_rx) = watch::channel(8);
        let connected_tx = Arc::new(connected_tx);
        let connected = Arc::new(AtomicUsize::from(0));
        connected_tx.send(0).unwrap();
        let config = Arc::new(config);
        Self {
            client,
            config,
            connected,
            connected_rx,
            connected_tx,
            active_resources,
        }
    }
}

#[derive(Debug)]
pub struct Username(String);

impl Key for Username {
    fn category_key(&'_ self) -> &'_ str {
        "user"
    }

    fn key(&'_ self) -> &'_ str {
        self.0.as_str()
    }
}

#[derive(Clone, Debug)]
pub struct ContainerId(String);

#[derive(Clone, Debug)]
pub struct ExecId(String);

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

pub struct HostelHandler {
    client: Arc<Docker>,
    config: Arc<config::MetaConfig>,
    active_resources: Arc<ResourcePool<Username, ContainerId>>,
    sessions: HashMap<ChannelId, SSHSession>,
    connected: Arc<AtomicUsize>,
    connected_tx: Arc<watch::Sender<usize>>,
    username: Option<Username>,
    data_tx: mpsc::Sender<InputItem>,
}

impl HostelHandler {
    pub fn new(
        client: Arc<Docker>,
        config: Arc<config::MetaConfig>,
        connected: Arc<AtomicUsize>,
        connected_tx: Arc<watch::Sender<usize>>,
        active_resources: Arc<ResourcePool<Username, ContainerId>>,
    ) -> Self {
        let sessions = HashMap::new();
        let (data_tx, data_rx) = mpsc::channel(24);
        tokio::spawn(handle_input(data_rx));
        Self {
            client,
            config,
            sessions,
            connected,
            connected_tx,
            active_resources,
            username: None,
            data_tx,
        }
    }
}

type BoxedFuture<V> = Pin<Box<dyn Future<Output = V> + Send>>;

impl Handler for HostelHandler {
    type Error = HandlerError;

    type FutureAuth = BoxedFuture<Result<(Self, Auth), Self::Error>>;

    type FutureUnit = BoxedFuture<Result<(Self, Session), Self::Error>>;

    type FutureBool = Ready<Result<(Self, Session, bool), Self::Error>>;

    fn channel_open_session(mut self, channel: ChannelId, session: Session) -> Self::FutureUnit {
        log::info!("creating new session: {:?}", channel);
        if self.sessions.contains_key(&channel) {
            panic!("already active session initialised on {:?}", channel);
        }

        self.sessions.insert(channel, Default::default());
        let n = self.connected.fetch_add(1, Ordering::SeqCst) + 1;
        log::info!("joining, reporting {} items", n);
        if self.connected_tx.send(n).is_err() {
            log::error!("not able to send new count on connection, may experience interrupts");
        };
        self.finished(session)
    }

    fn finished_auth(self, auth: Auth) -> Self::FutureAuth {
        ready(Ok((self, auth))).boxed()
    }

    fn finished_bool(self, b: bool, session: Session) -> Self::FutureBool {
        ready(Ok((self, session, b)))
    }

    fn finished(self, session: Session) -> Self::FutureUnit {
        ready(Ok((self, session))).boxed()
    }

    fn auth_publickey(mut self, user: &str, public_key: &PublicKey) -> Self::FutureAuth {
        let key = clone_key(public_key);
        let user = user.to_string();
        self.username = Some(Username(user.clone()));
        async move {
            let auth = match self.auth_pubkey(user, &key).await? {
                (s, true) => (s, Auth::Accept),
                (s, false) => (s, Auth::Reject),
            };
            Ok(auth)
        }
        .boxed()
    }

    fn auth_none(mut self, user: &str) -> Self::FutureAuth {
        log::info!("user {} connecting", user);
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
        log::info!("user {} connecting", user);
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
        self.handle_shell(channel, session)
            .map(|r| r.map_err(|e| e.into()))
            .boxed()
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

    fn channel_close(self, channel: ChannelId, session: Session) -> Self::FutureUnit {
        self.handle_close(channel, session).boxed()
    }
}

async fn handle_exec(
    client: Arc<Docker>,
    exec_id: String,
    handle: thrussh::server::Handle,
    output: LogStream,
    channel: ChannelId,
) {
    if let Err(e) = handle_exec_result(client, exec_id, handle, output, channel).await {
        log::error!("error while handling exec: {e}");
    }
}

#[derive(Debug, Error)]
enum ExecHandleError {
    #[error("reading from docker message queue: {0}")]
    ReceivingMessage(bollard::errors::Error),
    #[error("reading created exec: {0}")]
    InspectExec(bollard::errors::Error),
}

async fn handle_exec_result(
    client: Arc<Docker>,
    exec_id: String,
    mut handle: thrussh::server::Handle,
    mut output: LogStream,
    channel: ChannelId,
) -> Result<(), ExecHandleError> {
    while let Some(e) = output.next().await {
        let e = e.map_err(ExecHandleError::ReceivingMessage)?;

        let res = match e {
            LogOutput::StdErr { message } => {
                let msg = CryptoVec::from_slice(&message);
                Some(handle.extended_data(channel, 1, msg).await)
            }
            LogOutput::StdOut { message } => {
                let msg = CryptoVec::from_slice(&message);
                Some(handle.data(channel, msg).await)
            }
            _ => None,
        };

        if let Some(Err(l)) = res {
            log::warn!("dropped {} bytes of data to client", l.len());
        }
    }

    let exec = client
        .inspect_exec(&exec_id)
        .await
        .map_err(ExecHandleError::InspectExec)?;
    let code = exec.exit_code.unwrap_or(0) as u32;

    if handle.exit_status_request(channel, code).await.is_err() {
        log::warn!("unable to send exit status to client");
    }
    if handle.eof(channel).await.is_err() {
        log::warn!("unable to send EOF to client");
    }
    if handle.close(channel).await.is_err() {
        log::warn!("unable to send EOF to client");
    }

    Ok(())
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
                    log::warn!("already contains active key: {:?}", &id);
                    continue;
                }

                items.insert(id, sink);
            }
            InputItem::Message(id, data) => match items.get_mut(&id) {
                Some(sink) => {
                    if let Err(e) = sink.write_all(&data).await {
                        log::error!("failed to write data to container: {e}");
                    }
                }
                None => log::warn!("received data but had no sink for {:?}", id),
            },
        }
    }
}

#[derive(Debug, Error)]
pub enum ContainerCreateError {
    #[error("parsing user config file: {0}")]
    ParsingUserConfig(#[from] config::FileParseError),
    #[error("creating new container: {0}")]
    CreatingContainer(bollard::errors::Error),
    #[error("setting store metadata: {0}")]
    SettingMetadata(#[from] PoolError),
    #[error("starting new container: {0}")]
    StartingContainer(bollard::errors::Error),
}

#[derive(Debug, Error)]
pub enum ShellHandlerError {
    #[error("joining/claiming new container: {0}")]
    SettingMetadata(#[from] PoolError),
    #[error("starting container exec: {0}")]
    StartingExec(bollard::errors::Error),
    #[error("creating container exec: {0}")]
    CreatingExec(bollard::errors::Error),
    #[error("starting new container: {0}")]
    CreatingContainer(#[from] ContainerCreateError),
}

#[derive(Debug, Error)]
pub enum CloseError {
    #[error("marking resources freed")]
    SettingMetadata(#[from] PoolError),
}

impl HostelHandler {
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
        if let ConnectionState::Connected(_, _) = &mut ssh_session.state {
            if let Err(e) = self.data_tx.send(InputItem::Message(channel, data)).await {
                log::error!("failed to send data: {}", e);
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

        let exec_id = match &ssh_session.state {
            ConnectionState::NotConnected => {
                panic!("how did we resize before a shell was requested?")
            }
            ConnectionState::Connected(_, id) => id,
        };

        log::debug!("resizing to {}x{}", h, w);

        self.client
            .resize_exec(
                &exec_id.0,
                ResizeExecOptions {
                    height: h,
                    width: w,
                },
            )
            .await?;

        Ok((self, session))
    }

    async fn create_container(&self) -> Result<ContainerId, ContainerCreateError> {
        let username = self.username.as_ref().unwrap();
        let user_config = self.config.user_config(&username.0).await?;

        let image = user_config
            .as_ref()
            .and_then(|c| c.image.as_deref())
            .unwrap_or(self.config.config.image.as_str());

        let mut info = self.client.create_image(
            Some(CreateImageOptions {
                from_image: image,
                ..Default::default()
            }),
            None,
            None,
        );

        while let Some(i) = info.next().await {
            let i = i.expect("error pulling");
            match (i.status, i.progress) {
                (None, None) => eprintln!("(unknown)"),
                (None, Some(p)) => eprintln!("(state unknown) {}\n", p),
                (Some(s), None) => eprintln!("{}", s),
                (Some(s), Some(p)) => eprintln!("{}: {}\n", s, p),
            };
        }

        log::debug!("creating container");
        let container = self
            .client
            .create_container(
                None::<CreateContainerOptions<&str>>,
                Config {
                    entrypoint: Some(vec!["sleep", "infinity"]),
                    image: Some(image),
                    ..Default::default()
                },
            )
            .await
            .map_err(ContainerCreateError::CreatingContainer)?;

        let cid = ContainerId(container.id.clone());
        self.active_resources
            .set_id(self.username.as_ref().unwrap(), &cid)
            .await?;

        log::info!("starting container {:?}", &cid);
        self.client
            .start_container(&container.id, None::<StartContainerOptions<&str>>)
            .await
            .map_err(ContainerCreateError::StartingContainer)?;

        log::debug!("started container");

        Ok(cid)
    }

    async fn handle_shell(
        mut self,
        channel: ChannelId,
        session: Session,
    ) -> Result<(Self, Session), ShellHandlerError> {
        let client = self.client.clone();
        let resources = self.active_resources.clone();
        let u = self.username.as_ref().expect("username unset");
        let id = match resources.join(u).await? {
            CountAndResource(_, Resource::New) => Some(self.create_container().await?),
            CountAndResource(_, Resource::Pending) => todo!(),
            CountAndResource(_, Resource::Existing(id)) => Some(id),
        };

        log::debug!("started container {:?}", id);

        let ssh_session = self.sessions.get_mut(&channel).expect("uninitialised");
        let env = ssh_session.env();
        let request_tty = env.is_some();

        log::debug!("creating exec");

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
            .map_err(ShellHandlerError::CreatingExec)?;

        log::debug!("created exec {}", &exec.id);

        let exec_hand = client
            .start_exec(&exec.id, Some(StartExecOptions { detach: false }))
            .await
            .map_err(ShellHandlerError::StartingExec)?;

        if let Some(info) = &ssh_session.pty_data {
            if let Err(e) = client
                .resize_exec(
                    &exec.id,
                    ResizeExecOptions {
                        height: info.height,
                        width: info.width,
                    },
                )
                .await
            {
                log::warn!("failed to resize remote session: {e}");
            }
        }

        let (output, input) = match exec_hand {
            StartExecResults::Attached { output, input } => (output, input),
            StartExecResults::Detached => panic!("detached?"),
        };

        log::debug!("connected");
        // TODO: we need to create a channel and send input to a spawned task,
        // and keep the rx end of the channel in this map instead.
        let exec_id = ExecId(exec.id.to_string());
        ssh_session.set_connected(container_id.clone(), exec_id);
        if let Err(e) = self.data_tx.send(InputItem::Channel(channel, input)).await {
            log::error!("failed to send channel: {}", e);
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

    async fn handle_close(
        mut self,
        channel: ChannelId,
        session: Session,
    ) -> Result<(Self, Session), HandlerError> {
        log::debug!("channel closed: {:?}", &channel);
        if let ConnectionState::Connected(c_id, _) = self.sessions.remove(&channel).unwrap().state {
            let state_res = self
                .active_resources
                .leave(self.username.as_ref().unwrap(), &c_id)
                .await;
            match state_res {
                Ok(state) => {
                    if matches!(state, PoolState::Empty) {
                        let client = self.client.clone();
                        cleanup_container(client, c_id).await;
                    }
                }
                Err(e) => {
                    log::warn!("error marking container as cleaned up: {e}");
                }
            }

            // Only send this once we've finished cleaning up containers, to
            // avoid premature shutdown.
            let n = self.connected.fetch_sub(1, Ordering::SeqCst) - 1;
            log::info!("leaving, reporting {} items", n);
            if self.connected_tx.send(n).is_err() {
                log::warn!("was not able to transmit remaining client count");
            }
        };

        Ok((self, session))
    }

    async fn auth_pubkey(
        self,
        user: String,
        key: &PublicKey,
    ) -> Result<(Self, bool), HandlerError> {
        let contains = match self.config.user_keys(user.as_str()).await? {
            Some(keys) => keys.contains(key),
            None => false,
        };

        Ok((self, contains))
    }
}

async fn cleanup_container(client: Arc<Docker>, id: ContainerId) {
    log::info!("stopping container {}", &id.0);

    if let Err(e) = client.stop_container(id.0.as_str(), None).await {
        log::warn!("stopping container {} failed: {}", &id.0, e);
        log::info!("killing container {}", &id.0);

        let opts = KillContainerOptions { signal: "SIGKILL" };
        if let Err(e) = client.kill_container(id.0.as_str(), Some(opts)).await {
            log::warn!("killing container {} failed: {}", &id.0, e);
            return;
        }
    }

    log::info!("removing container {}", &id.0);
    let opts = RemoveContainerOptions {
        force: true,
        ..Default::default()
    };
    if let Err(e) = client.remove_container(&id.0, Some(opts)).await {
        log::warn!("removing container {} failed: {}", &id.0, e);
    }
}
