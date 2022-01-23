use std::fmt::Debug;
use std::marker::PhantomData;

use bb8_redis::bb8::{Pool, PooledConnection, RunError};
use bb8_redis::redis::{self, FromRedisValue, RedisError, RedisResult, ToRedisArgs, Value};
use bb8_redis::RedisConnectionManager;
use futures::Future;
use thiserror::Error;

const RESOURCE_NEW: &str = "<new>";
const RESOURCE_PENDING: &str = "<pending>";

const TRANSACTION_ATTEMPTS: usize = 3;

#[derive(Debug)]
pub enum Resource<V: Debug> {
    New,
    Pending,
    Existing(V),
}

pub enum PoolState {
    Empty,
    NotEmpty,
}

#[derive(Debug)]
pub struct OptionalCountAndResource<V: Debug>(Option<(usize, Resource<V>)>);

impl<V: From<String> + Debug> FromRedisValue for OptionalCountAndResource<V> {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let (n, id): (Option<usize>, Option<String>) = redis::from_redis_value(v)?;

        Ok(Self(match (n, id.as_deref()) {
            (Some(n), Some(RESOURCE_PENDING)) => Some((n, Resource::Pending)),
            (Some(n), Some(RESOURCE_NEW)) => Some((n, Resource::New)),
            (Some(n), Some(_)) => Some((n, Resource::Existing(V::from(id.unwrap())))),
            (None, None) => None,
            (Some(_), None) => panic!("count set, resource not set"),
            (None, Some(_)) => panic!("resource set, count not set"),
        }))
    }
}

impl<V: Debug> OptionalCountAndResource<V> {
    pub fn transpose(self) -> Option<CountAndResource<V>> {
        self.0.map(|r| CountAndResource(r.0, r.1))
    }
}

pub struct CountAndResource<V: Debug>(pub usize, pub Resource<V>);

impl<V> ToRedisArgs for Resource<V>
where
    V: AsRef<str> + Debug,
{
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + bb8_redis::redis::RedisWrite,
    {
        match self {
            Self::New => out.write_arg(RESOURCE_NEW.as_bytes()),
            Self::Pending => out.write_arg(RESOURCE_PENDING.as_bytes()),
            Self::Existing(v) => out.write_arg(v.as_ref().as_bytes()),
        }
    }
}

impl<V> FromRedisValue for Resource<V>
where
    V: From<String> + Debug,
{
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let s: String = redis::from_redis_value(v)?;
        match s.as_str() {
            // NOTE: We should never return New if this function is called,
            // because if this key exists at all it means that it is already
            // being used or constructed by a concurrent connection. The empty
            // string is handled to be defensive only.
            RESOURCE_NEW => Ok(Self::Pending),
            RESOURCE_PENDING => Ok(Self::Pending),
            "" => Ok(Self::New),
            _ => Ok(Self::Existing(V::from(s))),
        }
    }
}

pub struct ResourcePool<K, V>
where
    K: Key,
    V: AsRef<str> + From<String>,
{
    _k: PhantomData<K>,
    _v: PhantomData<V>,
    pool: Pool<RedisConnectionManager>,
}

impl<K, V> ResourcePool<K, V>
where
    K: Key,
    V: AsRef<str> + From<String>,
{
    pub fn new(pool: Pool<RedisConnectionManager>) -> Self {
        let _k = PhantomData::default();
        let _v = PhantomData::default();
        Self { _k, _v, pool }
    }
}

pub trait Key {
    fn category_key(&'_ self) -> &'_ str;
    fn key(&'_ self) -> &'_ str;
}

#[derive(Debug, Error)]
pub enum PoolError {
    #[error("connection timeout")]
    ConnectionTimeout,
    #[error("redis error: {0}")]
    Redis(#[from] RedisError),
}

impl From<RunError<RedisError>> for PoolError {
    fn from(e: RunError<RedisError>) -> Self {
        match e {
            RunError::User(e) => Self::Redis(e),
            RunError::TimedOut => Self::ConnectionTimeout,
        }
    }
}

impl<K, V> ResourcePool<K, V>
where
    K: Key + Debug,
    V: AsRef<str> + From<String> + Sync + Debug,
{
    async fn get_conn(
        &self,
    ) -> Result<PooledConnection<'_, RedisConnectionManager>, RunError<RedisError>> {
        Ok(self.pool.get().await?)
    }

    fn count_key(&self, key: &K) -> String {
        format!("{}:count:{}", key.category_key(), key.key())
    }

    fn resource_id_key(&self, key: &K) -> String {
        format!("{}:id:{}", key.category_key(), key.key())
    }

    pub async fn set_id(&self, key: &K, v: &V) -> Result<(), PoolError> {
        let res_key = self.resource_id_key(key);

        let mut conn = self.get_conn().await?;
        let mut cmd = redis::Cmd::new();
        cmd.arg("SET").arg(&res_key).arg(v.as_ref());
        let _: () = cmd.query_async(&mut *conn).await?;

        Ok(())
    }

    pub async fn join(&self, key: &K) -> Result<CountAndResource<V>, PoolError> {
        let count_key = self.count_key(key);
        let res_key = self.resource_id_key(key);

        let data = (count_key.clone(), res_key.clone());

        let conn = self.get_conn().await?;
        let (_conn, r): (_, Option<CountAndResource<V>>) =
            transaction(&[&count_key, &res_key], conn, data, join_txn).await?;

        Ok(r.unwrap())
    }

    pub async fn leave(&self, key: &K, resource_id: &V) -> Result<PoolState, PoolError> {
        let count_key = self.count_key(key);
        let res_key = self.resource_id_key(key);

        let conn = self.get_conn().await?;
        let data = (count_key.clone(), res_key.clone(), resource_id);

        let (_conn, pool_state) =
            transaction(&[&count_key, &res_key], conn, data, leave_txn).await?;

        Ok(pool_state)
    }
}

enum Tx<RV> {
    Continue(redis::Pipeline, RV),
    Abort(RV),
}

type RedisConn<'a> = PooledConnection<'a, RedisConnectionManager>;

async fn transaction<'a, F, A, ConnFut, RV>(
    keys: &[&str],
    mut conn: PooledConnection<'a, RedisConnectionManager>,
    args: A,
    f: F,
) -> RedisResult<(RedisConn<'a>, RV)>
where
    ConnFut: Future<Output = RedisResult<(RedisConn<'a>, Tx<RV>)>>,
    A: Clone,
    F: Fn(RedisConn<'a>, A) -> ConnFut,
{
    for i in 0..TRANSACTION_ATTEMPTS {
        let mut pl = redis::pipe();
        pl.cmd("WATCH");
        for i in keys {
            pl.arg(i);
        }

        let _: () = pl.query_async(&mut *conn).await?;
        let (c, tx) = f(conn, args.clone()).await?;
        // Re-assign to outer loop variable to avoid losing
        conn = c;
        match tx {
            Tx::Abort(v) => return Ok((conn, v)),
            Tx::Continue(pl, v) => {
                let res: Option<()> = pl.query_async(&mut *conn).await?;
                log::debug!("transaction: {:?} {}/{}", res, i + 1, TRANSACTION_ATTEMPTS);
                match res {
                    Some(_) => return Ok((conn, v)),
                    None => continue,
                }
            }
        }
    }
    panic!("too many attempts");
}

async fn join_txn<V: From<String> + AsRef<str> + Debug>(
    mut conn: RedisConn<'_>,
    args: (String, String),
) -> RedisResult<(RedisConn<'_>, Tx<Option<CountAndResource<V>>>)> {
    let mut cmd = redis::Cmd::new();
    let (ck, rk) = args;

    cmd.arg("MGET").arg(&ck).arg(&rk);
    let data: OptionalCountAndResource<V> = cmd.query_async(&mut *conn).await?;
    log::debug!("MGET #1: {:?}", &data);

    let (n, id, res) = match data.transpose() {
        Some(CountAndResource(s, Resource::Existing(id))) => {
            (s + 1, id.as_ref().to_string(), Resource::Existing(id))
        }
        None => (1, RESOURCE_PENDING.to_string(), Resource::New),
        Some(CountAndResource(_, Resource::Pending)) => todo!(),
        _ => todo!(),
    };

    let mut pl = redis::pipe();
    pl.cmd("MULTI")
        .ignore()
        .cmd("SET")
        .arg(&ck)
        .arg::<usize>(n)
        .ignore()
        .cmd("SET")
        .arg(&rk)
        .arg(&id)
        .ignore()
        .cmd("EXEC");

    log::debug!(
        "query: MULTI / SET {} {} / SET {} {} / EXEC",
        &ck,
        n,
        &rk,
        id
    );

    Ok((conn, Tx::Continue(pl, Some(CountAndResource(n, res)))))
}

// Needs to:
//  - Check how many sessions remain active in the container
//  - If just us, remove the session and mark that the caller needs to clean up
//  - If somebody else, decrement the count and mark that the caller doesn't
//    need to do anything
async fn leave_txn<'a, 'b, V: AsRef<str> + From<String> + Debug>(
    mut conn: RedisConn<'a>,
    args: (String, String, &'b V),
) -> RedisResult<(RedisConn<'a>, Tx<PoolState>)> {
    let (count_key, res_key, _resource) = args;

    let mut cmd = redis::Cmd::new();
    cmd.arg("MGET").arg(&count_key).arg(&res_key);
    let data: OptionalCountAndResource<V> = cmd.query_async(&mut *conn).await?;
    let mut pl = redis::pipe();
    pl.cmd("MULTI").ignore();
    let state = match data.transpose() {
        None | Some(CountAndResource(0, _)) | Some(CountAndResource(1, _)) => {
            pl.cmd("DEL").arg(&res_key).ignore();
            pl.cmd("DEL").arg(&count_key).ignore();
            PoolState::Empty
        }
        Some(CountAndResource(n, _)) => {
            pl.cmd("SET").arg(&count_key).arg(n - 1).ignore();
            PoolState::NotEmpty
        }
    };

    pl.cmd("EXEC");
    Ok((conn, Tx::Continue(pl, state)))
}
