use std::convert::Infallible;
use std::marker::PhantomData;

use bb8_redis::bb8::{Pool, PooledConnection};
use bb8_redis::redis::aio::ConnectionLike;
use bb8_redis::redis::{
    self, AsyncCommands, Cmd, FromRedisValue, Pipeline, RedisResult, ToRedisArgs, Value,
};
use bb8_redis::RedisConnectionManager;
use futures::Future;

const RESOURCE_NEW: &str = "<new>";
const RESOURCE_PENDING: &str = "<pending>";

const TRANSACTION_ATTEMPTS: usize = 3;

pub enum Resource<V> {
    New,
    Pending,
    Existing(V),
}

pub enum PoolState {
    Empty,
    NotEmpty,
}

pub struct OptionalCountAndResource<V>(Option<(usize, Resource<V>)>);

impl<V: From<String>> FromRedisValue for OptionalCountAndResource<V> {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        let v: (Option<usize>, Option<String>) = redis::from_redis_value(v)?;

        Ok(match v {
            (Some(s), Some(r)) => {
                let resource = Resource::Existing(V::from(r));
                Self(Some((s, resource)))
            }
            (None, None) => Self(None),
            _ => unreachable!(),
        })
    }
}

impl<V> OptionalCountAndResource<V> {
    pub fn transpose(self) -> Option<CountAndResource<V>> {
        self.0.map(|r| CountAndResource(r.0, r.1))
    }
}

pub struct CountAndResource<V>(usize, Resource<V>);

impl<V> ToRedisArgs for Resource<V>
where
    V: AsRef<str>,
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
    V: From<String>,
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

pub trait Key {
    fn category_key(&'_ self) -> &'_ str;
    fn key(&self) -> String;
}

impl<K, V> ResourcePool<K, V>
where
    K: Key,
    V: AsRef<str> + From<String> + Sync,
{
    async fn get_conn(&self) -> Result<PooledConnection<'_, RedisConnectionManager>, Infallible> {
        Ok(self.pool.get().await.unwrap())
    }

    fn count_key(&self, key: &K) -> String {
        format!("{}:count:{}", key.category_key(), key.key())
    }

    fn resource_id_key(&self, key: &K) -> String {
        format!("{}:id:{}", key.category_key(), key.key())
    }

    pub async fn join(&self, key: &K) -> Result<CountAndResource<V>, Infallible> {
        let count_key = self.count_key(key);
        let res_key = self.resource_id_key(key);

        let data = (count_key.clone(), res_key.clone());

        let conn = self.get_conn().await.unwrap();
        let (_conn, r): (_, Option<CountAndResource<V>>) =
            transaction(&[&count_key, &res_key], conn, data, join_txn)
                .await
                .unwrap();

        Ok(r.unwrap())
    }

    pub async fn leave(&self, key: &K, resource_id: &V) -> Result<PoolState, Infallible> {
        let count_key = self.count_key(key);
        let res_key = self.resource_id_key(key);

        let conn = self.get_conn().await.unwrap();
        let data = (count_key.clone(), res_key.clone(), resource_id);

        let (_conn, pool_state) = transaction(&[&count_key, &res_key], conn, data, leave_txn)
            .await
            .unwrap();

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
    for _ in 0..TRANSACTION_ATTEMPTS {
        let mut pl = redis::pipe();
        pl.cmd("WATCH");
        for i in keys {
            pl.arg(i);
        }

        let _: () = pl.query_async(&mut *conn).await.unwrap();
        let (c, tx) = f(conn, args.clone()).await?;
        // Re-assign to outer loop variable to avoid losing
        conn = c;
        match tx {
            Tx::Abort(v) => return Ok((conn, v)),
            Tx::Continue(pl, v) => {
                let res: Option<()> = pl.query_async(&mut *conn).await.unwrap();
                match res {
                    Some(_) => return Ok((conn, v)),
                    None => continue,
                }
            }
        }
    }
    panic!("too many attempts");
}

async fn join_txn<V: From<String>>(
    mut conn: RedisConn<'_>,
    args: (String, String),
) -> RedisResult<(RedisConn<'_>, Tx<Option<CountAndResource<V>>>)> {
    let mut pl = redis::pipe();
    let (rk, ck) = args;

    pl.cmd("GET").arg(&rk).cmd("GET").arg(&ck);
    let data: OptionalCountAndResource<V> = pl.query_async(&mut *conn).await.unwrap();
    if let Some(r) = data.transpose() {
        let n = r.0 + 1;
        let mut pl = redis::pipe();
        pl.cmd("SET").arg(&ck).arg(n);
        return Ok((conn, Tx::Continue(pl, Some(CountAndResource(n, r.1)))));
    };

    let mut pl = redis::pipe();
    pl.cmd("MULTI")
        .ignore()
        .cmd("SET")
        .arg(&ck)
        .arg::<usize>(1)
        .ignore()
        .cmd("SET")
        .arg(&rk)
        .arg(RESOURCE_PENDING)
        .ignore()
        .cmd("EXEC");

    Ok((
        conn,
        Tx::Continue(pl, Some(CountAndResource(1, Resource::New))),
    ))
}

// Needs to:
//  - Check how many sessions remain active in the container
//  - If just us, remove the session and mark that the caller needs to clean up
//  - If somebody else, decrement the count and mark that the caller doesn't
//    need to do anything
async fn leave_txn<'a, 'b, V: AsRef<str> + From<String>>(
    mut conn: RedisConn<'a>,
    args: (String, String, &'b V),
) -> RedisResult<(RedisConn<'a>, Tx<PoolState>)> {
    let (count_key, res_key, _resource) = args;

    let mut pl = redis::pipe();
    pl.cmd("GET").arg(&count_key).cmd("GET").arg(&res_key);
    let data: OptionalCountAndResource<V> = pl.query_async(&mut *conn).await.unwrap();
    let mut pl = redis::pipe();
    let state = match data.transpose() {
        None | Some(CountAndResource(0, _)) | Some(CountAndResource(1, _)) => {
            pl.cmd("DEL").arg(&res_key).arg(&count_key);
            PoolState::Empty
        }
        Some(CountAndResource(_, _)) => {
            pl.cmd("DECR").arg(&count_key);
            PoolState::NotEmpty
        }
    };

    Ok((conn, Tx::Continue(pl, state)))
}
