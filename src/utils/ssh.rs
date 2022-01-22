use thrussh_keys::key::{OpenSSLPKey, PublicKey};
use thrussh_libsodium::ed25519::PublicKey as Ed25519PKey;

pub fn clone_key(key: &PublicKey) -> PublicKey {
    match key {
        PublicKey::Ed25519(key) => {
            let key = key.key;
            PublicKey::Ed25519(Ed25519PKey { key })
        }
        PublicKey::RSA { key, hash } => {
            let key = OpenSSLPKey(key.0.clone());
            let hash = *hash;
            PublicKey::RSA { key, hash }
        }
    }
}
