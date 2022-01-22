# hostel

## What is this?
Small SSH server that creates shells in temporary containers instead of on the
bare host.

## How do I run this?
### Runtime arguments
```
$ ./target/release/hostel --help
hostel

USAGE:
    hostel [OPTIONS]

OPTIONS:
    -b, --bind-addr <BIND_ADDR>
            Socket address to bind server to [env: BIND_ADDR=] [default: 0.0.0.0:2222]

    -c, --config-dir <CONFIG_DIR>
            Path to hostel configuration directory [env: CONFIG_DIR=] [default: .]

    -h, --help
            Print help information

    -l, --log-level <LOG_LEVEL>
            Logging verbosity [env: LOG_LEVEL=] [default: info]

    -r, --redis-url <REDIS_URL>
            URL of redis address [env: REDIS_URL=] [default: redis://localhost:6379]

    -t, --ssh-auth-timeout-ms <SSH_AUTH_TIMEOUT_MS>
            SSH Auth rejection timeout (in ms) [env: SSH_AUTH_TIMEOUT_MS=] [default: 5000]
```

### General Configuration


Have a redis instance available for state management

Create a config directory like this:
```
$ tree sample_config
sample_config
├── config.yaml
├── id_ed25519
└── users
    ├── denbeigh
    │   └── authorized_keys
    └── fred
        ├── authorized_keys
        └── config.yaml
```

Example `config.yaml`:
```yaml
image: ubuntu:latest
```

User-level configuration files currently support overriding every parameter of
the global configuration file, but this may change.
