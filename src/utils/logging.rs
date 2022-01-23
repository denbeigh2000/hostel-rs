use log::{LevelFilter, SetLoggerError};
use simplelog::{ColorChoice, Config, TermLogger, TerminalMode};

pub fn init(level: LevelFilter) -> Result<(), SetLoggerError> {
    TermLogger::init(
        level,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )
}
