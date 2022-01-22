use log::LevelFilter;
use simplelog::{ColorChoice, Config, TermLogger, TerminalMode};

pub fn init(level: LevelFilter) {
    TermLogger::init(
        level,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )
    .unwrap();
}
