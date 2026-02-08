use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Deserialize, Default)]
pub struct Config {
    pub semantic: Option<SemanticConfig>,
}

#[derive(Debug, Deserialize, Default)]
pub struct SemanticConfig {
    pub enabled: bool,
    pub model_path: Option<String>,
    pub pooling: Option<String>,
    pub query_prefix: Option<String>,
}

impl Config {
    pub fn load() -> anyhow::Result<Self> {
        let config_dir = dirs::config_dir().unwrap_or_else(|| PathBuf::from("."));
        let config_path = config_dir.join("remi").join("config.toml");
        
        if !config_path.exists() {
             return Ok(Self::default());
        }

        let content = std::fs::read_to_string(&config_path)
            .map_err(|e| anyhow::anyhow!("failed to read config file at {}: {}", config_path.display(), e))?;
            
        let config: Config = toml::from_str(&content)
            .map_err(|e| anyhow::anyhow!("failed to parse config file: {}", e))?;
            
        Ok(config)
    }
}
