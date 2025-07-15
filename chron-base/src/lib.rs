use std::{fmt::Display, str::FromStr};

use anyhow::anyhow;
use config::Config;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumCount, IntoStaticStr, VariantArray};
use time::OffsetDateTime;
use unicode_normalization::UnicodeNormalization;

pub mod cache;

#[derive(Deserialize)]
pub struct ChronConfig {
    pub database_uri: String,
    pub maps_api_key: Option<String>,

    #[serde(default)]
    pub jitter: bool,
}

pub fn normalize_location(s: &str) -> String {
    s.to_lowercase().nfkc().to_string()
}

pub fn load_config() -> anyhow::Result<ChronConfig> {
    // maybe we shouldn't do this here idk
    // tracing_subscriber::fmt::init();
    tracing_subscriber::fmt().compact().without_time().init();

    let settings = Config::builder()
        .add_source(config::File::with_name("config"))
        .add_source(config::Environment::with_prefix("CHRON"))
        .build()?
        .try_deserialize()?;
    Ok(settings)
}

pub struct ObjectId([u8; 12]);

impl Display for ObjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl FromStr for ObjectId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // let engine = base64::engine::general_purpose::URL_SAFE;
        // let data = engine.decode(s)?;
        // if data.len() != 32 {
        //     return Err(anyhow::anyhow!("invalid page token"));
        // }

        // let timestamp_nanos = i128::from_le_bytes(data[0..16].try_into().unwrap());
        // let timestamp = OffsetDateTime::from_unix_timestamp_nanos(timestamp_nanos)?;
        // let entity_id = Uuid::from_slice(&data[16..32])?;

        // Ok(PageToken {
        //     entity_id,
        //     timestamp,
        // })
        Ok(todo!())
    }
}

impl Serialize for ObjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let str = String::deserialize(deserializer)?;
        ObjectId::from_str(&str).map_err(|_| serde::de::Error::custom("invalid object id"))
    }
}

pub fn objectid_to_timestamp(id: &str) -> anyhow::Result<OffsetDateTime> {
    if id.len() != 24 {
        return Err(anyhow!("not a valid objectid"));
    }

    let mut data = [0u8; 12];
    hex::decode_to_slice(id, &mut data)?;

    let unix_timestamp = u32::from_be_bytes(data[0..4].try_into().unwrap());
    Ok(OffsetDateTime::from_unix_timestamp(unix_timestamp as i64)?)
}

#[derive(
    Serialize,
    Deserialize,
    EnumCount,
    VariantArray,
    Display,
    IntoStaticStr,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Debug,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[repr(u8)]
pub enum StatKey {
    AllowedStolenBases = 0,
    Appearances = 1,
    Assists = 2,
    AtBats = 3,
    BattersFaced = 4,
    BlownSaves = 5,
    CaughtDoublePlay = 6,
    CaughtStealing = 7,
    CompleteGames = 8,
    DoublePlays = 9,
    Doubles = 10,
    EarnedRuns = 11,
    Errors = 12,
    FieldOut = 13,
    FieldersChoice = 14,
    Flyouts = 15,
    ForceOuts = 16,
    GamesFinished = 17,
    GroundedIntoDoublePlay = 18,
    Groundouts = 19,
    HitBatters = 20,
    HitByPitch = 21,
    HitsAllowed = 22,
    HomeRuns = 23,
    HomeRunsAllowed = 24,
    InheritedRunners = 25,
    InheritedRunsAllowed = 26,
    LeftOnBase = 27,
    Lineouts = 28,
    Losses = 29,
    MoundVisits = 30,
    NoHitters = 31,
    Outs = 32,
    PerfectGames = 33,
    PitchesThrown = 34,
    PlateAppearances = 35,
    Popouts = 36,
    Putouts = 37,
    QualityStarts = 38,
    ReachedOnError = 39,
    RunnersCaughtStealing = 40,
    Runs = 41,
    RunsBattedIn = 42,
    SacFlies = 43,
    SacrificeDoublePlays = 44,
    Saves = 45,
    Shutouts = 46,
    Singles = 47,
    Starts = 48,
    StolenBases = 49,
    Strikeouts = 50,
    StruckOut = 51,
    Triples = 52,
    UnearnedRuns = 53,
    Walked = 54,
    Walks = 55,
    Wins = 56,
}

pub async fn stop_signal() -> tokio::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::{self, unix::SignalKind};

        let mut int_fut = signal::unix::signal(SignalKind::interrupt())?;
        let mut term_fut = signal::unix::signal(SignalKind::terminate())?;

        tokio::select! {
            _ = int_fut.recv() => {},
            _ = term_fut.recv() => {}
        }

        Ok(())
    }

    #[cfg(not(unix))]
    {
        signal::ctrl_c().await
    }
}