use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hasher},
    sync::Arc,
};

use async_stream::try_stream;
use axum::{extract::State, http::HeaderValue, response::IntoResponse};
use axum_streams::{
    CsvStreamFormat, JsonArrayStreamFormat, JsonNewLineStreamFormat, StreamBodyAs,
    StreamBodyAsOptions,
};
use chron_base::StatKey;
use chron_db::derived::{SlotOrPosition, StatFilter, StatsQueryNew, StatsRow};
use futures::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize, Serializer, ser::SerializeStruct};
use serde_qs::axum::QsQuery;

use crate::{AppError, AppState, derived_api::SeasonDay};

use crate::chron_api::comma_separated2;

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq, PartialOrd, Ord, Copy, Debug)]
#[serde(rename_all = "snake_case")]
pub enum GroupColumn {
    Player,
    Team,
    League,
    Season,
    Day, // implies season
    Game,
    // Slot,
    PlayerName,
}

struct StatOutputRow {
    row: StatsRow,
    q: Arc<StatsRequest>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq, PartialOrd, Ord, Copy, Debug)]
#[serde(rename_all = "snake_case")]
pub enum StatsFormat {
    Csv,
    Json,
    Ndjson,
}

// need custom serde bullshit because of the "variable" amount of fields
// and i don't want to make a million hashmaps
impl Serialize for StatOutputRow {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut field_count = self.q.fields.len();
        if self.q.group.contains(&GroupColumn::League) {
            field_count += 1;
        }
        if self.q.group.contains(&GroupColumn::Team) {
            field_count += 1;
            if self.q.names {
                field_count += 1;
            }
        }
        if self.q.group.contains(&GroupColumn::Player) {
            field_count += 1;
        }
        if (self.q.group.contains(&GroupColumn::Player) && self.q.names)
            || self.q.group.contains(&GroupColumn::PlayerName)
        {
            field_count += 1;
        }
        if self.q.group.contains(&GroupColumn::Day) {
            field_count += 2;
        } else if self.q.group.contains(&GroupColumn::Season) {
            field_count += 1;
        }
        if self.q.group.contains(&GroupColumn::Game) {
            field_count += 1;
        }

        let mut state = serializer.serialize_struct("StatRow", field_count)?;
        if self.q.group.contains(&GroupColumn::Day) {
            state.serialize_field("season", &self.row.season.unwrap_or(0))?;
            state.serialize_field("day", &self.row.day.unwrap_or(0))?;
        } else if self.q.group.contains(&GroupColumn::Season) {
            state.serialize_field("season", &self.row.season.unwrap_or(0))?;
        }
        if self.q.group.contains(&GroupColumn::Game) {
            state.serialize_field("game_id", &self.row.game.as_deref())?;
        }
        if self.q.group.contains(&GroupColumn::Player) {
            state.serialize_field("player_id", &self.row.player.as_deref())?;
        }
        if (self.q.group.contains(&GroupColumn::Player) && self.q.names)
            || self.q.group.contains(&GroupColumn::PlayerName)
        {
            state.serialize_field("player_name", &self.row.player_name.as_deref())?;
        }
        // if self.q.group.contains(&GroupColumn::Slot) {
        //     state.serialize_field("slot", &self.row.slot)?;
        // }
        if self.q.group.contains(&GroupColumn::Team) {
            state.serialize_field("team_id", &self.row.team.as_deref())?;
            if self.q.names {
                state.serialize_field("team_name", &self.row.team_name.as_deref())?;
            }
        }
        if self.q.group.contains(&GroupColumn::League) {
            state.serialize_field("league_id", &self.row.league.as_deref())?;
        }
        for f in &self.q.fields {
            let name: &'static str = f.into();
            let value = self.row.values[*f as usize];
            state.serialize_field(name, &value)?;
        }

        state.end()
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct StatsRequest {
    pub start: Option<SeasonDay>,
    pub end: Option<SeasonDay>,
    pub season: Option<i32>,

    pub player: Option<String>,
    pub team: Option<String>,
    pub league: Option<String>,
    pub game: Option<String>,
    // pub slot: Option<SlotOrPosition>,
    #[serde(deserialize_with = "comma_separated2")]
    pub fields: Vec<StatKey>,

    #[serde(deserialize_with = "comma_separated2", default)]
    pub group: Vec<GroupColumn>,

    pub format: Option<StatsFormat>,

    // todo: rename to "order" or "sortby" or something?
    pub sort: Option<StatKey>,
    pub count: Option<u64>,

    #[serde(default)]
    pub filter: HashMap<StatKey, StatFilter>,

    #[serde(default)]
    pub names: bool,
}

pub async fn stats(
    State(ctx): State<AppState>,
    QsQuery(mut q): QsQuery<StatsRequest>,
) -> Result<impl IntoResponse, AppError> {
    dbg!(&q);
    let format = q.format.unwrap_or(StatsFormat::Csv);

    let count = q.count.unwrap_or(100_000).min(100_000);

    dedup_preserving_order(&mut q.fields);

    if let Some(season) = q.season {
        q.start = Some(SeasonDay::new(season, 0));
        q.end = Some(SeasonDay::new(season + 1, 0));
    }

    let q = Arc::new(q.clone());

    let qq = StatsQueryNew {
        start: q.start.map(Into::into),
        end: q.end.map(Into::into),
        player: q.player.clone(),
        team: q.team.clone(),
        league: q.league.clone(),
        game: q.game.clone(),
        slot: None,
        // slot: q.slot.clone(),
        group_league: q.group.contains(&GroupColumn::League),
        group_team: q.group.contains(&GroupColumn::Team),
        group_player: q.group.contains(&GroupColumn::Player),
        group_season: q.group.contains(&GroupColumn::Season),
        group_day: q.group.contains(&GroupColumn::Day),
        group_game: q.group.contains(&GroupColumn::Game),
        group_slot: false,
        // group_slot: q.group.contains(&GroupColumn::Slot),
        group_player_name: q.group.contains(&GroupColumn::PlayerName),
        sort: q.sort,
        count: Some(count),
        fields: q.fields.clone(),
        include_names: q.names,
        filters: q.filter.iter().map(|(k, v)| (*k, v.clone())).collect(),
    };

    let db = ctx.db.clone();

    let mut stream = db.get_stats(qq.clone())?;
    let results = stream
        .map_ok(|row| StatOutputRow { row, q: q.clone() })
        .try_collect::<Vec<_>>()
        .await?;

    // TODO: figure out some way to buffer the first few lines, check for errors, then start streaming...?
    // for now, we just buffer everything
    /*let s = try_stream! {
        let mut res = db.get_stats(qq.clone())?;
        while let Some(row) = res.try_next().await? {
            yield StatOutputRow { row: row, q: q.clone() };
        }
    }
    .inspect_err(|e| {
        tracing::error!("error in stats query: {:?}", e);
    })
    .map_err(|x: anyhow::Error| axum::Error::new(x));

    */

    let s = stream::iter(results).map(|x| -> Result<StatOutputRow, axum::Error> { Ok(x) });
    let opts = StreamBodyAsOptions::new().buffering_ready_items(1000);
    Ok(match format {
        StatsFormat::Csv => {
            StreamBodyAs::with_options(
                CsvStreamFormat::new(true, b','),
                s,
                // deliberately setting the wrong content type here(!!!)
                // i want it to display in the browser when possible
                opts.content_type(HeaderValue::from_static("text/plain; charset=utf-8")),
            )
        }
        StatsFormat::Json => StreamBodyAs::with_options(
            JsonArrayStreamFormat::new(),
            s,
            opts.content_type(HeaderValue::from_static("application/json; charset=utf-8")),
        ),
        StatsFormat::Ndjson => StreamBodyAs::with_options(
            JsonNewLineStreamFormat::new(),
            s,
            opts.content_type(HeaderValue::from_static(
                "application/x-ndjson; charset=utf-8",
            )),
        ),
    })
}

// way more generic than it needs to be
fn dedup_preserving_order<T: PartialEq + std::hash::Hash>(vec: &mut Vec<T>) {
    let mut seen = Vec::new();
    vec.retain(|x| {
        let mut hasher = DefaultHasher::new();
        T::hash(x, &mut hasher);
        let hash = hasher.finish();

        if seen.contains(&hash) {
            false
        } else {
            seen.push(hash);
            true
        }
    });
}
