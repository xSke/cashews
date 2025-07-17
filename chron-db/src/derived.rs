use async_stream::try_stream;
use chron_base::{StatKey, objectid_to_timestamp};
use compact_str::CompactString;
use futures::{Stream, TryStreamExt};
use sea_query::{Asterisk, Cond, Expr, ExprTrait, Func, PostgresQueryBuilder, Query};
use sea_query_binder::SqlxBinder;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Row, postgres::PgRow};
use strum::{EnumCount, IntoStaticStr, VariantArray};
use time::OffsetDateTime;

use crate::{
    ChronDb, Idens,
    models::{HasPageToken, PageToken},
    queries::{PaginatedResult, SortOrder, get_order, paginate_simple, with_page_token},
};

#[derive(Serialize, Deserialize, Debug, FromRow)]
pub struct DbGame {
    pub game_id: String,
    pub season: i32,
    pub day: i32,
    pub home_team_id: String,
    pub away_team_id: String,
    pub state: String,
    pub event_count: i32,
    pub last_update: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug, FromRow)]
pub struct DbGamePlayerStats {
    pub game_id: String,
    pub season: i16,
    pub day: i16,
    pub player_id: String,
    pub team_id: String,
    pub data: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug, FromRow)]
pub struct DbTeam {
    pub team_id: String,
    pub league_id: Option<String>,
    pub name: String,
    pub location: String,
    pub full_location: String,
    pub emoji: String,
    pub color: String,
    pub abbreviation: String,
}

#[derive(Serialize, Deserialize, Debug, FromRow)]
pub struct DbLeague {
    pub league_id: String,
    pub league_type: String,
    pub name: String,
    pub emoji: String,
    pub color: String,
}

impl HasPageToken for DbGame {
    fn page_token(&self) -> PageToken {
        // oh god oh no this is a mess
        PageToken {
            entity_id: self.game_id.clone(),
            timestamp: objectid_to_timestamp(&self.game_id).unwrap_or(OffsetDateTime::UNIX_EPOCH),
        }
    }
}

pub struct GetGamesQuery {
    pub season: Option<i32>,
    pub day: Option<i32>,
    pub team: Option<String>,
    pub count: u64,
    pub order: SortOrder,
    pub page: Option<PageToken>,
}

pub struct GetPlayerStatsQuery {
    pub start: Option<(i32, i32)>,
    pub end: Option<(i32, i32)>,
    pub player: Option<String>,
    pub team: Option<String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct StatFilter {
    #[serde(alias = ">")]
    gt: Option<u32>,
    #[serde(alias = "<")]
    lt: Option<u32>,
    #[serde(alias = "=")]
    eq: Option<u32>,
    #[serde(alias = "<=")]
    lte: Option<u32>,
    #[serde(alias = ">=")]
    gte: Option<u32>,
}

#[derive(Clone)]
pub struct StatsQueryNew {
    pub start: Option<(i32, i32)>,
    pub end: Option<(i32, i32)>,
    pub player: Option<String>,
    pub team: Option<String>,
    pub league: Option<String>,
    pub game: Option<String>,
    pub slot: Option<SlotOrPosition>,

    pub group_league: bool,
    pub group_team: bool,
    pub group_player: bool,
    pub group_season: bool,
    pub group_day: bool,
    pub group_game: bool,
    pub group_slot: bool,
    pub group_player_name: bool,

    pub sort: Option<StatKey>,
    pub count: Option<u64>,
    pub include_names: bool,

    pub fields: Vec<StatKey>,
    pub filters: Vec<(StatKey, StatFilter)>,
}

#[derive(sqlx::Type, Debug, Clone, Copy, Serialize, Deserialize, IntoStaticStr, PartialEq, Eq)]
#[sqlx(type_name = "text")]
pub enum SlotOrPosition {
    #[sqlx(rename = "1B")]
    #[serde(rename = "1B")]
    FirstB,
    #[sqlx(rename = "2B")]
    #[serde(rename = "2B")]
    SecondB,
    #[sqlx(rename = "3B")]
    #[serde(rename = "3B")]
    ThirdB,
    C,
    CF,
    CL,
    DH,
    LF,
    RF,
    RP,
    RP1,
    RP2,
    RP3,
    SP,
    SP1,
    SP2,
    SP3,
    SP4,
    SP5,
    SS,
}

#[derive(Debug, Clone)]
pub struct StatsRow {
    pub player: Option<CompactString>,
    pub player_name: Option<CompactString>,
    pub game: Option<CompactString>,
    pub team: Option<CompactString>,
    pub team_name: Option<CompactString>,
    pub league: Option<CompactString>,
    pub season: Option<i16>,
    pub day: Option<i16>,
    pub slot: Option<SlotOrPosition>,
    pub values: [u32; StatKey::COUNT],
}

impl FromRow<'_, PgRow> for StatsRow {
    fn from_row(row: &PgRow) -> sqlx::Result<Self> {
        let mut values = [0u32; StatKey::COUNT];
        for (i, sk) in StatKey::VARIANTS.iter().enumerate() {
            let name: &'static str = sk.into();
            // dbg!(i, name, row.try_get::<i32, _>(name));
            values[i] = row.try_get::<i32, _>(name).unwrap_or(0) as u32;
        }

        let team_location: Option<CompactString> = row.try_get("team_location").ok();
        let team_name: Option<CompactString> = row.try_get("team_name").ok();

        let team_name =
            if let (Some(mut team_location), Some(team_name)) = (team_location, team_name) {
                // reuse this buffer and reshadow
                team_location.push(' ');
                team_location.push_str(&team_name);
                Some(team_location)
            } else {
                None
            };

        Ok(Self {
            // column may not exist in the result set if it wasn't requested
            // but we should return None, not an error, in that case
            season: row.try_get("season").ok(),
            day: row.try_get("day").ok(),
            game: row.try_get("game_id").ok(),

            player: row.try_get("player_id").ok(),
            player_name: row.try_get("player_name").ok(),
            team: row.try_get("team_id").ok(),
            team_name,
            league: row.try_get("league_id").ok(),
            slot: row.try_get("slot").ok(),

            values: values,
        })
    }
}

#[derive(FromRow, Debug, Clone, Serialize)]
pub struct PercentileStats {
    pub season: i32,
    pub league_id: Option<String>,
    pub percentile: f32,

    pub ba: f32,
    pub obp: f32,
    pub slg: f32,
    pub ops: f32,
    pub sb_success: f32,
    pub era: f32,
    pub whip: f32,
    pub fip_base: f32,
    pub fip_const: f32,
    pub h9: f32,
    pub k9: f32,
    pub bb9: f32,
    pub hr9: f32,
}

#[derive(Serialize, Deserialize, Debug, FromRow)]
pub struct AverageStats {
    pub season: i16,

    #[serde(default)]
    pub league_id: Option<String>,

    pub ip: i64,
    pub plate_appearances: i64,
    pub at_bats: i64,
    pub ba: f32,
    pub obp: f32,
    pub slg: f32,
    pub ops: f32,
    pub era: f64,
    pub whip: f64,
    pub hr9: f64,
    pub bb9: f64,
    pub k9: f64,
    pub h9: f64,
    pub fip_base: f64,
    pub sb_attempts: i64,
    pub sb_success: f32,
    pub babip: f32,
    pub fpct: f32,
}

impl ChronDb {
    pub async fn get_teams(&self) -> anyhow::Result<Vec<DbTeam>> {
        let res = sqlx::query_as("select * from teams")
            .fetch_all(&self.pool)
            .await?;

        Ok(res)
    }

    pub async fn get_leagues(&self) -> anyhow::Result<Vec<DbLeague>> {
        let res = sqlx::query_as("select * from leagues")
            .fetch_all(&self.pool)
            .await?;

        Ok(res)
    }

    pub async fn get_games(&self, q: GetGamesQuery) -> anyhow::Result<PaginatedResult<DbGame>> {
        let mut qq = Query::select()
            .expr(Expr::col((Idens::Games, Asterisk)))
            .from(Idens::Games)
            .order_by_columns([(Idens::GameId, get_order(q.order))])
            .limit(q.count)
            .to_owned();

        if let Some(season) = q.season {
            qq = qq.and_where(Expr::col(Idens::Season).eq(season)).to_owned();
        }

        if let Some(day) = q.day {
            qq = qq.and_where(Expr::col(Idens::Day).eq(day)).to_owned();
        }

        if let Some(team) = q.team {
            qq = qq
                .and_where(
                    Expr::col(Idens::HomeTeamId)
                        .eq(&team)
                        .or(Expr::col(Idens::AwayTeamId).eq(&team)),
                )
                .to_owned();
        }

        if let Some(page) = q.page {
            qq = qq
                .and_where(paginate_simple(q.order, Idens::GameId, page))
                .to_owned();
        }

        let (q, vals) = qq.build_sqlx(PostgresQueryBuilder);
        let res = sqlx::query_as_with(&q, vals).fetch_all(&self.pool).await?;
        Ok(with_page_token(res))
    }

    pub async fn get_league_percentiles(
        &self,
        percentiles: &[f32],
        season: i32,
    ) -> anyhow::Result<Vec<PercentileStats>> {
        let mut q = String::new();
        q.push_str("select season, league_id,");

        let cols = "ba obp slg ops sb_success era whip fip_base fip_const h9 k9 bb9 hr9";
        for col in cols.split_ascii_whitespace() {
            q.push_str(&format!("unnest({}) as {}, ", col, col));
        }
        q.push_str(" unnest($1) as percentile from league_percentiles($1::real[]) where ");

        for col in cols.split_ascii_whitespace() {
            q.push_str(&format!("{} is distinct from null and ", col));
        }
        q.push_str(" season = $2");

        let res = sqlx::query_as(&q)
            .bind(&percentiles)
            .bind(season)
            .fetch_all(&self.pool)
            .await?;
        Ok(res)
    }

    pub async fn get_league_averages(&self, season: i16) -> anyhow::Result<Vec<AverageStats>> {
        let res =
            sqlx::query_as("select * from game_player_stats_league_aggregate where season = $1")
                .bind(season)
                .fetch_all(&self.pool)
                .await?;

        Ok(res)
    }

    pub async fn get_player_stats(
        &self,
        q: GetPlayerStatsQuery,
    ) -> anyhow::Result<Vec<DbGamePlayerStats>> {
        let mut qq = Query::select()
            .expr(Expr::col((Idens::GamePlayerStats, Asterisk)))
            .from(Idens::GamePlayerStats)
            .to_owned();

        if let Some((s, d)) = q.start {
            qq = qq
                .and_where(
                    Expr::tuple([
                        Expr::col(Idens::Season).into(),
                        Expr::col(Idens::Day).into(),
                    ])
                    .gte(Expr::tuple([Expr::value(s as i16), Expr::value(d as i16)])),
                )
                .to_owned();
        }

        if let Some((s, d)) = q.end {
            qq = qq
                .and_where(
                    Expr::tuple([
                        Expr::col(Idens::Season).into(),
                        Expr::col(Idens::Day).into(),
                    ])
                    .lte(Expr::tuple([Expr::value(s as i16), Expr::value(d as i16)])),
                )
                .to_owned();
        }

        if let Some(player) = q.player {
            qq = qq
                .and_where(Expr::col(Idens::PlayerId).eq(&player))
                .to_owned();
        }

        if let Some(team) = q.team {
            qq = qq.and_where(Expr::col(Idens::TeamId).eq(&team)).to_owned();
        }

        let (q, vals) = qq.build_sqlx(PostgresQueryBuilder);
        let res = sqlx::query_as_with(&q, vals).fetch_all(&self.pool).await?;
        Ok(res)
    }

    pub fn get_stats(
        &self,
        q: StatsQueryNew,
    ) -> anyhow::Result<impl Stream<Item = Result<StatsRow, anyhow::Error>> + use<'_>> {
        let mut qqq = Query::select()
            .from(Idens::GamePlayerStatsExploded)
            .to_owned();

        let mut qq: &mut _ = &mut qqq;
        // todo: can prob clean this up. or just use a better damn query builder
        for x in &q.fields {
            let name: &'static str = x.into();
            qq = qq.expr_as(Expr::sum(Expr::col(name)).cast_as("int"), name);
        }

        if let Some(count) = q.count {
            qq = qq.limit(count);
        }

        let mut needs_teams_table_join = false;
        let mut needs_players_table_join = false;
        let mut needs_player_name_expr = false;

        if let Some(player) = q.player {
            qq = qq.and_where(
                Expr::col((Idens::GamePlayerStatsExploded, Idens::PlayerId)).eq(&player),
            );
        }

        if let Some(team) = q.team {
            qq = qq.and_where(Expr::col((Idens::GamePlayerStatsExploded, Idens::TeamId)).eq(&team));
        }

        if let Some(league) = q.league {
            needs_teams_table_join = true;
            qq = qq.and_where(Expr::col((Idens::Teams, Idens::LeagueId)).eq(&league));
        }

        if let Some(game) = q.game {
            qq = qq.and_where(Expr::col(Idens::GameId).eq(&game));
        }

        if let Some(slot) = q.slot {
            let name: &'static str = slot.into();
            qq = qq.and_where(Expr::col(Idens::Slot).eq(name));
        }

        if q.group_day {
            qq = qq
                .group_by_columns([Idens::Season, Idens::Day])
                .column(Idens::Season)
                .column(Idens::Day);
        } else if q.group_season {
            qq = qq.group_by_col(Idens::Season).column(Idens::Season);
        }

        if q.group_player {
            qq = qq
                .group_by_col((Idens::GamePlayerStatsExploded, Idens::PlayerId))
                .column((Idens::GamePlayerStatsExploded, Idens::PlayerId));

            if q.include_names && !q.group_player_name {
                let full_name = Func::cust(Idens::AnyValue).arg(Expr::col((
                    Idens::GamePlayerStatsExploded,
                    Idens::PlayerName,
                )));
                qq = qq.expr_as(full_name, "player_name");
            }
        }

        if q.group_team {
            qq = qq
                .group_by_col((Idens::GamePlayerStatsExploded, Idens::TeamId))
                .column((Idens::GamePlayerStatsExploded, Idens::TeamId));

            if q.include_names {
                needs_teams_table_join = true;
                let location =
                    Func::cust(Idens::AnyValue).arg(Expr::col((Idens::Teams, Idens::Location)));
                let name = Func::cust(Idens::AnyValue).arg(Expr::col((Idens::Teams, Idens::Name)));
                // let full_name = location.concat(" ").concat(name);
                // slightly faster to do the concat in FromRow rather than in postgres
                // but really we should just alter that table into having a `full_name` column...
                qq = qq.expr_as(location, "team_location");
                qq = qq.expr_as(name, "team_name");
            }
        }

        if q.group_league {
            needs_teams_table_join = true;
            qq = qq.group_by_col(Idens::LeagueId).column(Idens::LeagueId);
        }

        if q.group_game {
            qq = qq.group_by_col(Idens::GameId).column(Idens::GameId);
        }

        if q.group_slot {
            qq = qq.group_by_col(Idens::Slot).column(Idens::Slot);
        }

        if q.group_player_name {
            qq = qq.group_by_col(Idens::PlayerName).column(Idens::PlayerName);
        }

        if let Some((s, d)) = q.start {
            qq = qq.and_where(
                Expr::tuple([
                    Expr::col(Idens::Season).into(),
                    Expr::col(Idens::Day).into(),
                ])
                .gte(Expr::tuple([Expr::value(s as i16), Expr::value(d as i16)])),
            );
        }

        if let Some((s, d)) = q.end {
            qq = qq.and_where(
                Expr::tuple([
                    Expr::col(Idens::Season).into(),
                    Expr::col(Idens::Day).into(),
                ])
                .lte(Expr::tuple([Expr::value(s as i16), Expr::value(d as i16)])),
            );
        }

        if needs_teams_table_join {
            qq = qq.left_join(
                Idens::Teams,
                Expr::col((Idens::GamePlayerStatsExploded, Idens::TeamId))
                    .equals((Idens::Teams, Idens::TeamId)),
            );
        }

        if needs_players_table_join {
            qq = qq.left_join(
                Idens::Players,
                Expr::col((Idens::GamePlayerStatsExploded, Idens::PlayerId))
                    .equals((Idens::Players, Idens::PlayerId)),
            );
        }

        let mut nonzero_cond = Cond::any();
        for x in &q.fields {
            let name: &'static str = x.into();
            nonzero_cond = nonzero_cond.add(Expr::col(name).sum().gt(0));
        }
        qq = qq.cond_having(nonzero_cond);

        if let Some(sort) = q.sort {
            let name: &'static str = sort.into();
            qq = qq.order_by_expr(Expr::col(name).sum(), sea_query::Order::Desc);
        }

        for (filter_key, filter) in q.filters {
            let name: &'static str = filter_key.into();
            if let Some(v) = filter.eq {
                qq = qq.and_having(Expr::col(name).sum().eq(v));
            }
            if let Some(v) = filter.lt {
                qq = qq.and_having(Expr::col(name).sum().lt(v));
            }
            if let Some(v) = filter.gt {
                qq = qq.and_having(Expr::col(name).sum().gt(v));
            }
            if let Some(v) = filter.lte {
                qq = qq.and_having(Expr::col(name).sum().lte(v));
            }
            if let Some(v) = filter.gte {
                qq = qq.and_having(Expr::col(name).sum().gte(v));
            }
        }

        let (q, vals) = qq.build_sqlx(PostgresQueryBuilder);

        tracing::info!("generated query: {}, {:?}", &q, &vals);
        let s = try_stream! {
            let mut rows = sqlx::query_as_with(&q, vals).fetch(&self.pool);

            while let Some(row) = rows.try_next().await? {
                yield row;
            }
        };

        Ok(Box::pin(s))
    }

    pub async fn update_game(&self, game: DbGameSaveModel<'_>) -> anyhow::Result<()> {
        sqlx::query("insert into games (game_id, season, day, home_team_id, away_team_id, state, event_count, last_update) values ($1, $2, $3, $4, $5, $6, $7, $8) on conflict (game_id) do update set state = excluded.state, event_count = excluded.event_count, last_update = excluded.last_update")
            .bind(game.game_id)
            .bind(game.season)
            .bind(game.day)
            .bind(game.home_team_id)
            .bind(game.away_team_id)
            .bind(game.state)
            .bind(game.event_count)
            .bind(game.last_update)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_game_events(
        &self,
        game_id: &str,
        season: i32,
        day: i32,
        timestamp: &OffsetDateTime,

        event_indexes: &[i32],
        event_datas: &[&serde_json::Value],
        event_pitchers: &[Option<String>],
        event_batters: &[Option<String>],
    ) -> anyhow::Result<()> {
        assert!(event_indexes.len() == event_datas.len());
        assert!(event_indexes.len() == event_pitchers.len());
        assert!(event_indexes.len() == event_batters.len());

        let chunk_size = 100;
        for i in (0..event_indexes.len()).step_by(chunk_size) {
            sqlx::query("insert into game_events (game_id, index, data, pitcher_id, batter_id, observed_at, season, day) select $1 as game_id, unnest($2::int[]) as index, unnest($3::jsonb[]) as data, unnest($4::text[]) as pitcher_id, unnest($5::text[]) as batter_id, $6 as observed_at, $7 as season, $8 as day on conflict (game_id, index) do update set observed_at = excluded.observed_at, pitcher_id = excluded.pitcher_id, batter_id = excluded.batter_id, season = excluded.season, day = excluded.day where (game_events.observed_at is null or excluded.observed_at <= game_events.observed_at)")
                .bind(game_id)
                .bind(&event_indexes[i..(i+chunk_size).min(event_indexes.len())])
                .bind(&event_datas[i..(i+chunk_size).min(event_indexes.len())])
                .bind(&event_pitchers[i..(i+chunk_size).min(event_indexes.len())])
                .bind(&event_batters[i..(i+chunk_size).min(event_indexes.len())])
                .bind(timestamp)
                .bind(season as i16)
                .bind(day as i16)
                .execute(&self.pool).await?;
        }

        Ok(())
    }

    pub async fn update_game_player_stats(
        &self,
        game_id: &str,
        season: i32,
        day: i32,
        stats: &[(&str, &str, &serde_json::Value)],
    ) -> anyhow::Result<()> {
        let mut team_ids = Vec::with_capacity(stats.len());
        let mut player_ids = Vec::with_capacity(stats.len());
        let mut datas = Vec::with_capacity(stats.len());
        for (team_id, player_id, data) in stats {
            team_ids.push(*team_id);
            player_ids.push(*player_id);
            datas.push(data);
        }

        sqlx::query("insert into game_player_stats (game_id, season, day, team_id, player_id, data) select $1 as game_id, $2 as season, $3 as day, unnest($4::text[]) as team_id, unnest($5::text[]) as player_id, unnest($6::jsonb[]) as data on conflict (game_id, team_id, player_id) do update set data=excluded.data")
            .bind(game_id)
            .bind(season)
            .bind(day)
            .bind(&team_ids)
            .bind(&player_ids)
            .bind(&datas)
            .execute(&self.pool).await?;

        Ok(())
    }

    pub async fn update_team(&self, team: DbTeamSaveModel<'_>) -> anyhow::Result<()> {
        sqlx::query("insert into teams (team_id, league_id, location, name, full_location, emoji, color, abbreviation) values ($1, $2, $3, $4, $5, $6, $7, $8) on conflict (team_id) do update set league_id = excluded.league_id, location = excluded.location, name = excluded.name, full_location = excluded.full_location, emoji = excluded.emoji, color = excluded.color, abbreviation = excluded.abbreviation")
            .bind(team.team_id)
            .bind(team.league_id)
            .bind(team.location)
            .bind(team.name)
            .bind(team.full_location)
            .bind(team.emoji)
            .bind(team.color)
            .bind(team.abbreviation)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn update_league(&self, league: DbLeagueSaveModel<'_>) -> anyhow::Result<()> {
        sqlx::query("insert into leagues (league_id, league_type, name, color, emoji) values ($1, $2, $3, $4, $5) on conflict (league_id) do update set league_type = excluded.league_type, name = excluded.name, color = excluded.color, emoji = excluded.emoji")
            .bind(league.league_id)
            .bind(league.league_type)
            .bind(league.name)
            .bind(league.color)
            .bind(league.emoji)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_all_team_ids_from_stats(&self) -> anyhow::Result<Vec<String>> {
        Ok(
            sqlx::query_scalar("select distinct team_id from game_player_stats")
                .fetch_all(&self.pool)
                .await?,
        )
    }

    pub async fn get_all_player_ids_from_stats(&self) -> anyhow::Result<Vec<String>> {
        Ok(
            sqlx::query_scalar("select distinct player_id from game_player_stats")
                .fetch_all(&self.pool)
                .await?,
        )
    }
}

pub struct DbTeamSaveModel<'a> {
    pub team_id: &'a str,
    pub league_id: Option<&'a str>,
    pub location: &'a str,
    pub name: &'a str,
    pub full_location: &'a str,
    pub emoji: &'a str,
    pub color: &'a str,
    pub abbreviation: &'a str,
}

pub struct DbLeagueSaveModel<'a> {
    pub league_id: &'a str,
    pub league_type: &'a str,
    pub name: &'a str,
    pub color: &'a str,
    pub emoji: &'a str,
}

pub struct DbGameSaveModel<'a> {
    pub game_id: &'a str,
    pub season: i32,
    pub day: i32,
    pub home_team_id: &'a str,
    pub away_team_id: &'a str,
    pub state: &'a str,
    pub event_count: i32,
    pub last_update: Option<&'a serde_json::Value>,
}
