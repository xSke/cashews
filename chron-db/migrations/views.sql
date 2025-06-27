
-- lol
CREATE OR REPLACE FUNCTION println(str text) RETURNS integer
    LANGUAGE plpgsql AS
$$BEGIN
    RAISE NOTICE '%', str;
    RETURN 42;
END;$$;

select println('waiting for lock');

-- ensure we don't get trampled on by actively running ingest matview workers
select pg_advisory_xact_lock(0x13371337);

CREATE FUNCTION objectid_to_timestamp(text) RETURNS timestamptz
    AS 'select to_timestamp((''0x\'' || substring($1 from 1 for 8))::double);'
    LANGUAGE SQL
    IMMUTABLE
    PARALLEL SAFE
    RETURNS NULL ON NULL INPUT;

drop materialized view if exists game_player_stats_exploded cascade;
drop materialized view if exists game_player_stats_league_aggregate cascade;
drop materialized view if exists game_player_stats_global_aggregate cascade;
drop materialized view if exists pitches cascade;

drop view if exists league_percentiles;
drop view if exists game_player_stats_advanced;

select println('creating players');
drop materialized view if exists players cascade;
create materialized view players as
    select
        lv.entity_id as player_id,
        (jt.first_name || ' ' || jt.last_name) as full_name,
        jt.* from latest_versions lv
    inner join objects using (hash)
    join lateral json_table(objects.data, '$[*]' columns (
        first_name text PATH '$.FirstName',
        last_name text PATH '$.LastName',
        team_id text PATH '$.TeamID',
        throws text PATH '$.Throws',
        bats text PATH '$.Bats',
        position text PATH '$.Position',
        position_type text PATH '$.PositionType',
        likes text PATH '$.Likes',
        dislikes text PATH '$.Dislikes',
        durability real PATH '$.Durability',
        augments smallint PATH '$.Augments',
        number smallint PATH '$.Number'
    )) jt on true
    where lv.kind = 4;
create unique index players_pkey_idx on players(player_id);

select println('creating team feeds');
drop materialized view if exists team_feeds cascade;
create materialized view team_feeds as
    select
        lv.entity_id as team_id,
        jt.* from latest_versions lv
    inner join objects using (hash)
    join lateral json_table(objects.data, '$.Feed[*]' columns (
        season smallint PATH '$.season',
        day smallint PATH '$.day',
        text text PATH '$.text',
        type text PATH '$.type',
        emoji text PATH '$.emoji',
        timestamp timestamptz PATH '$.ts',
        player_ids TEXT[] PATH '($.links[*] ? (@.type == "player")).id' WITH ARRAY WRAPPER,
        team_ids TEXT[] PATH '($.links[*] ? (@.type == "team")).id' WITH ARRAY WRAPPER,
        game_ids TEXT[] PATH '($.links[*] ? (@.type == "game")).id' WITH ARRAY WRAPPER
    )) jt on true
    where lv.kind = 3;
create unique index team_feeds_pkey_idx on team_feeds(team_id, timestamp);

select println('creating rosters');
drop materialized view if exists rosters cascade;
create materialized view rosters as
    select lv.entity_id as team_id, jt.* from latest_versions lv
    inner join objects using (hash)
    join lateral json_table(objects.data, '$.Players[*] ? (@.PlayerID != "#")' columns (
        player_id text PATH '$.PlayerID',
        slot_index for ordinality,
        slot text PATH '$.Slot'
    )) jt on true
    where lv.kind = 3;
create unique index rosters_pkey_idx on rosters(team_id, slot);

select println('creating roster slots');
drop materialized view if exists roster_slot_history;
create materialized view roster_slot_history as
    with
        roster_slot_versions as (
            select
                v.entity_id as team_id,
                jt.slot,
                jt.index,
                jt.player_id,
                v.valid_from,
                v.valid_to
            from versions v
                     inner join objects using (hash)
                     join lateral json_table(objects.data, '$.Players[*]' columns (
                index for ordinality,
                player_id text PATH '$.PlayerID',
                slot text PATH '$.Slot'
            )) jt on true
            where v.kind = 10
        ), slots_with_increment as (
        select
            *,
            case
                when (lag(player_id) over w) is distinct from player_id then 1
                end as version_increment
        from roster_slot_versions
        window w as (partition by team_id, slot order by valid_from rows between unbounded preceding and current row)
    ), slots_with_seq as (
        select
            *,
            count(version_increment) over w as seq
        from slots_with_increment
        window w as (partition by team_id, slot order by valid_from rows between unbounded preceding and current row)
    )
    select
        team_id,
        slot,
        seq,
        any_value(index) as slot_index,
        any_value(player_id) as player_id,
        min(valid_from) as valid_from,
        -- todo: should this contain nulls? how to make max do that right?
        max(coalesce(valid_to, 'infinity')) as valid_to
    from slots_with_seq
    group by team_id, slot, seq;
create unique index roster_slot_history_pkey_idx on roster_slot_history(team_id, slot, seq);
create index roster_slot_history_player_idx on roster_slot_history(player_id, valid_from, valid_to);

select println('creating game_player_stats_exploded');
create materialized view game_player_stats_exploded as
    select
        gps.season,
        gps.day,
        gps.game_id,
        gps.player_id,
        gps.team_id,
        rsh.slot,
        jt.*
    from game_player_stats gps
    join lateral json_table(data, '$[*]' columns (
        allowed_stolen_bases smallint PATH '$.allowed_stolen_bases' default 0 on empty default 0 on error,
        appearances smallint PATH '$.appearances' default 0 on empty default 0 on error,
        assists smallint PATH '$.assists' default 0 on empty default 0 on error,
        at_bats smallint PATH '$.at_bats' default 0 on empty default 0 on error,
        batters_faced smallint PATH '$.batters_faced' default 0 on empty default 0 on error,
        blown_saves smallint PATH '$.blown_saves' default 0 on empty default 0 on error,
        caught_double_play smallint PATH '$.caught_double_play' default 0 on empty default 0 on error,
        caught_stealing smallint PATH '$.caught_stealing' default 0 on empty default 0 on error,
        complete_games smallint PATH '$.complete_games' default 0 on empty default 0 on error,
        double_plays smallint PATH '$.double_plays' default 0 on empty default 0 on error,
        doubles smallint PATH '$.doubles' default 0 on empty default 0 on error,
        earned_runs smallint PATH '$.earned_runs' default 0 on empty default 0 on error,
        errors smallint PATH '$.errors' default 0 on empty default 0 on error,
        field_out smallint PATH '$.field_out' default 0 on empty default 0 on error,
        fielders_choice smallint PATH '$.fielders_choice' default 0 on empty default 0 on error,
        flyouts smallint PATH '$.flyouts' default 0 on empty default 0 on error,
        force_outs smallint PATH '$.force_outs' default 0 on empty default 0 on error,
        games_finished smallint PATH '$.games_finished' default 0 on empty default 0 on error,
        grounded_into_double_play smallint PATH '$.grounded_into_double_play' default 0 on empty default 0 on error,
        groundouts smallint PATH '$.groundouts' default 0 on empty default 0 on error,
        hit_batters smallint PATH '$.hit_batters' default 0 on empty default 0 on error,
        hit_by_pitch smallint PATH '$.hit_by_pitch' default 0 on empty default 0 on error,
        hits_allowed smallint PATH '$.hits_allowed' default 0 on empty default 0 on error,
        home_runs smallint PATH '$.home_runs' default 0 on empty default 0 on error,
        home_runs_allowed smallint PATH '$.home_runs_allowed' default 0 on empty default 0 on error,
        inherited_runners smallint PATH '$.inherited_runners' default 0 on empty default 0 on error,
        inherited_runs_allowed smallint PATH '$.inherited_runs_allowed' default 0 on empty default 0 on error,
        left_on_base smallint PATH '$.left_on_base' default 0 on empty default 0 on error,
        lineouts smallint PATH '$.lineouts' default 0 on empty default 0 on error,
        losses smallint PATH '$.losses' default 0 on empty default 0 on error,
        mound_visits smallint PATH '$.mound_visits' default 0 on empty default 0 on error,
        no_hitters smallint PATH '$.no_hitters' default 0 on empty default 0 on error,
        outs smallint PATH '$.outs' default 0 on empty default 0 on error,
        perfect_games smallint PATH '$.perfect_games' default 0 on empty default 0 on error,
        pitches_thrown smallint PATH '$.pitches_thrown' default 0 on empty default 0 on error,
        plate_appearances smallint PATH '$.plate_appearances' default 0 on empty default 0 on error,
        popouts smallint PATH '$.popouts' default 0 on empty default 0 on error,
        putouts smallint PATH '$.putouts' default 0 on empty default 0 on error,
        quality_starts smallint PATH '$.quality_starts' default 0 on empty default 0 on error,
        reached_on_error smallint PATH '$.reached_on_error' default 0 on empty default 0 on error,
        runners_caught_stealing smallint PATH '$.runners_caught_stealing' default 0 on empty default 0 on error,
        runs smallint PATH '$.runs' default 0 on empty default 0 on error,
        runs_batted_in smallint PATH '$.runs_batted_in' default 0 on empty default 0 on error,
        sac_flies smallint PATH '$.sac_flies' default 0 on empty default 0 on error,
        sacrifice_double_plays smallint PATH '$.sacrifice_double_plays' default 0 on empty default 0 on error,
        saves smallint PATH '$.saves' default 0 on empty default 0 on error,
        shutouts smallint PATH '$.shutouts' default 0 on empty default 0 on error,
        singles smallint PATH '$.singles' default 0 on empty default 0 on error,
        starts smallint PATH '$.starts' default 0 on empty default 0 on error,
        stolen_bases smallint PATH '$.stolen_bases' default 0 on empty default 0 on error,
        strikeouts smallint PATH '$.strikeouts' default 0 on empty default 0 on error,
        struck_out smallint PATH '$.struck_out' default 0 on empty default 0 on error,
        triples smallint PATH '$.triples' default 0 on empty default 0 on error,
        unearned_runs smallint PATH '$.unearned_runs' default 0 on empty default 0 on error,
        walked smallint PATH '$.walked' default 0 on empty default 0 on error,
        walks smallint PATH '$.walks' default 0 on empty default 0 on error,
        wins smallint PATH '$.wins' default 0 on empty default 0 on error
    )) as jt on true
    left join public.roster_slot_history rsh on (
        rsh.player_id = gps.player_id and
        rsh.team_id = gps.team_id and
        rsh.valid_from <= objectid_to_timestamp(gps.game_id) and
        rsh.valid_to >= objectid_to_timestamp(gps.game_id)
    );
create unique index game_player_stats_exploded_pkey on game_player_stats_exploded(game_id, team_id, player_id);

select println('creating game_player_stats_exploded indexes');
create index game_player_stats_exploded_by_season_day_idx on game_player_stats_exploded(season, day);
create index game_player_stats_exploded_by_player_idx on game_player_stats_exploded(player_id, season, day);
create index game_player_stats_exploded_by_team_idx on game_player_stats_exploded(team_id, season, day);

-- todo: express a lot of these views in terms of `game_player_stats_exploded` instead
create or replace view game_player_stats_advanced as
with helper as (
    select
        coalesce((data->'allowed_stolen_bases')::int, 0) as allowed_stolen_bases,
        coalesce((data->'allowed_stolen_bases_risp')::int, 0) as allowed_stolen_bases_risp,
        coalesce((data->'appearances')::int, 0) as appearances,
        coalesce((data->'assists')::int, 0) as assists,
        coalesce((data->'assists_risp')::int, 0) as assists_risp,
        coalesce((data->'at_bats')::int, 0) as at_bats,
        coalesce((data->'at_bats_risp')::int, 0) as at_bats_risp,
        coalesce((data->'batters_faced')::int, 0) as batters_faced,
        coalesce((data->'batters_faced_risp')::int, 0) as batters_faced_risp,
        coalesce((data->'blown_saves')::int, 0) as blown_saves,
        coalesce((data->'caught_double_play')::int, 0) as caught_double_play,
        coalesce((data->'caught_double_play_risp')::int, 0) as caught_double_play_risp,
        coalesce((data->'caught_stealing')::int, 0) as caught_stealing,
        coalesce((data->'caught_stealing_risp')::int, 0) as caught_stealing_risp,
        coalesce((data->'complete_games')::int, 0) as complete_games,
        coalesce((data->'double_plays')::int, 0) as double_plays,
        coalesce((data->'double_plays_risp')::int, 0) as double_plays_risp,
        coalesce((data->'doubles')::int, 0) as doubles,
        coalesce((data->'doubles_risp')::int, 0) as doubles_risp,
        coalesce((data->'earned_runs')::int, 0) as earned_runs,
        coalesce((data->'earned_runs_risp')::int, 0) as earned_runs_risp,
        coalesce((data->'errors')::int, 0) as errors,
        coalesce((data->'errors_risp')::int, 0) as errors_risp,
        coalesce((data->'field_out')::int, 0) as field_out,
        coalesce((data->'field_out_risp')::int, 0) as field_out_risp,
        coalesce((data->'fielders_choice')::int, 0) as fielders_choice,
        coalesce((data->'fielders_choice_risp')::int, 0) as fielders_choice_risp,
        coalesce((data->'flyouts')::int, 0) as flyouts,
        coalesce((data->'flyouts_risp')::int, 0) as flyouts_risp,
        coalesce((data->'force_outs')::int, 0) as force_outs,
        coalesce((data->'force_outs_risp')::int, 0) as force_outs_risp,
        coalesce((data->'games_finished')::int, 0) as games_finished,
        coalesce((data->'grounded_into_double_play')::int, 0) as grounded_into_double_play,
        coalesce((data->'grounded_into_double_play_risp')::int, 0) as grounded_into_double_play_risp,
        coalesce((data->'groundout')::int, 0) as groundout,
        coalesce((data->'groundout_risp')::int, 0) as groundout_risp,
        coalesce((data->'hit_batters')::int, 0) as hit_batters,
        coalesce((data->'hit_batters_risp')::int, 0) as hit_batters_risp,
        coalesce((data->'hit_by_pitch')::int, 0) as hit_by_pitch,
        coalesce((data->'hit_by_pitch_risp')::int, 0) as hit_by_pitch_risp,
        coalesce((data->'hits_allowed')::int, 0) as hits_allowed,
        coalesce((data->'hits_allowed_risp')::int, 0) as hits_allowed_risp,
        coalesce((data->'home_runs')::int, 0) as home_runs,
        coalesce((data->'home_runs_allowed')::int, 0) as home_runs_allowed,
        coalesce((data->'home_runs_allowed_risp')::int, 0) as home_runs_allowed_risp,
        coalesce((data->'home_runs_risp')::int, 0) as home_runs_risp,
        coalesce((data->'inherited_runners')::int, 0) as inherited_runners,
        coalesce((data->'inherited_runners_risp')::int, 0) as inherited_runners_risp,
        coalesce((data->'inherited_runs_allowed')::int, 0) as inherited_runs_allowed,
        coalesce((data->'inherited_runs_allowed_risp')::int, 0) as inherited_runs_allowed_risp,
        coalesce((data->'left_on_base')::int, 0) as left_on_base,
        coalesce((data->'left_on_base_risp')::int, 0) as left_on_base_risp,
        coalesce((data->'lineouts')::int, 0) as lineouts,
        coalesce((data->'lineouts_risp')::int, 0) as lineouts_risp,
        coalesce((data->'losses')::int, 0) as losses,
        coalesce((data->'mound_visits')::int, 0) as mound_visits,
        coalesce((data->'no_hitters')::int, 0) as no_hitters,
        coalesce((data->'outs')::int, 0) as outs,
        coalesce((data->'perfect_games')::int, 0) as perfect_games,
        coalesce((data->'pitches_thrown')::int, 0) as pitches_thrown,
        coalesce((data->'pitches_thrown_risp')::int, 0) as pitches_thrown_risp,
        coalesce((data->'plate_appearances')::int, 0) as plate_appearances,
        coalesce((data->'plate_appearances_risp')::int, 0) as plate_appearances_risp,
        coalesce((data->'popouts')::int, 0) as popouts,
        coalesce((data->'popouts_risp')::int, 0) as popouts_risp,
        coalesce((data->'putouts')::int, 0) as putouts,
        coalesce((data->'putouts_risp')::int, 0) as putouts_risp,
        coalesce((data->'quality_starts')::int, 0) as quality_starts,
        coalesce((data->'reached_on_error')::int, 0) as reached_on_error,
        coalesce((data->'reached_on_error_risp')::int, 0) as reached_on_error_risp,
        coalesce((data->'runners_caught_stealing')::int, 0) as runners_caught_stealing,
        coalesce((data->'runners_caught_stealing_risp')::int, 0) as runners_caught_stealing_risp,
        coalesce((data->'runs')::int, 0) as runs,
        coalesce((data->'runs_batted_in')::int, 0) as runs_batted_in,
        coalesce((data->'runs_batted_in_risp')::int, 0) as runs_batted_in_risp,
        coalesce((data->'runs_risp')::int, 0) as runs_risp,
        coalesce((data->'sac_flies')::int, 0) as sac_flies,
        coalesce((data->'sac_flies_risp')::int, 0) as sac_flies_risp,
        coalesce((data->'sacrifice_double_plays')::int, 0) as sacrifice_double_plays,
        coalesce((data->'sacrifice_double_plays_risp')::int, 0) as sacrifice_double_plays_risp,
        coalesce((data->'saves')::int, 0) as saves,
        coalesce((data->'shutouts')::int, 0) as shutouts,
        coalesce((data->'singles')::int, 0) as singles,
        coalesce((data->'singles_risp')::int, 0) as singles_risp,
        coalesce((data->'starts')::int, 0) as starts,
        coalesce((data->'stolen_bases')::int, 0) as stolen_bases,
        coalesce((data->'stolen_bases_risp')::int, 0) as stolen_bases_risp,
        coalesce((data->'strikeouts')::int, 0) as strikeouts,
        coalesce((data->'strikeouts_risp')::int, 0) as strikeouts_risp,
        coalesce((data->'struck_out')::int, 0) as struck_out,
        coalesce((data->'struck_out_risp')::int, 0) as struck_out_risp,
        coalesce((data->'triples')::int, 0) as triples,
        coalesce((data->'triples_risp')::int, 0) as triples_risp,
        coalesce((data->'unearned_runs')::int, 0) as unearned_runs,
        coalesce((data->'unearned_runs_risp')::int, 0) as unearned_runs_risp,
        coalesce((data->'walked')::int, 0) as walked,
        coalesce((data->'walked_risp')::int, 0) as walked_risp,
        coalesce((data->'walks')::int, 0) as walks,
        coalesce((data->'walks_risp')::int, 0) as walks_risp,
        coalesce((data->'wins')::int, 0) as wins,

        coalesce((data->'singles')::int, 0) + coalesce((data->'doubles')::int, 0) + coalesce((data->'triples')::int, 0) + coalesce((data->'home_runs')::int, 0) as hits,
        (coalesce((data->'outs')::real, 0) / 3) as ip,
        game_player_stats.*
    from game_player_stats
)
select
    h.season,
    h.player_id,
    h.team_id,
    -- (select league_id from teams t where t.team_id = h.team_id limit 1),

    sum(plate_appearances) as plate_appearances,
    sum(at_bats) as at_bats,

    sum(hits) as hits,
    sum(doubles) as doubles,
    sum(triples) as triples,
    sum(home_runs) as home_runs,
    sum(walked) as walked,
    sum(struck_out) as struck_out,
    sum(sac_flies) as sac_flies,

    ((sum(hits)::real) / nullif(sum(at_bats)::real, 0)) as ba,
    (sum(hits + walked + hit_by_pitch)::real / nullif(sum(plate_appearances)::real, 0)) as obp,
    (sum(singles + doubles * 2 + triples * 3 + home_runs * 4)::real / nullif(sum(at_bats)::real, 0)) as slg,
    (sum(hits + walked + hit_by_pitch)::real / nullif(sum(plate_appearances)::real, 0) + sum(singles + doubles * 2 + triples * 3 + home_runs * 4)::real / nullif(sum(at_bats)::real, 0)) as ops,
    (sum(singles + doubles + triples)::real / nullif(sum(at_bats - struck_out + sac_flies)::real, 0)) as babip,

    sum(appearances) as appearances,
    sum(starts) as starts,
    sum(ip) as ip,
    sum(outs) as outs,

    sum(strikeouts) as strikeouts,
    sum(walks) as walks,
    sum(hits_allowed) as hits_allowed,
    sum(home_runs_allowed) as home_runs_allowed,
    sum(hit_batters) as hit_batters,
    sum(earned_runs) as earned_runs,
    sum(runs) as runs,

    ((9 * sum(earned_runs))::real / nullif(sum(ip), 0)) as era,
    (sum(walks + hits_allowed)::real / nullif(sum(ip), 0)) as whip,
    (sum(9 * home_runs_allowed)::real / nullif(sum(ip), 0)) as hr9,
    (sum(9 * walks)::real / nullif(sum(ip), 0)) as bb9,
    (sum(9 * strikeouts)::real / nullif(sum(ip), 0)) as k9,
    (sum(9 * hits_allowed)::real / nullif(sum(ip), 0)) as h9,
    (sum(13 * home_runs_allowed + 3 * (walks + hit_batters) - 2 * strikeouts)::real / nullif(sum(ip), 0)) as fip_base,

    sum(stolen_bases) as stolen_bases,
    sum(caught_stealing) as caught_stealing,
    sum(stolen_bases + caught_stealing) as sb_attempts,
    (sum(stolen_bases)::real / nullif(sum(stolen_bases + caught_stealing)::real, 0)) as sb_success,

    sum(errors) as errors,
    (sum(putouts + assists)::real / nullif(sum(putouts + assists + errors)::real, 0)) as fpct
from helper h
group by (season, h.player_id, h.team_id);

-- Materialized view for calculating league-average stats, for use in normalized statistics and the like. Averages over
-- individual leagues, so there will be a bit of variation in how OPS translates to OPS+ for different Lesser Leagues,
-- if you use this. Join this to the team info on league_id to select the relevant league average.
-- particular note: FIP constant is implicitly defined in here: use fip_const = era - fip_base to get it\
select println('creating game_player_stats_league_aggregate');
create materialized view if not exists game_player_stats_league_aggregate as
with helper as (
    select
        coalesce((data->'allowed_stolen_bases')::int, 0) as allowed_stolen_bases,
        coalesce((data->'allowed_stolen_bases_risp')::int, 0) as allowed_stolen_bases_risp,
        coalesce((data->'appearances')::int, 0) as appearances,
        coalesce((data->'assists')::int, 0) as assists,
        coalesce((data->'assists_risp')::int, 0) as assists_risp,
        coalesce((data->'at_bats')::int, 0) as at_bats,
        coalesce((data->'at_bats_risp')::int, 0) as at_bats_risp,
        coalesce((data->'batters_faced')::int, 0) as batters_faced,
        coalesce((data->'batters_faced_risp')::int, 0) as batters_faced_risp,
        coalesce((data->'blown_saves')::int, 0) as blown_saves,
        coalesce((data->'caught_double_play')::int, 0) as caught_double_play,
        coalesce((data->'caught_double_play_risp')::int, 0) as caught_double_play_risp,
        coalesce((data->'caught_stealing')::int, 0) as caught_stealing,
        coalesce((data->'caught_stealing_risp')::int, 0) as caught_stealing_risp,
        coalesce((data->'complete_games')::int, 0) as complete_games,
        coalesce((data->'double_plays')::int, 0) as double_plays,
        coalesce((data->'double_plays_risp')::int, 0) as double_plays_risp,
        coalesce((data->'doubles')::int, 0) as doubles,
        coalesce((data->'doubles_risp')::int, 0) as doubles_risp,
        coalesce((data->'earned_runs')::int, 0) as earned_runs,
        coalesce((data->'earned_runs_risp')::int, 0) as earned_runs_risp,
        coalesce((data->'errors')::int, 0) as errors,
        coalesce((data->'errors_risp')::int, 0) as errors_risp,
        coalesce((data->'field_out')::int, 0) as field_out,
        coalesce((data->'field_out_risp')::int, 0) as field_out_risp,
        coalesce((data->'fielders_choice')::int, 0) as fielders_choice,
        coalesce((data->'fielders_choice_risp')::int, 0) as fielders_choice_risp,
        coalesce((data->'flyouts')::int, 0) as flyouts,
        coalesce((data->'flyouts_risp')::int, 0) as flyouts_risp,
        coalesce((data->'force_outs')::int, 0) as force_outs,
        coalesce((data->'force_outs_risp')::int, 0) as force_outs_risp,
        coalesce((data->'games_finished')::int, 0) as games_finished,
        coalesce((data->'grounded_into_double_play')::int, 0) as grounded_into_double_play,
        coalesce((data->'grounded_into_double_play_risp')::int, 0) as grounded_into_double_play_risp,
        coalesce((data->'groundout')::int, 0) as groundout,
        coalesce((data->'groundout_risp')::int, 0) as groundout_risp,
        coalesce((data->'hit_batters')::int, 0) as hit_batters,
        coalesce((data->'hit_batters_risp')::int, 0) as hit_batters_risp,
        coalesce((data->'hit_by_pitch')::int, 0) as hit_by_pitch,
        coalesce((data->'hit_by_pitch_risp')::int, 0) as hit_by_pitch_risp,
        coalesce((data->'hits_allowed')::int, 0) as hits_allowed,
        coalesce((data->'hits_allowed_risp')::int, 0) as hits_allowed_risp,
        coalesce((data->'home_runs')::int, 0) as home_runs,
        coalesce((data->'home_runs_allowed')::int, 0) as home_runs_allowed,
        coalesce((data->'home_runs_allowed_risp')::int, 0) as home_runs_allowed_risp,
        coalesce((data->'home_runs_risp')::int, 0) as home_runs_risp,
        coalesce((data->'inherited_runners')::int, 0) as inherited_runners,
        coalesce((data->'inherited_runners_risp')::int, 0) as inherited_runners_risp,
        coalesce((data->'inherited_runs_allowed')::int, 0) as inherited_runs_allowed,
        coalesce((data->'inherited_runs_allowed_risp')::int, 0) as inherited_runs_allowed_risp,
        coalesce((data->'left_on_base')::int, 0) as left_on_base,
        coalesce((data->'left_on_base_risp')::int, 0) as left_on_base_risp,
        coalesce((data->'lineouts')::int, 0) as lineouts,
        coalesce((data->'lineouts_risp')::int, 0) as lineouts_risp,
        coalesce((data->'losses')::int, 0) as losses,
        coalesce((data->'mound_visits')::int, 0) as mound_visits,
        coalesce((data->'no_hitters')::int, 0) as no_hitters,
        coalesce((data->'outs')::int, 0) as outs,
        coalesce((data->'perfect_games')::int, 0) as perfect_games,
        coalesce((data->'pitches_thrown')::int, 0) as pitches_thrown,
        coalesce((data->'pitches_thrown_risp')::int, 0) as pitches_thrown_risp,
        coalesce((data->'plate_appearances')::int, 0) as plate_appearances,
        coalesce((data->'plate_appearances_risp')::int, 0) as plate_appearances_risp,
        coalesce((data->'popouts')::int, 0) as popouts,
        coalesce((data->'popouts_risp')::int, 0) as popouts_risp,
        coalesce((data->'putouts')::int, 0) as putouts,
        coalesce((data->'putouts_risp')::int, 0) as putouts_risp,
        coalesce((data->'quality_starts')::int, 0) as quality_starts,
        coalesce((data->'reached_on_error')::int, 0) as reached_on_error,
        coalesce((data->'reached_on_error_risp')::int, 0) as reached_on_error_risp,
        coalesce((data->'runners_caught_stealing')::int, 0) as runners_caught_stealing,
        coalesce((data->'runners_caught_stealing_risp')::int, 0) as runners_caught_stealing_risp,
        coalesce((data->'runs')::int, 0) as runs,
        coalesce((data->'runs_batted_in')::int, 0) as runs_batted_in,
        coalesce((data->'runs_batted_in_risp')::int, 0) as runs_batted_in_risp,
        coalesce((data->'runs_risp')::int, 0) as runs_risp,
        coalesce((data->'sac_flies')::int, 0) as sac_flies,
        coalesce((data->'sac_flies_risp')::int, 0) as sac_flies_risp,
        coalesce((data->'sacrifice_double_plays')::int, 0) as sacrifice_double_plays,
        coalesce((data->'sacrifice_double_plays_risp')::int, 0) as sacrifice_double_plays_risp,
        coalesce((data->'saves')::int, 0) as saves,
        coalesce((data->'shutouts')::int, 0) as shutouts,
        coalesce((data->'singles')::int, 0) as singles,
        coalesce((data->'singles_risp')::int, 0) as singles_risp,
        coalesce((data->'starts')::int, 0) as starts,
        coalesce((data->'stolen_bases')::int, 0) as stolen_bases,
        coalesce((data->'stolen_bases_risp')::int, 0) as stolen_bases_risp,
        coalesce((data->'strikeouts')::int, 0) as strikeouts,
        coalesce((data->'strikeouts_risp')::int, 0) as strikeouts_risp,
        coalesce((data->'struck_out')::int, 0) as struck_out,
        coalesce((data->'struck_out_risp')::int, 0) as struck_out_risp,
        coalesce((data->'triples')::int, 0) as triples,
        coalesce((data->'triples_risp')::int, 0) as triples_risp,
        coalesce((data->'unearned_runs')::int, 0) as unearned_runs,
        coalesce((data->'unearned_runs_risp')::int, 0) as unearned_runs_risp,
        coalesce((data->'walked')::int, 0) as walked,
        coalesce((data->'walked_risp')::int, 0) as walked_risp,
        coalesce((data->'walks')::int, 0) as walks,
        coalesce((data->'walks_risp')::int, 0) as walks_risp,
        coalesce((data->'wins')::int, 0) as wins,

        coalesce((data->'singles')::int, 0) + coalesce((data->'doubles')::int, 0) + coalesce((data->'triples')::int, 0) + coalesce((data->'home_runs')::int, 0) as hits,
        (coalesce((data->'outs')::real, 0) / 3) as ip,
        game_player_stats.*
    from game_player_stats
)
select
    season,
    league_id,
    -- (select league_id from teams t where t.team_id = h.team_id limit 1),

    sum(outs / 3) as ip,

    sum(plate_appearances) as plate_appearances,
    sum(at_bats) as at_bats,

    ((sum(hits)::real) / nullif(sum(at_bats)::real, 0)) as ba,
    (sum(hits + walked + hit_by_pitch)::real / nullif(sum(plate_appearances)::real, 0)) as obp,
    (sum(singles + doubles * 2 + triples * 3 + home_runs * 4)::real / nullif(sum(at_bats)::real, 0)) as slg,
    (sum(hits + walked + hit_by_pitch)::real / nullif(sum(plate_appearances)::real, 0) + sum(singles + doubles * 2 + triples * 3 + home_runs * 4)::real / nullif(sum(at_bats)::real, 0)) as ops,

    ((9 * sum(earned_runs))::real / nullif(sum(ip), 0)) as era,
    (sum(walks + hits_allowed)::real / nullif(sum(ip), 0)) as whip,
    (sum(9 * home_runs_allowed)::real / nullif(sum(ip), 0)) as hr9,
    (sum(9 * walks)::real / nullif(sum(ip), 0)) as bb9,
    (sum(9 * strikeouts)::real / nullif(sum(ip), 0)) as k9,
    (sum(9 * hits_allowed)::real / nullif(sum(ip), 0)) as h9,
    (sum(13 * home_runs_allowed + 3 * (walks + hit_batters) - 2 * strikeouts)::real / nullif(sum(ip), 0)) as fip_base,

    sum(stolen_bases + caught_stealing) as sb_attempts,
    (sum(stolen_bases)::real / nullif(sum(stolen_bases + caught_stealing)::real, 0)) as sb_success,

    (sum(singles + doubles + triples)::real / nullif(sum(at_bats - struck_out + sac_flies)::real, 0)) as babip,
    (sum(putouts + assists)::real / nullif(sum(putouts + assists + errors)::real, 0)) as fpct
from helper h
         inner join teams t on (t.team_id = h.team_id)
group by (season, league_id)
with data;
create unique index game_player_stats_league_aggregate_idx on game_player_stats_league_aggregate(season, league_id);

-- Materialized view for calculating globally averaged stats. Unlike game_player_stats_league_aggregate, does not group
-- by league, so BA here is (total hits across every league in MMOLB) / (total at-bats across every league in MMOLB) for
-- a given season.
-- particular note: FIP constant is implicitly defined in here: use fip_const = era - fip_base to get it
select println('creating game_player_stats_global_aggregate');
create materialized view if not exists game_player_stats_global_aggregate as
with helper as (
    select
        coalesce((data->'allowed_stolen_bases')::int, 0) as allowed_stolen_bases,
        coalesce((data->'allowed_stolen_bases_risp')::int, 0) as allowed_stolen_bases_risp,
        coalesce((data->'appearances')::int, 0) as appearances,
        coalesce((data->'assists')::int, 0) as assists,
        coalesce((data->'assists_risp')::int, 0) as assists_risp,
        coalesce((data->'at_bats')::int, 0) as at_bats,
        coalesce((data->'at_bats_risp')::int, 0) as at_bats_risp,
        coalesce((data->'batters_faced')::int, 0) as batters_faced,
        coalesce((data->'batters_faced_risp')::int, 0) as batters_faced_risp,
        coalesce((data->'blown_saves')::int, 0) as blown_saves,
        coalesce((data->'caught_double_play')::int, 0) as caught_double_play,
        coalesce((data->'caught_double_play_risp')::int, 0) as caught_double_play_risp,
        coalesce((data->'caught_stealing')::int, 0) as caught_stealing,
        coalesce((data->'caught_stealing_risp')::int, 0) as caught_stealing_risp,
        coalesce((data->'complete_games')::int, 0) as complete_games,
        coalesce((data->'double_plays')::int, 0) as double_plays,
        coalesce((data->'double_plays_risp')::int, 0) as double_plays_risp,
        coalesce((data->'doubles')::int, 0) as doubles,
        coalesce((data->'doubles_risp')::int, 0) as doubles_risp,
        coalesce((data->'earned_runs')::int, 0) as earned_runs,
        coalesce((data->'earned_runs_risp')::int, 0) as earned_runs_risp,
        coalesce((data->'errors')::int, 0) as errors,
        coalesce((data->'errors_risp')::int, 0) as errors_risp,
        coalesce((data->'field_out')::int, 0) as field_out,
        coalesce((data->'field_out_risp')::int, 0) as field_out_risp,
        coalesce((data->'fielders_choice')::int, 0) as fielders_choice,
        coalesce((data->'fielders_choice_risp')::int, 0) as fielders_choice_risp,
        coalesce((data->'flyouts')::int, 0) as flyouts,
        coalesce((data->'flyouts_risp')::int, 0) as flyouts_risp,
        coalesce((data->'force_outs')::int, 0) as force_outs,
        coalesce((data->'force_outs_risp')::int, 0) as force_outs_risp,
        coalesce((data->'games_finished')::int, 0) as games_finished,
        coalesce((data->'grounded_into_double_play')::int, 0) as grounded_into_double_play,
        coalesce((data->'grounded_into_double_play_risp')::int, 0) as grounded_into_double_play_risp,
        coalesce((data->'groundout')::int, 0) as groundout,
        coalesce((data->'groundout_risp')::int, 0) as groundout_risp,
        coalesce((data->'hit_batters')::int, 0) as hit_batters,
        coalesce((data->'hit_batters_risp')::int, 0) as hit_batters_risp,
        coalesce((data->'hit_by_pitch')::int, 0) as hit_by_pitch,
        coalesce((data->'hit_by_pitch_risp')::int, 0) as hit_by_pitch_risp,
        coalesce((data->'hits_allowed')::int, 0) as hits_allowed,
        coalesce((data->'hits_allowed_risp')::int, 0) as hits_allowed_risp,
        coalesce((data->'home_runs')::int, 0) as home_runs,
        coalesce((data->'home_runs_allowed')::int, 0) as home_runs_allowed,
        coalesce((data->'home_runs_allowed_risp')::int, 0) as home_runs_allowed_risp,
        coalesce((data->'home_runs_risp')::int, 0) as home_runs_risp,
        coalesce((data->'inherited_runners')::int, 0) as inherited_runners,
        coalesce((data->'inherited_runners_risp')::int, 0) as inherited_runners_risp,
        coalesce((data->'inherited_runs_allowed')::int, 0) as inherited_runs_allowed,
        coalesce((data->'inherited_runs_allowed_risp')::int, 0) as inherited_runs_allowed_risp,
        coalesce((data->'left_on_base')::int, 0) as left_on_base,
        coalesce((data->'left_on_base_risp')::int, 0) as left_on_base_risp,
        coalesce((data->'lineouts')::int, 0) as lineouts,
        coalesce((data->'lineouts_risp')::int, 0) as lineouts_risp,
        coalesce((data->'losses')::int, 0) as losses,
        coalesce((data->'mound_visits')::int, 0) as mound_visits,
        coalesce((data->'no_hitters')::int, 0) as no_hitters,
        coalesce((data->'outs')::int, 0) as outs,
        coalesce((data->'perfect_games')::int, 0) as perfect_games,
        coalesce((data->'pitches_thrown')::int, 0) as pitches_thrown,
        coalesce((data->'pitches_thrown_risp')::int, 0) as pitches_thrown_risp,
        coalesce((data->'plate_appearances')::int, 0) as plate_appearances,
        coalesce((data->'plate_appearances_risp')::int, 0) as plate_appearances_risp,
        coalesce((data->'popouts')::int, 0) as popouts,
        coalesce((data->'popouts_risp')::int, 0) as popouts_risp,
        coalesce((data->'putouts')::int, 0) as putouts,
        coalesce((data->'putouts_risp')::int, 0) as putouts_risp,
        coalesce((data->'quality_starts')::int, 0) as quality_starts,
        coalesce((data->'reached_on_error')::int, 0) as reached_on_error,
        coalesce((data->'reached_on_error_risp')::int, 0) as reached_on_error_risp,
        coalesce((data->'runners_caught_stealing')::int, 0) as runners_caught_stealing,
        coalesce((data->'runners_caught_stealing_risp')::int, 0) as runners_caught_stealing_risp,
        coalesce((data->'runs')::int, 0) as runs,
        coalesce((data->'runs_batted_in')::int, 0) as runs_batted_in,
        coalesce((data->'runs_batted_in_risp')::int, 0) as runs_batted_in_risp,
        coalesce((data->'runs_risp')::int, 0) as runs_risp,
        coalesce((data->'sac_flies')::int, 0) as sac_flies,
        coalesce((data->'sac_flies_risp')::int, 0) as sac_flies_risp,
        coalesce((data->'sacrifice_double_plays')::int, 0) as sacrifice_double_plays,
        coalesce((data->'sacrifice_double_plays_risp')::int, 0) as sacrifice_double_plays_risp,
        coalesce((data->'saves')::int, 0) as saves,
        coalesce((data->'shutouts')::int, 0) as shutouts,
        coalesce((data->'singles')::int, 0) as singles,
        coalesce((data->'singles_risp')::int, 0) as singles_risp,
        coalesce((data->'starts')::int, 0) as starts,
        coalesce((data->'stolen_bases')::int, 0) as stolen_bases,
        coalesce((data->'stolen_bases_risp')::int, 0) as stolen_bases_risp,
        coalesce((data->'strikeouts')::int, 0) as strikeouts,
        coalesce((data->'strikeouts_risp')::int, 0) as strikeouts_risp,
        coalesce((data->'struck_out')::int, 0) as struck_out,
        coalesce((data->'struck_out_risp')::int, 0) as struck_out_risp,
        coalesce((data->'triples')::int, 0) as triples,
        coalesce((data->'triples_risp')::int, 0) as triples_risp,
        coalesce((data->'unearned_runs')::int, 0) as unearned_runs,
        coalesce((data->'unearned_runs_risp')::int, 0) as unearned_runs_risp,
        coalesce((data->'walked')::int, 0) as walked,
        coalesce((data->'walked_risp')::int, 0) as walked_risp,
        coalesce((data->'walks')::int, 0) as walks,
        coalesce((data->'walks_risp')::int, 0) as walks_risp,
        coalesce((data->'wins')::int, 0) as wins,

        coalesce((data->'singles')::int, 0) + coalesce((data->'doubles')::int, 0) + coalesce((data->'triples')::int, 0) + coalesce((data->'home_runs')::int, 0) as hits,
        (coalesce((data->'outs')::real, 0) / 3) as ip,
        game_player_stats.*
    from game_player_stats
)
select
    season,

    sum(outs / 3) as ip,

    sum(plate_appearances) as plate_appearances,
    sum(at_bats) as at_bats,

    ((sum(hits)::real) / nullif(sum(at_bats)::real, 0)) as ba,
    (sum(hits + walked + hit_by_pitch)::real / nullif(sum(plate_appearances)::real, 0)) as obp,
    (sum(singles + doubles * 2 + triples * 3 + home_runs * 4)::real / nullif(sum(at_bats)::real, 0)) as slg,
    (sum(hits + walked + hit_by_pitch)::real / nullif(sum(plate_appearances)::real, 0) + sum(singles + doubles * 2 + triples * 3 + home_runs * 4)::real / nullif(sum(at_bats)::real, 0)) as ops,

    ((9 * sum(earned_runs))::real / nullif(sum(ip), 0)) as era,
    (sum(walks + hits_allowed)::real / nullif(sum(ip), 0)) as whip,
    (sum(9 * home_runs_allowed)::real / nullif(sum(ip), 0)) as hr9,
    (sum(9 * walks)::real / nullif(sum(ip), 0)) as bb9,
    (sum(9 * strikeouts)::real / nullif(sum(ip), 0)) as k9,
    (sum(9 * hits_allowed)::real / nullif(sum(ip), 0)) as h9,
    (sum(13 * home_runs_allowed + 3 * (walks + hit_batters) - 2 * strikeouts)::real / nullif(sum(ip), 0)) as fip_base,

    sum(stolen_bases + caught_stealing) as sb_attempts,
    (sum(stolen_bases)::real / nullif(sum(stolen_bases + caught_stealing)::real, 0)) as sb_success,

    (sum(singles + doubles + triples)::real / nullif(sum(at_bats - struck_out + sac_flies)::real, 0)) as babip,
    (sum(putouts + assists)::real / nullif(sum(putouts + assists + errors)::real, 0)) as fpct
from helper h
         inner join teams t on (t.team_id = h.team_id)
group by (season)
with data;
create unique index game_player_stats_global_aggregate_idx on game_player_stats_global_aggregate(season);

drop function if exists league_percentiles;
create or replace function league_percentiles(real[]) returns table(
                                                                       season int,
                                                                       league_id text,

                                                                       ba real[],
                                                                       obp real[],
                                                                       slg real[],
                                                                       ops real[],
                                                                       sb_success real[],
                                                                       era real[],
                                                                       whip real[],
                                                                       fip_base real[],
                                                                       fip_const real[],
                                                                       h9 real[],
                                                                       k9 real[],
                                                                       bb9 real[],
                                                                       hr9 real[]
                                                                   )
AS $$
select
    season,
    league_id,

    percentile_cont($1) within group (order by h.ba) filter (where h.plate_appearances > 20) as ba,
    percentile_cont($1) within group (order by h.obp) filter (where h.plate_appearances > 20) as obp,
    percentile_cont($1) within group (order by h.slg) filter (where h.plate_appearances > 20) as slg,
    percentile_cont($1) within group (order by h.ops) filter (where h.plate_appearances > 20) as ops,
    percentile_cont($1) within group (order by h.sb_success) filter (where h.plate_appearances > 20) as sb_success,
    percentile_cont($1) within group (order by h.era desc) filter (where h.ip > 20) as era,
    percentile_cont($1) within group (order by h.whip desc) filter (where h.ip > 20) as whip,
    percentile_cont($1) within group (order by h.fip_base desc) filter (where h.ip > 20) as fip_base,
    percentile_cont($1) within group (order by h.era - h.fip_base desc) filter (where h.ip > 20) as fip_const,
    percentile_cont($1) within group (order by h.h9 desc) filter (where h.ip > 20) as h9,
    percentile_cont($1) within group (order by h.k9) filter (where h.ip > 20) as k9,
    percentile_cont($1) within group (order by h.bb9 desc) filter (where h.ip > 20) as bb9,
    percentile_cont($1) within group (order by h.hr9 desc) filter (where h.ip > 20) as hr9
from game_player_stats_advanced h
         inner join teams t on (t.team_id = h.team_id)
group by (season, t.league_id);
$$
    language sql;

drop function if exists global_percentiles;
create or replace function global_percentiles(real[]) returns table(
                                                                       season int,

                                                                       ba real[],
                                                                       obp real[],
                                                                       slg real[],
                                                                       ops real[],
                                                                       sb_success real[],
                                                                       era real[],
                                                                       whip real[],
                                                                       fip_base real[],
                                                                       fip_const real[],
                                                                       h9 real[],
                                                                       k9 real[],
                                                                       bb9 real[],
                                                                       hr9 real[]
                                                                   )
AS $$
select
    season,

    percentile_cont($1) within group (order by h.ba) filter (where h.plate_appearances > 20) as ba,
    percentile_cont($1) within group (order by h.obp) filter (where h.plate_appearances > 20) as obp,
    percentile_cont($1) within group (order by h.slg) filter (where h.plate_appearances > 20) as slg,
    percentile_cont($1) within group (order by h.ops) filter (where h.plate_appearances > 20) as ops,
    percentile_cont($1) within group (order by h.sb_success) filter (where h.plate_appearances > 20) as sb_success,
    percentile_cont($1) within group (order by h.era desc) filter (where h.ip > 20) as era,
    percentile_cont($1) within group (order by h.whip desc) filter (where h.ip > 20) as whip,
    percentile_cont($1) within group (order by h.fip_base desc) filter (where h.ip > 20) as fip_base,
    percentile_cont($1) within group (order by h.era - h.fip_base desc) filter (where h.ip > 20) as fip_const,
    percentile_cont($1) within group (order by h.h9 desc) filter (where h.ip > 20) as h9,
    percentile_cont($1) within group (order by h.k9) filter (where h.ip > 20) as k9,
    percentile_cont($1) within group (order by h.bb9 desc) filter (where h.ip > 20) as bb9,
    percentile_cont($1) within group (order by h.hr9 desc) filter (where h.ip > 20) as hr9
from game_player_stats_advanced h
         inner join teams t on (t.team_id = h.team_id)
group by (season);
$$
    language sql;

drop function if exists league_avgs;
create or replace function league_avgs() returns table(
                                                          season int,
                                                          league_id text,

                                                          ba real,
                                                          obp real,
                                                          slg real,
                                                          ops real,
                                                          sb_success real,
                                                          era real,
                                                          whip real,
                                                          fip_base real,
                                                          fip_const real,
                                                          h9 real,
                                                          k9 real,
                                                          bb9 real,
                                                          hr9 real
                                                      )
AS $$
select
    season,
    league_id,

    avg(ba) filter (where h.plate_appearances > 20) as ba,
    avg(obp) filter (where h.plate_appearances > 20) as obp,
    avg(slg) filter (where h.plate_appearances > 20) as slg,
    avg(ops) filter (where h.plate_appearances > 20) as ops,
    avg(sb_success) filter (where h.plate_appearances > 20) as sb_success,
    avg(era) filter (where h.ip > 20) as era,
    avg(whip) filter (where h.ip > 20) as whip,
    avg(fip_base) filter (where h.ip > 20) as fip_base,
    avg(era - fip_base) filter (where h.ip > 20) as fip_const,
    avg(h9) filter (where h.ip > 20) as h9,
    avg(k9) filter (where h.ip > 20) as k9,
    avg(bb9) filter (where h.ip > 20) as bb9,
    avg(hr9) filter (where h.ip > 20) as hr9
from game_player_stats_advanced h
         inner join teams t on (t.team_id = h.team_id)
group by (season, t.league_id);
$$
    language sql;

drop function if exists global_avgs;
create or replace function global_avgs() returns table(
                                                          season int,

                                                          ba real,
                                                          obp real,
                                                          slg real,
                                                          ops real,
                                                          sb_success real,
                                                          era real,
                                                          whip real,
                                                          fip_base real,
                                                          fip_const real,
                                                          h9 real,
                                                          k9 real,
                                                          bb9 real,
                                                          hr9 real
                                                      )
AS $$
select
    season,

    avg(ba) filter (where h.plate_appearances > 20) as ba,
    avg(obp) filter (where h.plate_appearances > 20) as obp,
    avg(slg) filter (where h.plate_appearances > 20) as slg,
    avg(ops) filter (where h.plate_appearances > 20) as ops,
    avg(sb_success) filter (where h.plate_appearances > 20) as sb_success,
    avg(era) filter (where h.ip > 20) as era,
    avg(whip) filter (where h.ip > 20) as whip,
    avg(fip_base) filter (where h.ip > 20) as fip_base,
    avg(era - fip_base) filter (where h.ip > 20) as fip_const,
    avg(h9) filter (where h.ip > 20) as h9,
    avg(k9) filter (where h.ip > 20) as k9,
    avg(bb9) filter (where h.ip > 20) as bb9,
    avg(hr9) filter (where h.ip > 20) as hr9
from game_player_stats_advanced h
         inner join teams t on (t.team_id = h.team_id)
group by (season);
$$
    language sql;

drop function if exists global_stddevs;
create or replace function global_stddevs() returns table(
                                                            season int,

                                                            ba real,
                                                            obp real,
                                                            slg real,
                                                            ops real,
                                                            sb_success real,
                                                            era real,
                                                            whip real,
                                                            fip_base real,
                                                            fip_const real,
                                                            h9 real,
                                                            k9 real,
                                                            bb9 real,
                                                            hr9 real
                                                        )
AS $$
select
    season,

    stddev_samp(ba) filter (where h.plate_appearances > 20) as ba,
    stddev_samp(obp) filter (where h.plate_appearances > 20) as obp,
    stddev_samp(slg) filter (where h.plate_appearances > 20) as slg,
    stddev_samp(ops) filter (where h.plate_appearances > 20) as ops,
    avg(sb_success) filter (where h.plate_appearances > 20) as sb_success,
    stddev_samp(era) filter (where h.ip > 20) as era,
    stddev_samp(whip) filter (where h.ip > 20) as whip,
    stddev_samp(fip_base) filter (where h.ip > 20) as fip_base,
    stddev_samp(era - fip_base) filter (where h.ip > 20) as fip_const,
    stddev_samp(h9) filter (where h.ip > 20) as h9,
    stddev_samp(k9) filter (where h.ip > 20) as k9,
    stddev_samp(bb9) filter (where h.ip > 20) as bb9,
    stddev_samp(hr9) filter (where h.ip > 20) as hr9
from game_player_stats_advanced h
         inner join teams t on (t.team_id = h.team_id)
group by (season);
$$
    language sql;

select println('creating pitches');
create materialized view if not exists pitches as
    select
        *,
        split_part(data->>'pitch_info', ' ', 1)::real as pitch_speed,
        split_part(data->>'pitch_info', 'MPH ', 2) as pitch_type,
        nullif(data->>'zone', '')::smallint as pitch_zone
    from game_events
        where pitcher_id is distinct from null and data->>'event' = 'Pitch';
select println('creating pitches index');
create unique index pitches_idx on pitches(game_id, index);