
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

CREATE OR REPLACE FUNCTION objectid_to_timestamp(text) RETURNS timestamptz
    AS 'select to_timestamp((''0x'' || substring($1 from 1 for 8))::double precision);'
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