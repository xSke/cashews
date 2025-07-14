create table player_name_map (
    timestamp timestamptz not null,
    player_id text not null,
    player_name text not null,
    primary key (player_id, timestamp)
);
create index player_name_map_timestamp_desc_idx on player_name_map(player_id, timestamp desc);