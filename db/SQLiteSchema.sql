create table songs (
    vID        text primary key,
    Title      text not null,
    STitle     text not null,
    ChannelID  text not null,
    Queries    text,
    DJable     integer not null default 1,
    SongVol    integer not null default 100,
    Duration   integer not null default 0,
    Qcount     integer not null default 0
);

create table history (
    Time       text primary key,
    vID        text not null,
    ServerID   text,
    ServerName text,
    Player     text
);