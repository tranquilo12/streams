-- auto-generated definition
create table polygon_stocks_trades
(
    event_type       varchar,
    symbol_ticker    varchar,
    trade_id         integer,
    exchange_id      integer,
    price            double precision,
    trade_size       bigint,
    trade_conditions integer[],
    trade_timestamp  timestamp,
    tape             integer
);

alter table polygon_stocks_trades
    owner to postgres;

create unique index polygon_stocks_trades_event_type_symbol_ticker_exchange_id_trad
    on polygon_stocks_trades (event_type, symbol_ticker, exchange_id, trade_timestamp);

-- auto-generated definition
create table polygon_stocks_quotes
(
    event_type       varchar,
    symbol_ticker    varchar,
    quote_condition  integer,
    bid_exchange_id  integer,
    ask_exchange_id  integer,
    bid_price        double precision,
    ask_price        double precision,
    bid_size         integer,
    ask_size         integer,
    quote_timestamp  timestamp,
    tape             integer
);

alter table polygon_stocks_quotes
    owner to postgres;

create unique index polygon_stocks_quotes_uindex
    on polygon_stocks_quotes (event_type, symbol_ticker, bid_exchange_id, ask_exchange_id, quote_timestamp);

-- auto-generated definition
create table polygon_stocks_agg
(
    event_type       varchar,
    symbol_ticker    varchar,
    tick_volume  double precision,
    accumulated_volume double precision,
    official_opening_price double precision,
    vwap double precision,
    open double precision,
    close double precision,
    high double precision,
    low double precision,
    tick_average double precision,
    tape integer,
    n integer,
    start_timestamp timestamp,
    end_timestamp timestamp
);

alter table polygon_stocks_agg
    owner to postgres;

create unique index polygon_stocks_agg_uindex
    on polygon_stocks_agg (event_type, symbol_ticker, start_timestamp, end_timestamp);

create table polygon_stocks_bbo_quotes
(
    sip_timestamp      timestamp,
    exchange_timestamp timestamp,
    sequence_number    bigint,
    conditions         integer[],
    tape               integer,

    bid_price          double precision,
    bid_size           double precision,
    bid_exchange_id    bigint,

    ask_price          double precision,
    ask_size           double precision,
    ask_exchange_id    bigint,

    indicators         integer[]
);

alter table polygon_stocks_bbo_quotes
    owner to postgres;

create unique index polygon_stocks_bbo_quotes_ticker_sip_timestamp_exchange_timesta
    on polygon_stocks_bbo_quotes (sip_timestamp, exchange_timestamp, sequence_number);