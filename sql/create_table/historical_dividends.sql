create table if not exists fmp.historical_dividends (
symbol varchar,
"date" date ,
label varchar,
adj_dividend float,
dividend float,
record_date date,
payment_date date,
declaration_date date,
updated_time timestamp
)

