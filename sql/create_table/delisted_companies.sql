create table if not exists fmp.delisted_companies (
symbol varchar,
company_name varchar ,
exchange varchar,
ipo_date date,
delisted_date date,
updated_time timestamp
)