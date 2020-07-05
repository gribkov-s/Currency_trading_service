CREATE TABLE trading.currency_rates_current (
	datetime timestamp NULL,
	currency varchar(3) NULL,
	rate numeric(13,8) NULL,
	datetime_hour timestamp NULL
);

CREATE TABLE trading.currency_rates_daily (
	date timestamp NULL,
	currency varchar(3) NULL,
	rate numeric(13,8) NULL
);

CREATE TABLE trading.currency_rates_predicted (
	datetime timestamp NULL,
	currency varchar(3) NULL,
	rate numeric(13,8) NULL,
	datetime_hour timestamp NULL
);


CREATE TABLE trading.dim_currency (
	id int4 NULL,
	currency varchar(3) NULL,
	is_basic bool NULL,
	scale int NULL
);
