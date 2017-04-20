/*
  Create a user and database for our historical stock data
*/
CREATE USER merovingian WITH PASSWORD 'LeVrai';
ALTER ROLE merovingian WITH SUPERUSER;
CREATE DATABASE chateau OWNER merovingian;

/*
User psql to login into the Postgresql database

psql -d historical_stock_data -U merovingian

Depneding on how your Postrgresql cluster is set up, you may need to enter
your password. Once your logged into the the database, create a table for 
the historical stock data and copy our parsed data
*/

CREATE TABLE yahoo_data (
  ticker TEXT
, dt DATE
, open NUMERIC
, high NUMERIC
, low NUMERIC
, close NUMERIC
, volume NUMERIC
, adj_close NUMERIC
);


COPY yahoo_data FROM '/tmp/data.txt' WITH DELIMITER ',' CSV HEADER;

/*
historical_stock_data=# select * from yahoo_data limit 10;
 ticker |     dt     |    open    |    high    |    low     |   close    |  volume  | adj_close  
--------+------------+------------+------------+------------+------------+----------+------------
 AAPL   | 2017-02-17 | 135.100006 | 135.830002 | 135.100006 | 135.720001 | 22084500 | 135.720001
 AAPL   | 2017-02-16 | 135.669998 | 135.899994 | 134.839996 | 135.350006 | 22118000 | 135.350006
 AAPL   | 2017-02-15 | 135.520004 | 136.270004 | 134.619995 | 135.509995 | 35501600 | 135.509995
 AAPL   | 2017-02-14 | 133.470001 | 135.089996 |     133.25 | 135.020004 | 32815500 | 135.020004
 AAPL   | 2017-02-13 | 133.080002 | 133.820007 |     132.75 | 133.289993 | 23035400 | 133.289993
 AAPL   | 2017-02-10 | 132.460007 | 132.940002 | 132.050003 | 132.119995 | 20065500 | 132.119995
 AAPL   | 2017-02-09 | 131.649994 | 132.449997 | 131.119995 | 132.419998 | 28349900 | 132.419998
 AAPL   | 2017-02-08 | 131.350006 | 132.220001 | 131.220001 | 132.039993 | 23004100 | 131.469994
 AAPL   | 2017-02-07 | 130.539993 | 132.089996 | 130.449997 | 131.529999 | 38183800 | 130.962201
 AAPL   | 2017-02-06 | 129.130005 |     130.50 | 128.899994 | 130.289993 | 26845900 | 129.727549
(10 rows)
&/