import psycopg2
import numpy
import matplotlib.pyplot as plt
import pandas as pd
import math
from sqlalchemy import create_engine
from ggplot import *


conn = psycopg2.connect(dbname="ventris", host="192.168.1.119", user="ventris_admin", password="X4NAdu")
engine = create_engine('postgresql://ventris_admin:X4NAdu@192.168.1.119:5432/ventris')

def query(periods):
    sql = """
    SELECT *
    , SUM(pnl) OVER (PARTITION BY model_name ORDER BY dt) AS cum_pnl
    FROM
    (
        SELECT
        ' {periods}_day_regression_' || ticker as model_name
        , dt
        , ticker
        , CASE 
            WHEN (x * slope) + intercept > 0 
                THEN  next_day_ret
            WHEN (x * slope) + intercept < 0 
                THEN -1 * next_day_ret
            ELSE NULL
            END AS pnl
        FROM
        (
          SELECT *
          , percent_rank() OVER (PARTITION BY dt ORDER BY pvalue) AS p_rank
          FROM
          (
            SELECT * 
            , x * slope + intercept AS pvalue
            FROM 
            (
              SELECT 
                dt
              , ticker
              , ret AS x
              , next_day_ret
              , regr_slope(next_day_ret, ret) OVER deriv_wdw AS slope
              , regr_intercept(next_day_ret, ret) OVER deriv_wdw AS intercept
              FROM xlf_components_returns
              WHERE dt > '1999-01-01'
              WINDOW deriv_wdw AS 
                (PARTITION BY ticker ORDER BY dt ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
            ) a
          ) b
        ) c
    ) c;
    """
    return sql.format(periods=periods)


day_5_df = pd.read_sql_query(query(5), con=conn)

plot_title = "XLF Components Cum PNL"
p = ggplot(aes(x='dt', y='cum_pnl'), data=day_5_df)
g = p + geom_line() + \
    facet_wrap("ticker") + \
    ggtitle(plot_title)

t = theme_gray()
t._rcParams['font.size'] = 5

g + t


# How about if we combine the results? 

def query(periods):
    sql = """
    SELECT 
      dt
    , SUM(cum_pnl) / COUNT(ticker)  AS total_pnl -- Assumes equal allocation
    FROM 
    (
      SELECT *
      , SUM(pnl) OVER (PARTITION BY model_name ORDER BY dt) AS cum_pnl
      FROM
      (
          SELECT
          ' {periods}_day_regression_' || ticker as model_name
          , dt
          , ticker
          , CASE 
              WHEN (x * slope) + intercept > 0 
                  THEN  next_day_ret
              WHEN (x * slope) + intercept < 0 
                  THEN -1 * next_day_ret
              ELSE NULL
              END AS pnl
          FROM
          (
            SELECT 
              dt
            , ticker
            , ret AS x
            , next_day_ret
            , regr_slope(next_day_ret, ret) OVER deriv_wdw AS slope
            , regr_intercept(next_day_ret, ret) OVER deriv_wdw AS intercept
            FROM xlf_components_returns
            WHERE dt > '1999-01-01'
            WINDOW deriv_wdw AS 
              (PARTITION BY ticker ORDER BY dt ROWS BETWEEN {periods} PRECEDING AND 1 PRECEDING)
          ) b
      ) c
    ) d
    GROUP BY dt
    ORDER BY dt
    """
    return sql.format(periods=periods)


day_5_df = pd.read_sql_query(query(5), con=conn)
plot_title = "XLF components total pnl / Equal allocation / Simple strategie"
p = ggplot(aes(x='dt', y='total_pnl'), data=day_5_df)
p + geom_line() + ggtitle(plot_title)