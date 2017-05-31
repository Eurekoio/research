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
                WHEN pvalue > 0
                AND p_rank > .5
                    THEN  next_day_ret
                WHEN pvalue < 0 
                AND p_rank < .5
                    THEN -next_day_ret
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
                    (PARTITION BY ticker ORDER BY dt ROWS BETWEEN {periods} PRECEDING AND 1 PRECEDING)
                ) a
            ) b
            ) c
        ) d
    ) e
    GROUP BY dt
    ORDER BY dt;
    """
    return sql.format(periods=periods)


day_5_df = pd.read_sql_query(query(5), con=conn)
plot_title = "XLF components total pnl / Equal allocation / p_rank: 50:50 / Simple strategy"
p = ggplot(aes(x='dt', y='total_pnl'), data=day_5_df)
p + geom_line() + ggtitle(plot_title)


# Allocated PNL based on p_rank of p_value

def query(periods):
    sql = """
    SELECT
      dt
    , pnl
    , trade_count
    , allocated_pnl
    , SUM(allocated_pnl) OVER (ORDER BY dt) AS cum_pnl
    FROM
    (
        SELECT 
            dt
            , pnl
            , trade_count
            , pnl / trade_count AS allocated_pnl-- Assumes equal allocation
        FROM 
        (
            SELECT 
              dt
            , SUM(pnl) AS pnl
            , SUM(trade_count) AS trade_count
            FROM
            (
                SELECT
                ' {periods}_day_regression_' || ticker as model_name
                , dt
                , ticker
                , CASE 
                    WHEN p_rank > .9
                        THEN  next_day_ret
                    WHEN p_rank < .1
                        THEN -next_day_ret
                    ELSE NULL
                    END AS pnl
                , CASE
                    WHEN p_rank > .9
                        THEN 1
                    WHEN p_rank < .1
                        THEN 1
                    ELSE NULL
                    END AS trade_count
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
                                (PARTITION BY ticker ORDER BY dt ROWS BETWEEN {periods} PRECEDING AND 1 PRECEDING)
                        ) a
                    ) b
                ) c
            ) d
            GROUP BY dt
            ORDER BY dt
        ) e
    ) f;
    """
    return sql.format(periods=periods)



day_250_df = pd.read_sql_query(query(250), con=conn)
plot_title = "XLF components total pnl / p_rank: .9 - .1/ Equal allocation / Simple strategie"
p = ggplot(aes(x='dt', y='cum_pnl'), data=day_250_df)
p + geom_line() + ggtitle(plot_title)
