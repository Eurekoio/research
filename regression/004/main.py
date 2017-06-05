import psycopg2
import numpy
import matplotlib.pyplot as plt
import pandas as pd
import math
from sqlalchemy import create_engine
from ggplot import *


conn = psycopg2.connect(dbname="ventris", host="192.168.1.119", user="ventris_admin", password="X4NAdu")
engine = create_engine('postgresql://ventris_admin:X4NAdu@192.168.1.119:5432/ventris')

# Allocated PNL based on p_rank of p_value

def query(paramObj):
    """ 
    paramsObj: {'periods': int, 'top_percentile': float, 'bottom_percentile': float}
    """
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
                    WHEN p_rank > {top_percentile}
                        THEN  next_day_ret
                    WHEN p_rank < {bottom_percentile}
                        THEN -next_day_ret
                    ELSE NULL
                    END AS pnl
                , CASE
                    WHEN p_rank > {top_percentile}
                        THEN 1
                    WHEN p_rank < {bottom_percentile}
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
    return sql.format(periods=paramObj['periods'],
                      top_percentile=paramObj['top_percentile'], 
                      bottom_percentile=paramObj['bottom_percentile'])


def plot_title(paramObj):
    title = "XLF components pnl / prank: {top_percentile} - {bottom_percentile}/ Equal allocation / {periods} day/ Simple strategy"
    return title.format(
        periods=paramObj['periods'],
        top_percentile=paramObj['top_percentile'], 
        bottom_percentile=paramObj['bottom_percentile'])

# 5 Day 80 20
params = dict(periods=5, top_percentile=.9, bottom_percentile=.1)

df = pd.read_sql_query(query(params), con=conn)
p = ggplot(aes(x='dt', y='cum_pnl'), data=df)
p + geom_line() + ggtitle(plot_title(params))


# 5 Day 90 10

params = dict(periods=5, top_percentile=.8, bottom_percentile=.2)

df = pd.read_sql_query(query(params), con=conn)
p = ggplot(aes(x='dt', y='cum_pnl'), data=df)
p + geom_line() + ggtitle(plot_title(params))

# 5 day 50 50
params = dict(periods=5, top_percentile=.5, bottom_percentile=.5)

df = pd.read_sql_query(query(params), con=conn)
p = ggplot(aes(x='dt', y='cum_pnl'), data=df)
p + geom_line() + ggtitle(plot_title(params))


# 250 day 50 50
params = dict(periods=250, top_percentile=.5, bottom_percentile=.5)

df = pd.read_sql_query(query(params), con=conn)
p = ggplot(aes(x='dt', y='cum_pnl'), data=df)
p + geom_line() + ggtitle(plot_title(params))


# 250 day 80 20
params = dict(periods=250, top_percentile=.8, bottom_percentile=.2)

df = pd.read_sql_query(query(params), con=conn)
p = ggplot(aes(x='dt', y='cum_pnl'), data=df)
p + geom_line() + ggtitle(plot_title(params))