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
    ' {periods}_day_regression' as model_name
    , dt
    , (x * slope) + intercept AS pvalue
    , next_day_ret
    FROM
    (
        SELECT * 
        , regr_slope(next_day_ret, x) OVER deriv_wdw AS slope
        , regr_intercept(next_day_ret, x) OVER deriv_wdw AS intercept
        FROM
        (
            SELECT 
            feature_0.dt
            , feature_0.ticker
            , feature_0.ret AS x
            , dep.ret AS next_day_ret
            FROM xlf_returns feature_0, xlf_next_day_returns dep
            WHERE feature_0.dt = dep.dt
        ) AS a
        WINDOW deriv_wdw AS 
            (PARTITION BY ticker ORDER BY dt ROWS BETWEEN {periods} PRECEDING AND 1 PRECEDING)
    ) b;
    """
    return sql.format(periods=periods) # Don't do this in production



day_5_df = pd.read_sql_query(query(5), con=conn)
day_20_df = pd.read_sql_query(query(20), con=conn)
day_50_df = pd.read_sql_query(query(50), con=conn)
day_250_df = pd.read_sql_query(query(250), con=conn)

# Merge frames

frames = [day_5_df, day_20_df, day_50_df, day_250_df]
big_frame = pd.concat(frames)

plot_title = "XLF pvalue to next day returns"
p = ggplot(aes(x='next_day_ret', y='pvalue'), data=big_frame)
p + geom_point() + \
    facet_wrap("model_name") +\
    ggtitle(plot_title)


# Filtered

def query(periods):
    sql = """
    SELECT * 
    FROM
    (
        SELECT
        ' {periods}_day_regression' as model_name
        , dt
        , (x * slope) + intercept AS pvalue
        , next_day_ret
        FROM
        (
            SELECT * 
            , regr_slope(next_day_ret, x) OVER deriv_wdw AS slope
            , regr_intercept(next_day_ret, x) OVER deriv_wdw AS intercept
            FROM
            (
                SELECT 
                feature_0.dt
                , feature_0.ticker
                , feature_0.ret AS x
                , dep.ret AS next_day_ret
                FROM xlf_returns feature_0, xlf_next_day_returns dep
                WHERE feature_0.dt = dep.dt
            ) AS a
            WINDOW deriv_wdw AS 
                (PARTITION BY ticker ORDER BY dt ROWS BETWEEN {periods} PRECEDING AND 1 PRECEDING)
        ) b
    ) c
    WHERE abs(pvalue) < 0.2
        ;
    """
    return sql.format(periods=periods) # Don't do this in production



day_5_df = pd.read_sql_query(query(5), con=conn)
day_20_df = pd.read_sql_query(query(20), con=conn)
day_50_df = pd.read_sql_query(query(50), con=conn)
day_250_df = pd.read_sql_query(query(250), con=conn)

# Merge frames

frames = [day_5_df, day_20_df, day_50_df, day_250_df]
big_frame = pd.concat(frames)
plot_title = "XLF pvalue to next day returns filtered"
p = ggplot(aes(x='next_day_ret', y='pvalue'), data=big_frame)

p + geom_point() + \
    facet_wrap("model_name") +\
    ggtitle(plot_title)