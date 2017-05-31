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
    , SUM(pnl) OVER (ORDER BY dt) AS cum_pnl
    FROM
    (
        SELECT
        ' {periods}_day_regression' as model_name
        , dt
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
    ) c;
    """
    return sql.format(periods=periods)


day_5_df = pd.read_sql_query(query(5), con=conn)
day_20_df = pd.read_sql_query(query(20), con=conn)
day_50_df = pd.read_sql_query(query(50), con=conn)
day_250_df = pd.read_sql_query(query(250), con=conn)

frames = [day_5_df, day_20_df, day_50_df, day_250_df]
big_frame = pd.concat(frames)


plot_title = "XLF Cum PNL"
p = ggplot(aes(x='dt', y='cum_pnl'), data=big_frame)
g = p + geom_line() + \
    facet_wrap("model_name") + \
    ggtitle(plot_title)

t = theme_gray()
t._rcParams['font.size'] = 10

g + t