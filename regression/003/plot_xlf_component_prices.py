import psycopg2
import numpy
import matplotlib.pyplot as plt
import pandas as pd
import math
from sqlalchemy import create_engine
from ggplot import *


conn = psycopg2.connect(dbname="ventris", host="192.168.1.119", user="ventris_admin", password="X4NAdu")
engine = create_engine('postgresql://ventris_admin:X4NAdu@192.168.1.119:5432/ventris')


# Single component

sql = """
    SELECT 
    dt
    , ticker
    , ret AS x
    , SUM(ret) OVER (PARTITION BY ticker ORDER BY dt) AS cum_pnl
    FROM xlf_components_returns 
    WHERE dt > '1999-01-01' AND ticker = 'WFC'
"""

df = pd.read_sql_query(sql, con=conn)


plot_title = "WFC CUM PNL"
p = ggplot(aes(x='dt', y='cum_pnl'), data=df)
p + geom_line() + \
    ggtitle(plot_title)


# All components

sql = """
    SELECT 
      dt
    , ticker
    , ret AS x
    , SUM(ret) OVER (PARTITION BY ticker ORDER BY dt) AS cum_pnl
    FROM xlf_components_returns
    WHERE dt > '1999-01-01'
    AND ticker NOT IN ('AIG', 'C', 'ETFC')
"""

df = pd.read_sql_query(sql, con=conn)


plot_title = "XLF Components CUM PNL excl. (AIG, C, ETFC)"
p = ggplot(aes(x='dt', y='cum_pnl'), data=df)
g = p + geom_line() + \
    facet_wrap("ticker") + \
    ggtitle(plot_title)

t = theme_gray()
t._rcParams['font.size'] = 12

g + t