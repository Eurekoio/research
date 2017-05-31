import psycopg2
import numpy
import matplotlib.pyplot as plt
import pandas as pd
from pandas.tools.plotting import bootstrap_plot
from pandas.tools.plotting import lag_plot
from pandas.tools.plotting import autocorrelation_plot
import math
from sqlalchemy import create_engine
from ggplot import *


conn = psycopg2.connect(dbname="ventris", host="192.168.1.119",
                        user="ventris_admin", password="X4NAdu")
engine = create_engine(
    'postgresql://ventris_admin:X4NAdu@192.168.1.119:5432/ventris')

# Need to create aggregate to multiply
# CREATE AGGREGATE mul(double precision) ( SFUNC = float8mul, STYPE=double
# precision )

SQL = """
SELECT
  dt
, ret
, ABS(ret) AS abs_ret
, SUM(ret) OVER (ORDER BY dt) AS cum_ret
, MUL(ret_factor) OVER (ORDER BY dt) AS compound_ret
, adj_close
FROM
(
    SELECT
      dt
    , (adj_close - LAG(adj_close, 1) OVER (ORDER BY dt) ) / LAG(adj_close, 1) OVER (ORDER BY dt) AS ret
    , 1 + ((adj_close - LAG(adj_close, 1) OVER (ORDER BY dt) ) / LAG(adj_close, 1) OVER (ORDER BY dt)) AS ret_factor
    , adj_close AS adj_close
    FROM xlf_etf_data
) a
ORDER BY dt
"""

df = pd.read_sql_query(SQL, con=conn)

# Describe data

df.describe()

"""
               ret      abs_ret      cum_ret  compound_ret    adj_close
count  4574.000000  4574.000000  4574.000000   4574.000000  4575.000000
mean      0.000337     0.012189     0.633268      1.191564    15.611933
std       0.020107     0.015994     0.374472      0.311003     4.074639
min      -0.173613     0.000000    -0.475585      0.329465     4.316813
25%      -0.007352     0.003328     0.365585      0.950934    12.464097
50%       0.000448     0.007735     0.594965      1.197367    15.687239
75%       0.008048     0.015309     0.865041      1.408304    18.452312
max       0.313869     0.313869     1.546321      1.903007    24.934158
"""

# Describe data with different subplots

df.describe(percentiles=[.05, .25, .75, .95])

"""
>>> df.describe(percentiles=[.05, .25, .75, .95])
               ret      abs_ret      cum_ret  compound_ret    adj_close
count  4574.000000  4574.000000  4574.000000   4574.000000  4575.000000
mean      0.000337     0.012189     0.633268      1.191564    15.611933
std       0.020107     0.015994     0.374472      0.311003     4.074639
min      -0.173613     0.000000    -0.475585      0.329465     4.316813
5%       -0.027079     0.000653     0.062069      0.704306     9.228332
25%      -0.007352     0.003328     0.365585      0.950934    12.464097
50%       0.000448     0.007735     0.594965      1.197367    15.687239
75%       0.008048     0.015309     0.865041      1.408304    18.452312
95%       0.026318     0.037491     1.276725      1.764994    23.125502
max       0.313869     0.313869     1.546321      1.903007    24.934158
"""


# Plot prices

p = ggplot(aes(x='dt', y='adj_close'), data=df)
p + geom_line() + ggtitle("XLF adj_close")


# Plot cumulative returns

p = ggplot(aes(x='dt', y='cum_ret'), data=df)
p + geom_line() + ggtitle("XLF cumulative return")


# Plot compounded returns

p = ggplot(aes(x='dt', y='compound_ret'), data=df)
p + geom_line() + ggtitle("XLF compound return")


# Plot returns

p = ggplot(aes(x='dt', y='ret'), data=df)
p + geom_line() + ggtitle("XLF returns")


# Plot absolute returns

p = ggplot(aes(x='dt', y='abs_ret'), data=df)
p + geom_line() + ggtitle("XLF absolute returns")


# Plot Density of returns

df.ret.plot.kde()
plt.show()


# Plot histogram of returns

df.ret.hist(color='k', alpha=0.5, bins=50)
plt.show()


# Lag plot returns

plt.figure()
lag_plot(df.ret)
plt.show()


plt.figure()
lag_plot(df.abs_ret)
plt.show()

# Plot autocorrelation of returns

plt.figure()
autocorrelation_plot(df.ret.dropna())
plt.show()


# Plot autocorrelation of absolute returns

plt.figure()
autocorrelation_plot(df.abs_ret.dropna())
plt.show()
