#connecting and reading to and from snowflake
import boto3
import snowflake.connector
from pathlib import Path
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer
from snowflake.connector import DictCursor
import pandas as pd
from snowflake.snowpark import functions as fn
from snowflake.snowpark import types as type
from snowflake.snowpark import DataFrame as df
import snowflake.connector
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session
#from streamlit_autorefresh import st_autorefresh
import matplotlib.pyplot as plt
import numpy as np
from snowflake.snowpark.functions import col, count, lit, sum as sum_, max as max_, month, year, concat, asc, to_date, floor,dayofyear, dayofweek,dateadd,avg
import re
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.functions import udf
import datetime as dt
import scipy.stats as stats
from scipy.stats import chi2_contingency
from statsmodels.stats.contingency_tables import mcnemar

# to run : python -m streamlit run EDA.py

def is_outlier(col_name, lower_multiplier=0.3, upper_multiplier=0.3): 
    quartiles = engagement_test.approxQuantile(col_name, [0.25, 0.75]) 
    Q1 = quartiles[0] 
    Q3 = quartiles[1] 
    IQR = Q3 - Q1 
    lower_bound = Q1 - (lower_multiplier * IQR) 
    upper_bound = Q3 + (upper_multiplier * IQR) 
    return (col(col_name) >= lower_bound) & (col(col_name) <= upper_bound) 


# Connect using the Snowflake Snowpark package
@st.cache_resource
def snowflake_session(connection_parameters) -> Session:
    return Session.builder.configs(connection_parameters).create()

connection_parameters = {
    "user" : "sophie.jones@contractor.itech.media",
    "account" : "gs46004.eu-west-1",
    "role" : "PRD_ANALYST",
    "warehouse" : "PRD_WH",
    "database" : "PRD_DWH",
    "authenticator" : "externalbrowser"
}

main_session = snowflake_session(connection_parameters)
engagement_test = main_session.table('sandbox.sj_engagement_test')

st.title('Engagement Rate Recreation Statistical Testing')
st.write("")
st.write('Data is for casino.org for dates 15-05-2023 - 17-05-2023')
#streamlit options for which pings to compare?
#sample method or all?
#plot graphs of past engagement rate trends
engagement_test = engagement_test.withColumnRenamed(col('has_engagement_test_5'),'5_engage')
engagement_test = engagement_test.withColumnRenamed(col('has_engagement_test_10'),'10_engage')
engagement_test = engagement_test.withColumnRenamed(col('has_engagement_test_15'),'15_engage')
engagement_test = engagement_test.withColumnRenamed(col('has_engagement_test_20'),'20_engage')
engagement_test = engagement_test.withColumnRenamed(col('has_engagement_test_25'),'25_engage')
engagement_test = engagement_test.withColumnRenamed(col('has_engagement_test_30'),'30_engage')
engagement_test = engagement_test.withColumnRenamed(col('has_engagement_test_35'),'35_engage')
engagement_test = engagement_test.withColumnRenamed(col('has_engagement_test_40'),'40_engage')
engagement_test = engagement_test.withColumnRenamed(col('has_engagement_test_45'),'45_engage')
engagement_test = engagement_test.withColumnRenamed(col('has_engagement_test_50'),'50_engage')
engagement_test = engagement_test.withColumnRenamed(col('has_engagement_test_55'),'55_engage')
engagement_test = engagement_test.withColumnRenamed(col('has_engagement_test_60'),'60_engage')
engagement_test = engagement_test.withColumnRenamed(col('has_engagement_test_90'),'90_engage')
st.line_chart(engagement_test.group_by('visit_date_utc').agg({'5_engage':'sum','10_engage':'sum','15_engage':'sum','20_engage':'sum','25_engage':'sum','30_engage':'sum','35_engage':'sum','40_engage':'sum','45_engage':'sum','50_engage':'sum','55_engage':'sum','60_engage':'sum','90_engage':'sum',}), x = 'VISIT_DATE_UTC')


st.divider()
option1 = st.selectbox(
'Which is the first Time on site ping do you wish to compare?',
(5,10,15,20,25,30,35,40,45,50,55,60,90), index=5)
st.write('You selected:', option1)
option2 = st.selectbox(
'Which is the second Time on site ping do you wish to compare?',
(5,10,15,20,25,30,35,40,45,50,55,60,90), index=8)
st.write('You selected:', option2)
st.divider()
st.write("")
datalist = engagement_test.select(count(col("*")).alias("count_all"),sum_(col(f"{option1}_engage")).alias(f"sum_{option1}"),((sum_(col(f"{option1}_engage"))/count(col("*")))*100).alias(f"Engage_rate_{option1}"),sum_(col(f"{option2}_engage")).alias(f"sum_{option2}"), ((sum_(col(f"{option2}_engage"))/count(col("*")))*100).alias(f"Engage_rate_{option2}")).collect()
st.write(datalist)
st.write('')
data = [[datalist[0][1], (datalist[0][0]-datalist[0][1])],[datalist[0][3], (datalist[0][0]-datalist[0][3])]]

#fishers exact
oddsratio, pvalue = stats.fisher_exact(data) 
alpha = 0.05 
st.subheader('Fisher Exact Test')
st.write('fishers exact p value is ',pvalue)
if pvalue <= alpha:
    st.write('Dependent (reject H0)')
else:
    st.write('Independent (H0 holds true)')
st.write('')

#chisquared
stat, p, dof, expected = chi2_contingency(data)
alpha = 0.05
st.subheader('ChiSquared Test')
st.write("chisquared p value is ",p)
if p <= alpha:
    st.write('Dependent (reject H0)')
else:
    st.write('Independent (H0 holds true)')
st.write('')

#mcnemar
result = mcnemar(data, exact=True)
st.subheader('McNemar Test')
st.write('mcnemar p-value is ', result.pvalue)
alpha = 0.05
if result.pvalue > alpha:
    st.write('Same proportions of errors (fail to reject H0)')
else:
    st.write('Different proportions of errors (reject H0)')
st.write('')

