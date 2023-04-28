
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
from streamlit_autorefresh import st_autorefresh
import matplotlib.pyplot as plt
import numpy as np
from snowflake.snowpark.functions import col, lit, sum as sum_, max as max_, month, year, concat, asc, to_date, floor,dayofyear, dayofweek,dateadd
import re
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.functions import udf
import datetime as dt

def is_outlier(col_name, lower_multiplier=0.3, upper_multiplier=0.3): 
    quartiles = f_aa_engagement_by_site.approxQuantile(col_name, [0.25, 0.75]) 
    Q1 = quartiles[0] 
    Q3 = quartiles[1] 
    IQR = Q3 - Q1 
    lower_bound = Q1 - (lower_multiplier * IQR) 
    upper_bound = Q3 + (upper_multiplier * IQR) 
    return (col(col_name) >= lower_bound) & (col(col_name) <= upper_bound) 


# Connect using the Snowflake Snowpark package
@st.cache_resource
def snowflake_sesion(connection_parameters) -> Session:
    return Session.builder.configs(connection_parameters).create()

connection_parameters = {
    "user" : "sophie.jones@contractor.itech.media",
    "account" : "gs46004.eu-west-1",
    "role" : "PRD_ANALYST",
    "warehouse" : "PRD_WH",
    "database" : "PRD_DWH",
    "authenticator" : "externalbrowser"
}

main_session = snowflake_sesion(connection_parameters)

with st.sidebar:
    option = st.selectbox(
    'How would you like to segment the data?',
    ('No Segment','Product', 'Device', 'Marketing channel'))
    st.write('You selected:', option)

st.title("Detailed analysis of the Time On Site variable")
st.write('Goal: determine if 30 seconds is the correct threshold to use for identifying engaged users')

if option == 'No Segment':

    #df = f_aa_engagement_by_site.select(['time_on_site_seconds','visit_date_time_utc']).to_pandas()
    f_aa_engagement_by_site = main_session.table('dimensional.f_aa_engagement_by_site')
    st.write("")
    st.write("")
    st.title('Cross Portfolio Analysis')
    st.write("")

    # Get basic descriptive statistics
    st.subheader('Descriptive Statistics')
    col1, col2, col3, col4= st.columns(4)
    avg = re.findall(r"(\d+(?:\.\d+)?)", str(f_aa_engagement_by_site.agg({"time_on_site_seconds": "avg"}).collect()))[0]
    col1.metric("Average", round(float(avg),1))
    std = re.findall(r"(\d+(?:\.\d+)?)", str(f_aa_engagement_by_site.agg({"time_on_site_seconds": "std"}).collect()))[0]
    col2.metric("Standard Deviation", round(float(std),1))
    min = re.findall(r"(\d+(?:\.\d+)?)", str(f_aa_engagement_by_site.agg({"time_on_site_seconds": "min"}).collect()))[0]
    col3.metric("Minumum", round(float(min),1))
    max = re.findall(r"(\d+(?:\.\d+)?)", str(f_aa_engagement_by_site.agg({"time_on_site_seconds": "max"}).collect()))[0]
    col4.metric("Maximum", round(float(max),1))

    quartiles = f_aa_engagement_by_site.approx_quantile("time_on_site_seconds", [0.25, 0.5, 0.75, 0.9])
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Median", round(float(quartiles[1]),1))
    col2.metric("25th percentile", round(float(quartiles[0]),1))
    col3.metric("75th percentile", round(float(quartiles[2]),1))
    col4.metric("90th percentile", round(float(quartiles[3]),1))
    st.write("")
    st.write("")

    st.subheader('Distrubtion')
    st.bar_chart(data=f_aa_engagement_by_site.group_by("time_on_site_seconds").agg((col("*"), "count")).collect(), x = "TIME_ON_SITE_SECONDS")
    st.write("")

    st.subheader('Distrubtion with outlier removal')
    df_filtered = f_aa_engagement_by_site.filter(is_outlier("TIME_ON_SITE_SECONDS"))
    st.bar_chart(data=df_filtered.group_by("time_on_site_seconds").agg((col("*"), "count")).collect(), x = "TIME_ON_SITE_SECONDS")
    st.write("")

    st.subheader('Total change over time - grouped monthly')
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("month", (month("VISIT_DATE_TIME_UTC")))
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("year", (year("VISIT_DATE_TIME_UTC")))
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("month_year", to_date(concat("month", lit("/01/"), "year")))
    st.line_chart(data = f_aa_engagement_by_site.group_by('MONTH_YEAR').agg(col('TIME_ON_SITE_SECONDS'),'sum').collect(), x='MONTH_YEAR')
    st.write("")

    st.subheader('Average change over time - grouped monthly')
    print(f_aa_engagement_by_site.group_by('MONTH_YEAR').agg({"TIME_ON_SITE_SECONDS": "avg"}).show(5))
    st.line_chart(data = f_aa_engagement_by_site.group_by('MONTH_YEAR').agg({"TIME_ON_SITE_SECONDS": "avg"}).collect(), x = 'MONTH_YEAR', y = "AVG(TIME_ON_SITE_SECONDS)")
    st.write("")

    st.subheader('Total change over time - grouped weekly')
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("Monday_date", to_date(dateadd('day', ((dayofweek("VISIT_DATE_TIME_UTC")-1)*-1), col("VISIT_DATE_TIME_UTC"))))
    st.line_chart(data = f_aa_engagement_by_site.group_by('Monday_date').agg(col('TIME_ON_SITE_SECONDS'),'sum').collect(), x = 'MONDAY_DATE', y = "SUM(TIME_ON_SITE_SECONDS)" )
    st.write("")

    st.subheader('Average change over time - grouped weekly')
    #f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("Monday_date", to_date(dateadd('day', ((dayofweek("VISIT_DATE_TIME_UTC")-1)*-1), col("VISIT_DATE_TIME_UTC"))))
    st.line_chart(data = f_aa_engagement_by_site.group_by('Monday_date').agg(col('TIME_ON_SITE_SECONDS'),'avg').collect(), x = 'MONDAY_DATE', y = "AVG(TIME_ON_SITE_SECONDS)" )
    st.write("")


    st.subheader('Correlations with other key metrics')
    col1, col2 = st.columns(2)

    f_aa_engagement_by_site = f_aa_engagement_by_site.withColumn("visit_engagement_encoded",fn.when(f_aa_engagement_by_site.SITE_VISIT_ENGAGEMENT == True, 1).otherwise(0))
    corr = f_aa_engagement_by_site.select(fn.corr(f_aa_engagement_by_site.TIME_ON_SITE_SECONDS, f_aa_engagement_by_site.VISIT_ENGAGEMENT_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on site and visit engagement is {corr:.3f}")
    col1.metric("Correlation between tos and visit enagagment", round(corr,3))

    f_aa_engagement_by_site = f_aa_engagement_by_site.withColumn("visit_conversion_encoded",fn.when(f_aa_engagement_by_site.SITE_VISIT_CONVERSION == True, 1).otherwise(0))
    corr = f_aa_engagement_by_site.select(fn.corr(f_aa_engagement_by_site.TIME_ON_SITE_SECONDS, f_aa_engagement_by_site.VISIT_CONVERSION_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on site and visit conversion is {corr:.3f}")
    col2.metric("Correlation between tos and visit conversion", round(corr,3))

    st.write('Would these improve with outlier removal?')
    col1, col2 = st.columns(2)
    df_filtered = df_filtered.withColumn("visit_engagement_encoded",fn.when(df_filtered.SITE_VISIT_ENGAGEMENT == True, 1).otherwise(0))
    corr = df_filtered.select(fn.corr(df_filtered.TIME_ON_SITE_SECONDS, df_filtered.VISIT_ENGAGEMENT_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on site and visit engagement, with time on site outliers removed, is {corr:.3f}")
    col1.metric("Correlation between tos and visit enagagment", round(corr,3))

    df_filtered = df_filtered.withColumn("visit_conversion_encoded",fn.when(df_filtered.SITE_VISIT_CONVERSION == True, 1).otherwise(0))
    corr = df_filtered.select(fn.corr(df_filtered.TIME_ON_SITE_SECONDS, df_filtered.VISIT_CONVERSION_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on sie and visit conversion, with time on site outliers removed, is {corr:.3f}")
    col2.metric("Correlation between tos and visit conversion", round(corr,3))

elif option == 'Device':
    st.write("")
    st.write("")
    st.title('Device Segmented Analysis')
    st.write("")

    f_aa_engagement_by_site = main_session.table('dimensional.f_aa_engagement_by_site')
    device_group = f_aa_engagement_by_site.groupBy('device_type')
    avg = device_group.agg({"time_on_site_seconds": "avg"})
    distinct_ids = [x.DEVICE_TYPE for x in avg.select('DEVICE_TYPE').distinct().collect()]
    option = st.selectbox(
    'Which Device type would you like to view?',
    distinct_ids)
    st.write('You selected:', option)

    st.subheader('Descriptive Statistics')
    devicedf= f_aa_engagement_by_site.filter(col('device_type') == option)
    col1, col2, col3, col4= st.columns(4)
    avg = re.findall(r"(\d+(?:\.\d+)?)", str(devicedf.agg({"time_on_site_seconds": "avg"}).collect()))[0]
    col1.metric("Average", round(float(avg),1))
    std = re.findall(r"(\d+(?:\.\d+)?)", str(devicedf.agg({"time_on_site_seconds": "std"}).collect()))[0]
    col2.metric("Standard Deviation", round(float(std),1))
    min = re.findall(r"(\d+(?:\.\d+)?)", str(devicedf.agg({"time_on_site_seconds": "min"}).collect()))[0]
    col3.metric("Minumum", round(float(min),1))
    max = re.findall(r"(\d+(?:\.\d+)?)", str(devicedf.agg({"time_on_site_seconds": "max"}).collect()))[0]
    col4.metric("Maximum", round(float(max),1))

    quartiles = devicedf.approx_quantile("time_on_site_seconds", [0.25, 0.5, 0.75, 0.9])
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Median", round(float(quartiles[1]),1))
    col2.metric("25th percentile", round(float(quartiles[0]),1))
    col3.metric("75th percentile", round(float(quartiles[2]),1))
    col4.metric("90th percentile", round(float(quartiles[3]),1))
    st.write("")
    st.write("")


    st.subheader('Distrubtion')
    for i in distinct_ids:
        f_aa_engagement_by_site = f_aa_engagement_by_site.withColumn(i,fn.when(f_aa_engagement_by_site.device_type == i, 1).otherwise(0))
    

    #.agg({"time_on_site_seconds": "max"})
    #dict = {col("*"): "count", device1: }
    dict = {}
    keys = range(len(distinct_ids))
    for i in distinct_ids:
            dict[i] = "sum"
    print(dict)

    st.bar_chart(data = f_aa_engagement_by_site.agg(dict).collect())

    '''
    st.bar_chart(data=f_aa_engagement_by_site.group_by(["time_on_site_seconds"]).agg(dict).collect(), x = "TIME_ON_SITE_SECONDS")
    st.write("")
    '''

    st.subheader('Device Type Distrubtion')


    st.subheader('Distrubtion with outlier removal')
    df_filtered = f_aa_engagement_by_site.filter(is_outlier("TIME_ON_SITE_SECONDS"))
    st.bar_chart(data=df_filtered.group_by("time_on_site_seconds").agg(dict).collect(), x = "TIME_ON_SITE_SECONDS")
    st.write("")

    st.subheader('Total change over time - grouped monthly')
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("month", (month("VISIT_DATE_TIME_UTC")))
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("year", (year("VISIT_DATE_TIME_UTC")))
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("month_year", to_date(concat("month", lit("/01/"), "year")))
    st.line_chart(data = f_aa_engagement_by_site.group_by('MONTH_YEAR').agg(dict).collect(), x='MONTH_YEAR')
    st.write("")

    st.subheader('Average change over time - grouped monthly')
    print(f_aa_engagement_by_site.group_by('MONTH_YEAR').agg({"TIME_ON_SITE_SECONDS": "avg"}).show(5))
    st.line_chart(data = f_aa_engagement_by_site.group_by('MONTH_YEAR').agg({"TIME_ON_SITE_SECONDS": "avg"}).collect(), x = 'MONTH_YEAR', y = "AVG(TIME_ON_SITE_SECONDS)")
    st.write("")

    st.subheader('Total change over time - grouped weekly')
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("Monday_date", to_date(dateadd('day', ((dayofweek("VISIT_DATE_TIME_UTC")-1)*-1), col("VISIT_DATE_TIME_UTC"))))
    st.line_chart(data = f_aa_engagement_by_site.group_by('Monday_date').agg(dict).collect(), x = 'MONDAY_DATE', y = "SUM(TIME_ON_SITE_SECONDS)" )
    st.write("")

    st.subheader('Average change over time - grouped weekly')
    #f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("Monday_date", to_date(dateadd('day', ((dayofweek("VISIT_DATE_TIME_UTC")-1)*-1), col("VISIT_DATE_TIME_UTC"))))
    st.line_chart(data = f_aa_engagement_by_site.group_by('Monday_date').agg(col('TIME_ON_SITE_SECONDS'),'avg').collect(), x = 'MONDAY_DATE', y = "AVG(TIME_ON_SITE_SECONDS)" )
    st.write("")



#main_session.close()