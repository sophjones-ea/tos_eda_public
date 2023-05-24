
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
from snowflake.snowpark.functions import col, lit, sum as sum_, max as max_, month, year, concat, asc, to_date, floor,dayofyear, dayofweek,dateadd,avg
import re
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.functions import udf
import datetime as dt

# to run : python -m streamlit run EDA.py

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

st.markdown("<h1 style='text-align: center; color: white;'>Detailed analysis of the Time On Site variable</h1>", unsafe_allow_html=True)
st.markdown("<h5 style='text-align: center; color: white;'>Goal: determine if 30 seconds is the correct threshold to use for identifying engaged users</h5>", unsafe_allow_html=True)
st.divider()

option = st.selectbox(
'How would you like to segment the data?',
('No Segment','Vertical', 'Device', 'Marketing channel', 'Page Category'))
st.write('You selected:', option)
st.divider()


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
    average = re.findall(r"(\d+(?:\.\d+)?)", str(f_aa_engagement_by_site.agg({"time_on_site_seconds": "avg"}).collect()))[0]
    col1.metric("Average", round(float(average),1))
    #std_test = re.findall(r"(\d+(?:\.\d+)?)", str(f_aa_engagement_by_site.agg({"time_on_site_seconds": "std"}).collect()))
    #st.write(std_test)
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
    #print(f_aa_engagement_by_site.group_by("time_on_site_seconds").agg((col("*"), "count")).show(5))
    st.bar_chart(data=f_aa_engagement_by_site.group_by("time_on_site_seconds").agg((col("*"), "count")).collect(), x = "TIME_ON_SITE_SECONDS")
    st.write("")

    st.subheader('Distrubtion with outlier removal')
    df_filtered = f_aa_engagement_by_site.filter(is_outlier("TIME_ON_SITE_SECONDS"))
    st.bar_chart(data=df_filtered.group_by("time_on_site_seconds").agg((col("*"), "count")).collect(), x = "TIME_ON_SITE_SECONDS")
    st.write("")

    #how to get total number of visits

    st.subheader('Total change over time - grouped monthly')
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("month", (month("VISIT_DATE_TIME_UTC")))
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("year", (year("VISIT_DATE_TIME_UTC")))
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("month_year", to_date(concat("month", lit("/01/"), "year")))
    print(f_aa_engagement_by_site.group_by('MONTH_YEAR').agg({'TIME_ON_SITE_SECONDS':'sum'}).show(5))
    today = dt.datetime.today()
    datem = dt.datetime(today.year, today.month, 1)
    st.line_chart(data = f_aa_engagement_by_site.filter(f_aa_engagement_by_site["month_year"] != datem).group_by('MONTH_YEAR').agg({'TIME_ON_SITE_SECONDS':'sum'}).collect(), x='MONTH_YEAR')
    st.write("")

    #st.subheader('Average change over time - grouped monthly')
    #st.line_chart(data = f_aa_engagement_by_site.filter(f_aa_engagement_by_site["month_year"] != datem).group_by('MONTH_YEAR').agg({'TIME_ON_SITE_SECONDS':'avg'}).collect(), x='MONTH_YEAR')
    #st.write("")
    
    st.subheader('Total change over time - grouped weekly')
    today = dt.datetime.today()
    last_monday = today - dt.timedelta(days=today.weekday())
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("Monday_date", to_date(dateadd('day', ((dayofweek("VISIT_DATE_TIME_UTC")-1)*-1), col("VISIT_DATE_TIME_UTC"))))
    st.line_chart(data = f_aa_engagement_by_site.filter(f_aa_engagement_by_site["Monday_date"] != last_monday.date()).group_by('Monday_date').agg(col('TIME_ON_SITE_SECONDS'),'sum').collect(), x = 'MONDAY_DATE', y = "SUM(TIME_ON_SITE_SECONDS)" )
    st.write("")

    #st.subheader('Average change over time - grouped weekly')
    #f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("Monday_date", to_date(dateadd('day', ((dayofweek("VISIT_DATE_TIME_UTC")-1)*-1), col("VISIT_DATE_TIME_UTC"))))
    #st.line_chart(data = f_aa_engagement_by_site.group_by('Monday_date').agg(col('TIME_ON_SITE_SECONDS'),'avg').collect(), x = 'MONDAY_DATE', y = "AVG(TIME_ON_SITE_SECONDS)" )
    #st.write("")

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
    f_aa_engagement_by_site = main_session.table('dimensional.f_aa_engagement_by_site')
    device_group = f_aa_engagement_by_site.groupBy('device_type')
    average = device_group.agg({"time_on_site_seconds": "avg"})
    distinct_ids = [x.DEVICE_TYPE for x in average.select('DEVICE_TYPE').distinct().collect()]
    
    st.write("")
    st.write("")
    st.header('Distrubtion')
    for i in distinct_ids:
        f_aa_engagement_by_site = f_aa_engagement_by_site.withColumn(i,fn.when(f_aa_engagement_by_site.device_type == i, 1).otherwise(0))
    

    dict = {}
    keys = range(len(distinct_ids))
    for i in distinct_ids:
            dict[i] = "sum"
    print(dict)

    st.subheader('Device Type Distrubtion')
    st.bar_chart(data = f_aa_engagement_by_site.group_by('device_type').agg((col("*"), "count")).collect(), height=500, x = 'DEVICE_TYPE')
    st.write("")
    st.write("")
    st.title('Device Segmented Analysis')
    st.write("")

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

    st.subheader('Correlations with other key metrics')
    col1, col2 = st.columns(2)

    devicedf = devicedf.withColumn("visit_engagement_encoded",fn.when(devicedf.SITE_VISIT_ENGAGEMENT == True, 1).otherwise(0))
    corr = devicedf.select(fn.corr(devicedf.TIME_ON_SITE_SECONDS, devicedf.VISIT_ENGAGEMENT_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on site and visit engagement is {corr:.3f}")
    col1.metric("Correlation between tos and visit enagagment", round(corr,3))

    devicedf = devicedf.withColumn("visit_conversion_encoded",fn.when(devicedf.SITE_VISIT_CONVERSION == True, 1).otherwise(0))
    corr = devicedf.select(fn.corr(devicedf.TIME_ON_SITE_SECONDS, devicedf.VISIT_CONVERSION_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on site and visit conversion is {corr:.3f}")
    col2.metric("Correlation between tos and visit conversion", round(corr,3))

    st.write('Would these improve with outlier removal?')
    col1, col2 = st.columns(2)
    devicedf_filtered = devicedf.filter(is_outlier("TIME_ON_SITE_SECONDS"))
    devicedf_filtered = devicedf_filtered.withColumn("visit_engagement_encoded",fn.when(devicedf_filtered.SITE_VISIT_ENGAGEMENT == True, 1).otherwise(0))
    corr = devicedf_filtered.select(fn.corr(devicedf_filtered.TIME_ON_SITE_SECONDS, devicedf_filtered.VISIT_ENGAGEMENT_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on site and visit engagement, with time on site outliers removed, is {corr:.3f}")
    col1.metric("Correlation between tos and visit enagagment", round(corr,3))

    devicedf_filtered = devicedf_filtered.withColumn("visit_conversion_encoded",fn.when(devicedf_filtered.SITE_VISIT_CONVERSION == True, 1).otherwise(0))
    corr = devicedf_filtered.select(fn.corr(devicedf_filtered.TIME_ON_SITE_SECONDS, devicedf_filtered.VISIT_CONVERSION_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on sie and visit conversion, with time on site outliers removed, is {corr:.3f}")
    col2.metric("Correlation between tos and visit conversion", round(corr,3))

    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.title('Change Over Time')

    st.subheader('Total change over time - grouped monthly')
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("month", (month("VISIT_DATE_TIME_UTC")))
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("year", (year("VISIT_DATE_TIME_UTC")))
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("month_year", to_date(concat("month", lit("/01/"), "year")))
    today = dt.datetime.today()
    datem = dt.datetime(today.year, today.month, 1)
    st.line_chart(data = f_aa_engagement_by_site.filter(f_aa_engagement_by_site["month_year"] != datem).group_by('MONTH_YEAR').agg(dict).collect(), x='MONTH_YEAR')
    st.write("")

    #st.subheader('Average change over time - grouped monthly')
    #print(f_aa_engagement_by_site.group_by('MONTH_YEAR').agg({"TIME_ON_SITE_SECONDS": "avg"}).show(5))
    #st.line_chart(data = f_aa_engagement_by_site.group_by('MONTH_YEAR').agg({"TIME_ON_SITE_SECONDS": "avg"}).collect(), x = 'MONTH_YEAR', y = "AVG(TIME_ON_SITE_SECONDS)")
    #st.write("")

    st.subheader('Total change over time - grouped weekly')
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("Monday_date", to_date(dateadd('day', ((dayofweek("VISIT_DATE_TIME_UTC")-1)*-1), col("VISIT_DATE_TIME_UTC"))))
    today = dt.datetime.today()
    last_monday = today - dt.timedelta(days=today.weekday())
    st.line_chart(data = f_aa_engagement_by_site.filter(f_aa_engagement_by_site["Monday_date"] != last_monday.date()).group_by('Monday_date').agg(dict).collect(), x = 'MONDAY_DATE')
    st.write("")

    #st.subheader('Average change over time - grouped weekly')
    ##f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("Monday_date", to_date(dateadd('day', ((dayofweek("VISIT_DATE_TIME_UTC")-1)*-1), col("VISIT_DATE_TIME_UTC"))))
    #st.line_chart(data = f_aa_engagement_by_site.group_by('Monday_date').agg(col('TIME_ON_SITE_SECONDS'),'avg').collect(), x = 'MONDAY_DATE', y = "AVG(TIME_ON_SITE_SECONDS)" )
    #st.write("")

elif option == 'Vertical':
    f_aa_engagement_by_site = main_session.table('dimensional.f_aa_engagement_by_site')

    f_aa_engagement_by_site = f_aa_engagement_by_site.withColumn("HAS_PAGE_VIEW_POKER_ENCODED", fn.when(f_aa_engagement_by_site["HAS_PAGE_VIEW_POKER"], 1).otherwise(0))
    f_aa_engagement_by_site = f_aa_engagement_by_site.withColumn("HAS_PAGE_VIEW_CASINO_ENCODED", fn.when(f_aa_engagement_by_site["HAS_PAGE_VIEW_CASINO"], 1).otherwise(0))
    f_aa_engagement_by_site = f_aa_engagement_by_site.withColumn("HAS_PAGE_VIEW_SPORTS_ENCODED", fn.when(f_aa_engagement_by_site["HAS_PAGE_VIEW_SPORTS"], 1).otherwise(0))

    f_aa_engagement_by_site = f_aa_engagement_by_site.withColumn(
        "Product_Count", 
        (f_aa_engagement_by_site.HAS_PAGE_VIEW_POKER_ENCODED  + f_aa_engagement_by_site.HAS_PAGE_VIEW_CASINO_ENCODED + f_aa_engagement_by_site.HAS_PAGE_VIEW_SPORTS_ENCODED))

    f_aa_engagement_by_site = f_aa_engagement_by_site.withColumn(
        "Product_Named",
        fn.when(f_aa_engagement_by_site.PRODUCT_COUNT == 2, "Multi-Product")
        .when(f_aa_engagement_by_site.PRODUCT_COUNT == 3, "Multi-Product")
        .when(f_aa_engagement_by_site.HAS_PAGE_VIEW_POKER == True, 'Poker')
        .when(f_aa_engagement_by_site.HAS_PAGE_VIEW_CASINO == True, 'Casino')
        .when(f_aa_engagement_by_site.HAS_PAGE_VIEW_SPORTS == True, 'Sports')
        .otherwise('None'))


    product_group = f_aa_engagement_by_site.groupBy('Product_Named')
    avg = product_group.agg({"time_on_site_seconds": "avg"})
    distinct_ids = [x.PRODUCT_NAMED for x in avg.select('Product_Named').distinct().collect()]
    
    st.write("")
    st.write("")
    st.header('Distrubtion')
    for i in distinct_ids:
        f_aa_engagement_by_site = f_aa_engagement_by_site.withColumn(i,fn.when(f_aa_engagement_by_site.Product_Named == i, 1).otherwise(0))
    
    dict = {}
    keys = range(len(distinct_ids))
    for i in distinct_ids:
            dict[i] = "sum"
    print(dict)

    st.subheader('Product Type Distrubtion')
    st.bar_chart(data = f_aa_engagement_by_site.group_by('PRODUCT_NAMED').agg((col("*"), "count")).collect(), height=500, x = 'PRODUCT_NAMED')
    st.write("")
    st.write("")
    st.title('Product Segmented Analysis')
    st.write("")

    option = st.selectbox(
    'Which Product type would you like to view?',
    distinct_ids)
    st.write('You selected:', option)

    st.subheader('Descriptive Statistics')
    productdf= f_aa_engagement_by_site.filter(col('Product_Named') == option)
    col1, col2, col3, col4= st.columns(4)
    avg = re.findall(r"(\d+(?:\.\d+)?)", str(productdf.agg({"time_on_site_seconds": "avg"}).collect()))[0]
    col1.metric("Average", round(float(avg),1))
    std = re.findall(r"(\d+(?:\.\d+)?)", str(productdf.agg({"time_on_site_seconds": "std"}).collect()))[0]
    col2.metric("Standard Deviation", round(float(std),1))
    min = re.findall(r"(\d+(?:\.\d+)?)", str(productdf.agg({"time_on_site_seconds": "min"}).collect()))[0]
    col3.metric("Minumum", round(float(min),1))
    max = re.findall(r"(\d+(?:\.\d+)?)", str(productdf.agg({"time_on_site_seconds": "max"}).collect()))[0]
    col4.metric("Maximum", round(float(max),1))

    quartiles = productdf.approx_quantile("time_on_site_seconds", [0.25, 0.5, 0.75, 0.9])
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Median", round(float(quartiles[1]),1))
    col2.metric("25th percentile", round(float(quartiles[0]),1))
    col3.metric("75th percentile", round(float(quartiles[2]),1))
    col4.metric("90th percentile", round(float(quartiles[3]),1))
    st.write("")
    st.write("")

    st.subheader('Correlations with other key metrics')
    col1, col2 = st.columns(2)

    productdf = productdf.withColumn("visit_engagement_encoded",fn.when(productdf.SITE_VISIT_ENGAGEMENT == True, 1).otherwise(0))
    corr = productdf.select(fn.corr(productdf.TIME_ON_SITE_SECONDS, productdf.VISIT_ENGAGEMENT_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on site and visit engagement is {corr:.3f}")
    col1.metric("Correlation between tos and visit enagagment", round(corr,3))

    productdf = productdf.withColumn("visit_conversion_encoded",fn.when(productdf.SITE_VISIT_CONVERSION == True, 1).otherwise(0))
    corr = productdf.select(fn.corr(productdf.TIME_ON_SITE_SECONDS, productdf.VISIT_CONVERSION_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on site and visit conversion is {corr:.3f}")
    col2.metric("Correlation between tos and visit conversion", round(corr,3))

    st.write('Would these improve with outlier removal?')
    col1, col2 = st.columns(2)
    productdf_filtered = productdf.filter(is_outlier("TIME_ON_SITE_SECONDS"))
    productdf_filtered = productdf_filtered.withColumn("visit_engagement_encoded",fn.when(productdf_filtered.SITE_VISIT_ENGAGEMENT == True, 1).otherwise(0))
    corr = productdf_filtered.select(fn.corr(productdf_filtered.TIME_ON_SITE_SECONDS, productdf_filtered.VISIT_ENGAGEMENT_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on site and visit engagement, with time on site outliers removed, is {corr:.3f}")
    col1.metric("Correlation between tos and visit enagagment", round(corr,3))

    productdf_filtered = productdf_filtered.withColumn("visit_conversion_encoded",fn.when(productdf_filtered.SITE_VISIT_CONVERSION == True, 1).otherwise(0))
    corr = productdf_filtered.select(fn.corr(productdf_filtered.TIME_ON_SITE_SECONDS, productdf_filtered.VISIT_CONVERSION_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on sie and visit conversion, with time on site outliers removed, is {corr:.3f}")
    col2.metric("Correlation between tos and visit conversion", round(corr,3))

    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.title('Change Over Time')

    st.subheader('Total change over time - grouped monthly')
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("month", (month("VISIT_DATE_TIME_UTC")))
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("year", (year("VISIT_DATE_TIME_UTC")))
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("month_year", to_date(concat("month", lit("/01/"), "year")))
    today = dt.datetime.today()
    datem = dt.datetime(today.year, today.month, 1)
    st.line_chart(data = f_aa_engagement_by_site.filter(f_aa_engagement_by_site["month_year"] != datem).group_by('MONTH_YEAR').agg(dict).collect(), x='MONTH_YEAR')
    st.write("")

    #st.subheader('Average change over time - grouped monthly')
    #print(f_aa_engagement_by_site.group_by('MONTH_YEAR').agg({"TIME_ON_SITE_SECONDS": "avg"}).show(5))
    #st.line_chart(data = f_aa_engagement_by_site.group_by('MONTH_YEAR').agg({"TIME_ON_SITE_SECONDS": "avg"}).collect(), x = 'MONTH_YEAR', y = "AVG(TIME_ON_SITE_SECONDS)")
    #st.write("")

    st.subheader('Total change over time - grouped weekly')
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("Monday_date", to_date(dateadd('day', ((dayofweek("VISIT_DATE_TIME_UTC")-1)*-1), col("VISIT_DATE_TIME_UTC"))))
    today = dt.datetime.today()
    last_monday = today - dt.timedelta(days=today.weekday())
    st.line_chart(data = f_aa_engagement_by_site.filter(f_aa_engagement_by_site["Monday_date"] != last_monday.date()).group_by('Monday_date').agg(dict).collect(), x = 'MONDAY_DATE')
    st.write("")

    #st.subheader('Average change over time - grouped weekly')
    #f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("Monday_date", to_date(dateadd('day', ((dayofweek("VISIT_DATE_TIME_UTC")-1)*-1), col("VISIT_DATE_TIME_UTC"))))
    #st.line_chart(data = f_aa_engagement_by_site.group_by('Monday_date').agg(col('TIME_ON_SITE_SECONDS'),'avg').collect(), x = 'MONDAY_DATE', y = "AVG(TIME_ON_SITE_SECONDS)" )
    #st.write("")

elif option == 'Marketing channel':
    f_aa_engagement_by_site = main_session.table('dimensional.f_aa_engagement_by_site')


    marketing_group = f_aa_engagement_by_site.groupBy('channel_name')
    average = marketing_group.agg({"time_on_site_seconds": "avg"})
    distinct_ids = [x.CHANNEL_NAME for x in average.select('channel_name').distinct().collect()]
    
    st.write("")
    st.write("")
    st.header('Distrubtion')
    for i in distinct_ids:
        f_aa_engagement_by_site = f_aa_engagement_by_site.withColumn(i,fn.when(f_aa_engagement_by_site.channel_name == i, 1).otherwise(0))
    
    dict = {}
    keys = range(len(distinct_ids))
    for i in distinct_ids:
            dict[i] = "sum"
    print(dict)

    st.subheader('Marketing Channel Distrubtion')
    st.bar_chart(data = f_aa_engagement_by_site.group_by('CHANNEL_NAME').agg((col("*"), "count")).collect(), height=500, x = 'CHANNEL_NAME')
    st.write("")
    st.write("")
    st.title('Marketing Channel Segmented Analysis')
    st.write("")

    option = st.selectbox(
    'Which Marketing Channel type would you like to view?',
    distinct_ids)
    st.write('You selected:', option)

    st.subheader('Descriptive Statistics')
    marketingdf= f_aa_engagement_by_site.filter(col('CHANNEL_NAME') == option)
    col1, col2, col3, col4= st.columns(4)
    avg = re.findall(r"(\d+(?:\.\d+)?)", str(marketingdf.agg({"time_on_site_seconds": "avg"}).collect()))[0]
    col1.metric("Average", round(float(avg),1))
    std = re.findall(r"(\d+(?:\.\d+)?)", str(marketingdf.agg({"time_on_site_seconds": "std"}).collect()))[0]
    col2.metric("Standard Deviation", round(float(std),1))
    min = re.findall(r"(\d+(?:\.\d+)?)", str(marketingdf.agg({"time_on_site_seconds": "min"}).collect()))[0]
    col3.metric("Minumum", round(float(min),1))
    max = re.findall(r"(\d+(?:\.\d+)?)", str(marketingdf.agg({"time_on_site_seconds": "max"}).collect()))[0]
    col4.metric("Maximum", round(float(max),1))

    quartiles = marketingdf.approx_quantile("time_on_site_seconds", [0.25, 0.5, 0.75, 0.9])
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Median", round(float(quartiles[1]),1))
    col2.metric("25th percentile", round(float(quartiles[0]),1))
    col3.metric("75th percentile", round(float(quartiles[2]),1))
    col4.metric("90th percentile", round(float(quartiles[3]),1))
    st.write("")
    st.write("")

    st.subheader('Correlations with other key metrics')
    col1, col2 = st.columns(2)

    marketingdf = marketingdf.withColumn("visit_engagement_encoded",fn.when(marketingdf.SITE_VISIT_ENGAGEMENT == True, 1).otherwise(0))
    corr = marketingdf.select(fn.corr(marketingdf.TIME_ON_SITE_SECONDS, marketingdf.VISIT_ENGAGEMENT_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on site and visit engagement is {corr:.3f}")
    col1.metric("Correlation between tos and visit enagagment", round(corr,3))

    marketingdf = marketingdf.withColumn("visit_conversion_encoded",fn.when(marketingdf.SITE_VISIT_CONVERSION == True, 1).otherwise(0))
    corr = marketingdf.select(fn.corr(marketingdf.TIME_ON_SITE_SECONDS, marketingdf.VISIT_CONVERSION_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on site and visit conversion is {corr:.3f}")
    col2.metric("Correlation between tos and visit conversion", round(corr,3))

    st.write('Would these improve with outlier removal?')
    col1, col2 = st.columns(2)
    marketingdf_filtered = marketingdf.filter(is_outlier("TIME_ON_SITE_SECONDS"))
    marketingdf_filtered = marketingdf_filtered.withColumn("visit_engagement_encoded",fn.when(marketingdf_filtered.SITE_VISIT_ENGAGEMENT == True, 1).otherwise(0))
    corr = marketingdf_filtered.select(fn.corr(marketingdf_filtered.TIME_ON_SITE_SECONDS, marketingdf_filtered.VISIT_ENGAGEMENT_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on site and visit engagement, with time on site outliers removed, is {corr:.3f}")
    col1.metric("Correlation between tos and visit enagagment", round(corr,3))

    marketingdf_filtered = marketingdf_filtered.withColumn("visit_conversion_encoded",fn.when(marketingdf_filtered.SITE_VISIT_CONVERSION == True, 1).otherwise(0))
    corr = marketingdf_filtered.select(fn.corr(marketingdf_filtered.TIME_ON_SITE_SECONDS, marketingdf_filtered.VISIT_CONVERSION_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on sie and visit conversion, with time on site outliers removed, is {corr:.3f}")
    col2.metric("Correlation between tos and visit conversion", round(corr,3))

    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.title('Change Over Time')

    st.subheader('Total change over time - grouped monthly')
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("month", (month("VISIT_DATE_TIME_UTC")))
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("year", (year("VISIT_DATE_TIME_UTC")))
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("month_year", to_date(concat("month", lit("/01/"), "year")))
    today = dt.datetime.today()
    datem = dt.datetime(today.year, today.month, 1)
    st.line_chart(data = f_aa_engagement_by_site.filter(f_aa_engagement_by_site["month_year"] != datem).group_by('MONTH_YEAR').agg(dict).collect(), x='MONTH_YEAR')
    st.write("")

    #st.subheader('Average change over time - grouped monthly')
    #print(f_aa_engagement_by_site.group_by('MONTH_YEAR').agg({"TIME_ON_SITE_SECONDS": "avg"}).show(5))
    #st.line_chart(data = f_aa_engagement_by_site.group_by('MONTH_YEAR').agg({"TIME_ON_SITE_SECONDS": "avg"}).collect(), x = 'MONTH_YEAR', y = "AVG(TIME_ON_SITE_SECONDS)")
    #st.write("")

    st.subheader('Total change over time - grouped weekly')
    f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("Monday_date", to_date(dateadd('day', ((dayofweek("VISIT_DATE_TIME_UTC")-1)*-1), col("VISIT_DATE_TIME_UTC"))))
    today = dt.datetime.today()
    last_monday = today - dt.timedelta(days=today.weekday())
    st.line_chart(data = f_aa_engagement_by_site.filter(f_aa_engagement_by_site["Monday_date"] != last_monday.date()).group_by('Monday_date').agg(dict).collect(), x = 'MONDAY_DATE')
    st.write("")

    #st.subheader('Average change over time - grouped weekly')
    #f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("Monday_date", to_date(dateadd('day', ((dayofweek("VISIT_DATE_TIME_UTC")-1)*-1), col("VISIT_DATE_TIME_UTC"))))
    #st.line_chart(data = f_aa_engagement_by_site.group_by('Monday_date').agg(col('TIME_ON_SITE_SECONDS'),'avg').collect(), x = 'MONDAY_DATE', y = "AVG(TIME_ON_SITE_SECONDS)" )
    #st.write("")
    
elif option == 'Page Category':
    f_aa_engagement_by_site = main_session.table('dimensional.f_aa_engagement_by_site')
    d_page_hist = main_session.table('dimensional.d_page_hist')
    joined_df = f_aa_engagement_by_site.select(['D_SITE_HIST_SK','visit_date_utc','visit_id','time_on_site_seconds','site_visit_conversion','site_visit_engagement']).join(d_page_hist.select(['CLEANSED_URL_NK','D_SITE_HIST_SK','CATEGORY']), on=["D_SITE_HIST_SK"], how="inner")


    category_group = joined_df.groupBy('CATEGORY')
    average = category_group.agg({"time_on_site_seconds": "avg"})
    distinct_ids = [x.CATEGORY for x in average.select('CATEGORY').distinct().collect()]
    
    st.write("")
    st.write("")
    st.header('Distrubtion')
    for i in distinct_ids:
        joined_df = joined_df.withColumn(i,fn.when(joined_df.CATEGORY == i, 1).otherwise(0))
    
    dict = {}
    keys = range(len(distinct_ids))
    for i in distinct_ids:
            dict[i] = "sum"
    print(dict)

    st.subheader('Page Category Distrubtion')
    st.bar_chart(data = joined_df.select(['D_SITE_HIST_SK','CATEGORY']).group_by('CATEGORY').agg((col("*"), "count")).collect(), height=500, x = 'CATEGORY')
    st.write("")
    st.write("")
    st.title('Page Category Segmented Analysis')
    st.write("")

    option = st.selectbox(
    'Which Page Category type would you like to view?',
    distinct_ids)
    st.write('You selected:', option)

    st.subheader('Descriptive Statistics')
    categorydf= joined_df.filter(col('CATEGORY') == option)
    col1, col2, col3, col4= st.columns(4)
    avg = re.findall(r"(\d+(?:\.\d+)?)", str(categorydf.agg({"time_on_site_seconds": "avg"}).collect()))[0]
    col1.metric("Average", round(float(avg),1))
    std = re.findall(r"(\d+(?:\.\d+)?)", str(categorydf.agg({"time_on_site_seconds": "std"}).collect()))[0]
    col2.metric("Standard Deviation", round(float(std),1))
    min = re.findall(r"(\d+(?:\.\d+)?)", str(categorydf.agg({"time_on_site_seconds": "min"}).collect()))[0]
    col3.metric("Minumum", round(float(min),1))
    max = re.findall(r"(\d+(?:\.\d+)?)", str(categorydf.agg({"time_on_site_seconds": "max"}).collect()))[0]
    col4.metric("Maximum", round(float(max),1))

    quartiles = categorydf.approx_quantile("time_on_site_seconds", [0.25, 0.5, 0.75, 0.9])
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Median", round(float(quartiles[1]),1))
    col2.metric("25th percentile", round(float(quartiles[0]),1))
    col3.metric("75th percentile", round(float(quartiles[2]),1))
    col4.metric("90th percentile", round(float(quartiles[3]),1))
    st.write("")
    st.write("")

    st.subheader('Correlations with other key metrics')
    col1, col2 = st.columns(2)

    categorydf = categorydf.withColumn("visit_engagement_encoded",fn.when(categorydf.SITE_VISIT_ENGAGEMENT == True, 1).otherwise(0))
    corr = categorydf.select(fn.corr(categorydf.TIME_ON_SITE_SECONDS, categorydf.VISIT_ENGAGEMENT_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on site and visit engagement is {corr:.3f}")
    col1.metric("Correlation between tos and visit enagagment", round(corr,3))

    categorydf = categorydf.withColumn("visit_conversion_encoded",fn.when(categorydf.SITE_VISIT_CONVERSION == True, 1).otherwise(0))
    corr = categorydf.select(fn.corr(categorydf.TIME_ON_SITE_SECONDS, categorydf.VISIT_CONVERSION_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on site and visit conversion is {corr:.3f}")
    col2.metric("Correlation between tos and visit conversion", round(corr,3))

    st.write('Would these improve with outlier removal?')
    col1, col2 = st.columns(2)
    categorydf_filtered = categorydf.filter(is_outlier("TIME_ON_SITE_SECONDS"))
    categorydf_filtered = categorydf_filtered.withColumn("visit_engagement_encoded",fn.when(categorydf_filtered.SITE_VISIT_ENGAGEMENT == True, 1).otherwise(0))
    corr = categorydf_filtered.select(fn.corr(categorydf_filtered.TIME_ON_SITE_SECONDS, categorydf_filtered.VISIT_ENGAGEMENT_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on site and visit engagement, with time on site outliers removed, is {corr:.3f}")
    col1.metric("Correlation between tos and visit enagagment", round(corr,3))

    categorydf_filtered = categorydf_filtered.withColumn("visit_conversion_encoded",fn.when(categorydf_filtered.SITE_VISIT_CONVERSION == True, 1).otherwise(0))
    corr = categorydf_filtered.select(fn.corr(categorydf_filtered.TIME_ON_SITE_SECONDS, categorydf_filtered.VISIT_CONVERSION_ENCODED)).collect()[0][0]
    #st.write(f"The correlation coefficient between time on sie and visit conversion, with time on site outliers removed, is {corr:.3f}")
    col2.metric("Correlation between tos and visit conversion", round(corr,3))

    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.write("")
    st.title('Change Over Time')

    st.subheader('Total change over time - grouped monthly')
    joined_df = joined_df.with_column("month", (month("VISIT_DATE_TIME_UTC")))
    joined_df = joined_df.with_column("year", (year("VISIT_DATE_TIME_UTC")))
    joined_df = joined_df.with_column("month_year", to_date(concat("month", lit("/01/"), "year")))
    st.line_chart(data = joined_df.group_by('MONTH_YEAR').agg(dict).collect(), x='MONTH_YEAR')
    st.write("")

    #st.subheader('Average change over time - grouped monthly')
    #print(f_aa_engagement_by_site.group_by('MONTH_YEAR').agg({"TIME_ON_SITE_SECONDS": "avg"}).show(5))
    #st.line_chart(data = f_aa_engagement_by_site.group_by('MONTH_YEAR').agg({"TIME_ON_SITE_SECONDS": "avg"}).collect(), x = 'MONTH_YEAR', y = "AVG(TIME_ON_SITE_SECONDS)")
    #st.write("")

    st.subheader('Total change over time - grouped weekly')
    joined_df = joined_df.with_column("Monday_date", to_date(dateadd('day', ((dayofweek("VISIT_DATE_TIME_UTC")-1)*-1), col("VISIT_DATE_TIME_UTC"))))
    st.line_chart(data = joined_df.group_by('Monday_date').agg(dict).collect(), x = 'MONDAY_DATE')
    st.write("")

    #st.subheader('Average change over time - grouped weekly')
    #f_aa_engagement_by_site = f_aa_engagement_by_site.with_column("Monday_date", to_date(dateadd('day', ((dayofweek("VISIT_DATE_TIME_UTC")-1)*-1), col("VISIT_DATE_TIME_UTC"))))
    #st.line_chart(data = f_aa_engagement_by_site.group_by('Monday_date').agg(col('TIME_ON_SITE_SECONDS'),'avg').collect(), x = 'MONDAY_DATE', y = "AVG(TIME_ON_SITE_SECONDS)" )
    #st.write("")
    


#main_session.close()