import streamlit as st
import pandas as pd
import psycopg2 as pg
import numpy as np



engine = pg.connect("dbname='huzzle_production' user='postgres' host='huzzle-production-db-read.ct4mk1ahmp9p.eu-central-1.rds.amazonaws.com' port='5432' password='S11mXHLGbA0Cb8z8uLfj'")
df_goals = pd.read_sql('select * from goals', con=engine)
df_tags = pd.read_sql('select * from tags', con=engine)
weight = [1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,1,2,1]
df_universities = pd.read_sql('select * from universities', con=engine)
df_subjects = pd.read_sql('select * from subjects', con=engine)
df_degrees = pd.read_sql('select * from degrees', con=engine)
df_degrees.replace("Bachelor's","Bachelors", inplace=True)
df_degrees.replace("Master's","Masters", inplace=True)
year = ['First Year ','Second Year','Third Year','Final Year']


@st.cache(ttl=24*3600)
def user_input():
  Goals =  st.multiselect('Enter the goals',df_goals['title'].unique(),key = "one")
  Interest = st.multiselect('Enter the interest',df_tags['name'].unique(),key = "two")
  Weight = st.multiselect('Enter the weight',weight,key = "three")
  University = st.selectbox('Enter the university',df_universities['name'].unique(),key = 'four')
  Subject = st.selectbox('Enter the subject',df_subjects['name'].unique(),key = 'five')
  Degree =  st.selectbox('Enter the degree',df_degrees['name'].unique(),key = 'six')
  Year = st.selectbox('Enter the year',year,key = 'seven')
  
  return Goals,Interest,Weight,University,Subject,Degree,Year

