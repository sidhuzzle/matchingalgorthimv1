import streamlit as st
import pandas as pd
import psycopg2 as pg
import numpy as np



engine = pg.connect("dbname='huzzle_production' user='postgres' host='huzzle-production-db-read.ct4mk1ahmp9p.eu-central-1.rds.amazonaws.com' port='5432' password='S11mXHLGbA0Cb8z8uLfj'")

@st.cache(ttl=24*3600)
@st.cache(suppress_st_warning=True)
@st.cache(hash_funcs={pg:engine})
def data():
  df_goals = pd.read_sql('select * from goals', con=engine)
  df_tags = pd.read_sql('select * from tags', con=engine)
  weight = [1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,1,2,1]
  df_universities = pd.read_sql('select * from universities', con=engine)
  df_subjects = pd.read_sql('select * from subjects', con=engine)
  df_degrees = pd.read_sql('select * from degrees', con=engine)
  df_degrees.replace("Bachelor's","Bachelors", inplace=True)
  df_degrees.replace("Master's","Masters", inplace=True)
  year = ['First Year ','Second Year','Third Year','Final Year']
  
  return df_goals,df_tags,weight,df_universities,df_subjects,df_degrees,year
  Goals =  st.multiselect('Enter the goals',df_goals['title'].unique(),key = "one")
  Interest = st.multiselect('Enter the interest',df_tags['name'].unique(),key = "two")
  Weight = st.multiselect('Enter the weight',weight,key = "three")
  University = st.selectbox('Enter the university',df_universities['name'].unique(),key = 'four')
  Subject = st.selectbox('Enter the subject',df_subjects['name'].unique(),key = 'five')
  Degree =  st.selectbox('Enter the degree',df_degrees['name'].unique(),key = 'six')
  Year = st.selectbox('Enter the year',year,key = 'seven')
  #return Goals,Interest,Weight,University,Subject,Degree,Year


  df_goal_weights = pd.read_sql('select * from matching_goal_weights', con=engine)
  df_touchpoints = pd.read_sql('select * from touchpoints', con=engine)
  grouped_1 = df_touchpoints.groupby(df_touchpoints.state)
  df_touchpoints = grouped_1.get_group(1)
  df_tagging = pd.read_sql('select * from taggings', con=engine)
  df_touchpoints = grouped_1.get_group(1)
  df_touchpoints =  pd.merge(df_touchpoints, df_tagging, left_on='id',right_on='taggable_id',suffixes=('', '_x'))
  df_touchpoints = df_touchpoints.loc[:,~df_touchpoints.columns.duplicated()]
  df = pd.merge(df_touchpoints,df_tags,left_on='tag_id',right_on='id',suffixes=('', '_x'))
  df = df.loc[:,~df.columns.duplicated()]
  df_goals = pd.merge(df_goals, df_goal_weights, left_on='id',right_on='goal_id',suffixes=('', '_x'),how = 'inner')
  df_goals = df_goals.loc[:,~df_goals.columns.duplicated()]
  df_goals = df_goals[['id','title','touchpointable_kind','value']].copy()
  df_goals.rename(columns = {'title':'goal'}, inplace = True)
  goals_1 =  pd.DataFrame(Goals,columns =['Goals'])
  df_goals = pd.merge(df_goals, goals_1, left_on='goal',right_on='Goals',suffixes=('', '_x'),how = 'inner')
  df_goals = df_goals.loc[:,~df_goals.columns.duplicated()]
  df =  pd.merge(df, df_goals, left_on='kind',right_on='touchpointable_kind',suffixes=('', '_x'),how = 'inner')
  df = df.loc[:,~df.columns.duplicated()]
  st.write(df)  
data()

