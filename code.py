import streamlit as st
import pandas as pd
import psycopg2 as pg
import numpy as np
engine = pg.connect("dbname='huzzle_production' user='postgres' host='huzzle-production-db-read.ct4mk1ahmp9p.eu-central-1.rds.amazonaws.com' port='5432' password='S11mXHLGbA0Cb8z8uLfj'")
df_touchpoints = pd.read_sql('select * from touchpoints', con=engine)
df_tags = pd.read_sql('select * from tags', con=engine)
df_tagging = pd.read_sql('select * from taggings', con=engine)
df_universities = pd.read_sql('select * from universities', con=engine)
df_cities = pd.read_sql('select * from cities', con=engine)
df_cities.rename(columns = {'name':'city_name'}, inplace = True)
df_subjects = pd.read_sql('select * from subjects', con=engine)
subject_topics = pd.read_sql('select * from subjects_topics', con=engine)
df_degrees = pd.read_sql('select * from degrees', con=engine)
df_degrees.replace("Bachelor's","Bachelors", inplace=True)
df_degrees.replace("Master's","Masters", inplace=True)
df_tc = pd.read_sql('select * from touchpoints_cities', con=engine)
df_goals = pd.read_sql('select * from goals', con=engine)
df_goal_weights = pd.read_sql('select * from matching_goal_weights', con=engine)
grouped_1 = df_touchpoints.groupby(df_touchpoints.state)
df_touchpoints = grouped_1.get_group(1)
df_touchpoints =  pd.merge(df_touchpoints, df_tagging, left_on='id',right_on='taggable_id',suffixes=('', '_x'))
df_touchpoints = df_touchpoints.loc[:,~df_touchpoints.columns.duplicated()]
df_touchpoints = pd.merge(df_touchpoints,df_tags,left_on='tag_id',right_on='id',suffixes=('', '_x'))
df = df_touchpoints.loc[:,~df_touchpoints.columns.duplicated()]
df = pd.merge(df,df_tc,left_on='id',right_on='touchpoint_id',suffixes=('', '_x'),how = 'left')
df = df.loc[:,~df.columns.duplicated()]
df = pd.merge(df,df_cities,left_on='city_id',right_on='id',suffixes=('', '_x'),how = 'left')
df = df.loc[:,~df.columns.duplicated()]
df_universities_1 = pd.merge(df_universities, df_cities, left_on='city_id',right_on='id',suffixes=('', '_x'),how = 'left')
df_universities_1 = df_universities_1.loc[:,~df_universities_1.columns.duplicated()]
df_goals = pd.merge(df_goals, df_goal_weights, left_on='id',right_on='goal_id',suffixes=('', '_x'),how = 'inner')
df_goals = df_goals.loc[:,~df_goals.columns.duplicated()]
df_goals = df_goals[['id','title','touchpointable_kind','value']].copy()
df_goals.rename(columns = {'title':'goal'}, inplace = True)
df_subjects = pd.merge(df_subjects, subject_topics, left_on='id',right_on='subject_id',suffixes=('', '_x'),how = 'inner')
df_subjects = df_subjects.loc[:,~df_subjects.columns.duplicated()]
df_subjects = pd.merge(df_subjects,df_tags,left_on='topic_id',right_on='id',suffixes=('', '_x'))
df_subjects = df_subjects.loc[:,~df_subjects.columns.duplicated()]
goals = ['Start my Career with a Spring Week','Get a Summer Internship','Get an Internship alongside my Studies', 'Land a Placement Year','Win Awards & Competitions','Secure a Graduate Job','Find a Co-founder & Start a Business', 'Meet Like-minded Students & join Societies','Expand my Network & Connect with Industry Leaders']
Goals =  st.multiselect('Enter the goals',goals,key = "one")
group_6 = df.groupby(df.type)
df_T = group_6.get_group("Topic")
interest = st.multiselect('Enter the interest',df_T['name'].unique(),key = "two")
weight = [1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,1,2,1]
Weight = st.multiselect('Enter the weight',weight,key = "three")
Interest = pd.DataFrame(interest,columns = ['Interest'])
Weight = pd.DataFrame(Weight,columns = ['Weight'])
df_interest = pd.concat([Interest,Weight],axis = 1)
University = st.selectbox('Enter the university',df_universities['name'].unique(),key = 'four')
Subject = st.selectbox('Enter the subject',df_subjects['name'].unique(),key = 'five')
Degree =  st.selectbox('Enter the degree',df_degrees['name'].unique(),key = 'six')
year = ['First Year ','Second Year','Third Year','Final Year']
Year = st.selectbox('Enter the year',year,key = 'seven')
if st.button('Submit',key = 'eight') or st.session_state.load_state:
  st.session_state.load_state == True
  if len(Goals) != 0:
    goals_1 =  pd.DataFrame(Goals,columns =['Goals'])
    df_goals = pd.merge(df_goals, goals_1, left_on='goal',right_on='Goals',suffixes=('', '_x'),how = 'inner')
    df_goals = df_goals.loc[:,~df_goals.columns.duplicated()]
    df =  pd.merge(df, df_goals, left_on='kind',right_on='touchpointable_kind',suffixes=('', '_x'),how = 'inner')
    df = df.loc[:,~df.columns.duplicated()]
    st.write(df)
