from streamlit import caching
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
def callback():
  if "one" not in st.session_state:
    Goals = []
    Goals = st.session_state.one
    goals_1 =  pd.DataFrame(Goals,columns =['Goals'])
    df_goals = pd.merge(df_goals, goals_1, left_on='title',right_on='Goals',suffixes=('', '_x'),how = 'inner')
    df_goals = df_goals.loc[:,~df_goals.columns.duplicated()]
    df =  pd.merge(df, df_goals, left_on='kind',right_on='touchpointable_kind',suffixes=('', '_x'),how = 'inner')
    df = df.loc[:,~df.columns.duplicated()]

#if "two" not in st.session_state:
  #interest = []
  #interest = st.session_state.two
  #Interest = pd.DataFrame(interest,columns = ['Interest'])
#if "three" not in st.session_state:
  #Weight = []
  #Weight = st.session_state.three
  #Weight = pd.DataFrame(Weight,columns = ['Weight'])
  #if len(interest) > 0:
    #group_7 = df.groupby(df.type)
    #df_I = group_7.get_group("Topic")
    #df_interest = pd.concat([Interest,Weight],axis = 1)
    #df_I =  pd.merge(df_I, df_interest, left_on='name',right_on='Interest',suffixes=('', '_x'),how = 'inner')
    #df_I = df_I.loc[:,~df_I.columns.duplicated()]
    #col_list = df_I['name'].unique()
    #df_I['idx'] = df_I.groupby(['touchpointable_id', 'name']).cumcount()
    #df_I = df_I.pivot(index=['idx','touchpointable_id'], columns='name', values='Weight').sort_index(level=1).reset_index().rename_axis(None, axis=1)
    #df_I = df_I.fillna(0)
    #df_I['Weight'] = df_I[col_list].sum(axis=1)
    #df_I = pd.merge(df, df_I, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
    #df_I = df_I.loc[:,~df_I.columns.duplicated()]
  #if len(interest) == 0:
    #df_I = df
    #df_I['Weight'] = 0
  
#if "four" not in st.session_state:
  #University = st.session_state["four"]
  #df_universities_1 = df_universities_1.loc[df_universities_1['name'] == University]
  #city_name = df_universities_1.iloc[0]['city_name']
  #df_I['city score'] = np.where(df_I['city_name'] == city_name, 1,0)
#if "five" not in st.session_state:
  #Degree = st.session_state["five"]
  #df_I['degree score'] = np.where(df_I['name'] == Degree, 1,0)
  #df_E = df_I.loc[df_I['type'] == 'EducationRequirement']

  #id = df_E['id'].to_list()
  #df_E = df_I[~df_I.id.isin(id)]
  #df_E = pd.merge(df, df_E, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
  #df_E = df_E.loc[:,~df_E.columns.duplicated()]
  #df_D = df_I.loc[df_I['degree score'] == 1]
  #df_T = pd.merge(df, df_D, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
  #df_T = df_T.loc[:,~df_T.columns.duplicated()]
  #df_T = pd.concat([df_T,df_E])
#if "six" not in st.session_state:
  #Subject = st.session_state["six"]
  #df_subjects_1 = df_subjects.loc[df_subjects['name'] == Subject]
  #df_subjects_1['subject score'] = 0.5
  #df_T = pd.merge(df_T,df_subjects_1, left_on='name',right_on='name_x',suffixes=('', '_x'),how = 'left')
  #df_T = df_T.loc[:,~df_T.columns.duplicated()]
  #df_S = df_T.loc[df_T['subject score'] == 0.5]
  #df_S = pd.merge(df, df_S, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
  #df_S = df_S.loc[:,~df_S.columns.duplicated()]
  #id = df_S['id'].to_list()
  #df_T = df_T[~df_T.id.isin(id)]
  #df_T = pd.concat([df_T,df_S])
#if "seven" not in st.session_state:
  #Year = st.session_state["seven"]
  #df_T['year score'] = np.where(df_T['name'] == Year, 1,0)
  
  #df_Y = df_T.loc[df_T['year score'] == 1]
  #df_Y = pd.merge(df, df_S, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
  #df_Y = df_Y.loc[:,~df_Y.columns.duplicated()]
  #id_y = df_Y['id'].to_list()
  #df_T = df_T[~df_T.id.isin(id_y)]
  #df_A =  pd.concat([df_T,df_Y])
  #df_A = df_A[['id','touchpointable_id','type','touchpointable_type','kind','title','name','creatable_for_name','Weight','city_name','city score','degree score','subject score','year score','value']].copy()
  #col_list = ['Weight','city score','degree score','subject score','year score']
  #df_A['matching score'] = df_A[col_list].sum(axis=1)
  #df = df_A.sort_values(by='matching score',ascending=False)
  #goals = ['Start my Career with a Spring Week','Get a Summer Internship','Get an Internship alongside my Studies', 'Land a Placement Year','Win Awards & Competitions','Secure a Graduate Job','Find a Co-founder & Start a Business', 'Meet Like-minded Students & join Societies','Expand my Network & Connect with Industry Leaders']
Goals =  st.multiselect('Enter the goals',goals,key = "one")
#group_6 = df.groupby(df.type)
#df_T = group_6.get_group("Topic")
#interest = st.multiselect('Enter the interest',df_T['name'].unique(),key = "two")
#weight = [1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,1,2,1]
#Weight = st.multiselect('Enter the weight',weight,key = "three")

#University = st.selectbox('Enter the university',df_universities['name'].unique(),key = 'four')
#Subject = st.selectbox('Enter the subject',df_subjects['name'].unique(),key = 'five')
#Degree =  st.selectbox('Enter the degree',df_degrees['name'].unique(),key = 'six')
#year = ['First Year ','Second Year','Third Year','Final Year']
#Year = st.selectbox('Enter the year',year,key = 'seven')
if st.button('Submit',key = 'eight',on_click=callback): 
    st.write(df)
