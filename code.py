import streamlit as st
import pandas as pd
import psycopg2 as pg
import numpy as np

engine = pg.connect("dbname='huzzle_production' user='postgres' host='huzzle-production-db-read.ct4mk1ahmp9p.eu-central-1.rds.amazonaws.com' port='5432' password='S11mXHLGbA0Cb8z8uLfj'")
df_goals = pd.read_sql('select * from goals', con=engine)
df_tags = pd.read_sql('select * from tags', con=engine)
df_universities = pd.read_sql('select * from universities', con=engine)
universities = df_universities['name'].unique()
universities = np.insert(universities,0,'Select an University')
df_degrees = pd.read_sql('select * from degrees', con=engine)
degree = df_degrees['name'].unique()
degree = np.insert(degree,0,'Select a Degree')                
df_subjects = pd.read_sql('select * from subjects', con=engine)
subject = df_subjects['name'].unique()
subject = np.insert(subject,0,'Select a Subject') 
weight = [1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,1]
year = ['Select a Year','1','2','3','4']


@st.cache(func=None, persist=False, allow_output_mutation=True, show_spinner=False, suppress_st_warning=False, hash_funcs=None, max_entries=1000, ttl=24*3600)
def matching_algo(Goals,Interest,weight,University,Degree,Subject,Year):
  engine = pg.connect("dbname='huzzle_production' user='postgres' host='huzzle-production-db-read.ct4mk1ahmp9p.eu-central-1.rds.amazonaws.com' port='5432' password='S11mXHLGbA0Cb8z8uLfj'")
  df_touchpoints = pd.read_sql('select * from touchpoints', con=engine)
  grouped_1 = df_touchpoints.groupby(df_touchpoints.state)
  df_touchpoints = grouped_1.get_group(1)
  df_tags = pd.read_sql('select * from tags', con=engine)
  df_tagging = pd.read_sql('select * from taggings', con=engine)
  df_tc = pd.read_sql('select * from touchpoints_cities', con=engine)
  df_cities = pd.read_sql('select * from cities', con=engine)
  df_cities.rename(columns = {'name':'city_name'}, inplace = True)
  df =  pd.merge(df_touchpoints, df_tagging, left_on='id',right_on='taggable_id',suffixes=('', '_x'))
  df = df.loc[:,~df.columns.duplicated()]
  df = pd.merge(df,df_tags,left_on='tag_id',right_on='id',suffixes=('', '_x'))
  df = df.loc[:,~df.columns.duplicated()]
  df = pd.merge(df,df_tc,left_on='id',right_on='touchpoint_id',suffixes=('', '_x'),how = 'left')
  df = df.loc[:,~df.columns.duplicated()]
  df = pd.merge(df,df_cities,left_on='city_id',right_on='id',suffixes=('', '_x'),how = 'left')
  df = df.loc[:,~df.columns.duplicated()]
  df = df[['id','touchpointable_id','type','touchpointable_type','kind','title','name','creatable_for_name','city_name']]
  df.replace("Bachelors","Bachelor's", inplace=True)
  df.replace("Masters","Master's", inplace=True)
  df.replace("First Year","1",inplace = True)
  df.replace("Second Year","2",inplace = True)
  df.replace("Third Year","3",inplace = True)
  df.replace("Fourth Year","4",inplace = True)  

  df_goals = pd.read_sql('select * from goals', con=engine)
  df_matching_goal_weights = pd.read_sql('select * from matching_goal_weights', con=engine)
  df_goals_weights = pd.merge(df_goals, df_matching_goal_weights, left_on='id',right_on='goal_id',suffixes=('', '_x'),how = 'inner')
  df_goals_weights = df_goals_weights.loc[:,~df_goals_weights.columns.duplicated()]

  df_universities = pd.read_sql('select * from universities', con=engine)
  #df_universities_1 = pd.merge(df_universities, df_cities, left_on='city_id',right_on='id',suffixes=('', '_x'),how = 'left')
  #df_universities_1 = df_universities_1.loc[:,~df_universities_1.columns.duplicated()]    

  df_degrees = pd.read_sql('select * from degrees', con=engine)
  
  df_subjects = pd.read_sql('select * from subjects', con=engine)
  

  year = ['1','2','3','4']


  if len(Goals) > 0:
    goals_1 =  pd.DataFrame(Goals,columns =['Goals'])
    df_goals = pd.merge(df_goals_weights, goals_1, left_on='title',right_on='Goals',suffixes=('', '_x'),how = 'inner')
    df_goals = df_goals.loc[:,~df_goals.columns.duplicated()]
    df =  pd.merge(df, df_goals, left_on='kind',right_on='touchpointable_kind',suffixes=('', '_x'),how = 'inner')
    df = df.loc[:,~df.columns.duplicated()]
    
  else:

    df['value'] = 0


  if len(Interest) > 0:

    interest = pd.DataFrame(Interest,columns = ['Interest'])
    Weight = pd.DataFrame(weight,columns = ['Weight'])
    df_interest = pd.concat([interest,Weight],axis = 1)

    df_I =  pd.merge(df, df_interest, left_on='name',right_on='Interest',suffixes=('', '_x'),how = 'inner')
    df_I = df_I.loc[:,~df_I.columns.duplicated()]
    col_list = df_I['name'].unique()
    df_I['idx'] = df_I.groupby(['touchpointable_id', 'name']).cumcount()
    df_I = df_I.pivot(index=['idx','touchpointable_id'], columns='name', values='Weight').sort_index(level=1).reset_index().rename_axis(None, axis=1)
    df_I = df_I.fillna(0)
    df_I['Weight'] = df_I[col_list].sum(axis=1)
    df = pd.merge(df, df_I, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
    df = df.loc[:,~df.columns.duplicated()]
  else:
    df['Weight'] = 0
  if University in df_universities['name'].unique():
    df_universities_1 = pd.merge(df_universities, df_cities, left_on='city_id',right_on='id',suffixes=('', '_x'),how = 'left')
    df_universities_1 = df_universities_1.loc[:,~df_universities_1.columns.duplicated()]
    df_universities_1 = df_universities_1.loc[df_universities_1['name'] == University]
    city_name = df_universities_1.iloc[0]['city_name']
    df['city score'] = np.where(df['city_name'] == city_name, 1,0)
  else:
    df['city score'] = 0
  if Degree in df_degrees['name'].unique():
    df['degree score'] = np.where(df['name'] == Degree, 1,0)
    df_O = df[df['name'] == 'Open to All Students']
    df_O = pd.merge(df, df_O, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
    df_O = df_O.loc[:,~df_O.columns.duplicated()]
    df_E = df.loc[df['type'] == 'EducationRequirement']
    id = df_E['id'].to_list()
    df_E = df[~df.id.isin(id)]
    df_E = pd.merge(df, df_E, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
    df_E = df_E.loc[:,~df_E.columns.duplicated()]
    df_D = df.loc[df['degree score'] == 1]
    df = pd.merge(df, df_D, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
    df = df.loc[:,~df.columns.duplicated()]
    df = pd.concat([df,df_E])
    df = pd.concat([df,df_O])
    
  else:
    df['degree score'] = 0
  if Subject in df_subjects['name'].unique():
    subject_topics = pd.read_sql('select * from subjects_topics', con=engine)
    df_subjects_1 = pd.merge(df_subjects, subject_topics, left_on='id',right_on='subject_id',suffixes=('', '_x'),how = 'inner')
    df_subjects_1 = df_subjects_1.loc[:,~df_subjects_1.columns.duplicated()]
    df_subjects_1 = pd.merge(df_subjects_1,df_tags,left_on='topic_id',right_on='id',suffixes=('', '_x'))
    df_subjects_1 = df_subjects_1.loc[:,~df_subjects_1.columns.duplicated()]
    df_subjects_1 = df_subjects_1.loc[df_subjects_1['name'] == Subject]
    df_subjects_1['subject score'] = 0.5
    df = pd.merge(df,df_subjects_1, left_on='name',right_on='name_x',suffixes=('', '_x'),how = 'left')
    df = df.loc[:,~df.columns.duplicated()]
    df_S = df.loc[df['subject score'] == 0.5]
    df_S = pd.merge(df, df_S, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
    df_S = df_S.loc[:,~df_S.columns.duplicated()]
    id = df_S['id'].to_list()
    df = df[~df.id.isin(id)]
    df = pd.concat([df,df_S])
  else:
    df['subject score'] = 0
  
  if Year in year:
    df['year score'] = np.where(df['name'] == Year, 1,0)
    df_Y = df.loc[df['year score'] == 1]
    df_Y = pd.merge(df, df_Y, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
    df_Y = df_Y.loc[:,~df_Y.columns.duplicated()]
    id = df_Y['id'].to_list()
    df = df[~df.id.isin(id)]
    df =  pd.concat([df,df_Y])
  else:
    df['year score'] = 0
  
  df = df[['id','touchpointable_id','type','touchpointable_type','kind','title','name','creatable_for_name','Weight','city_name','city score','degree score','subject score','year score','value']].copy()
  col_list = ['Weight','city score','degree score','subject score','year score']
  df['matching score'] = df[col_list].sum(axis=1)
  return df.sort_values(by='matching score',ascending=False)
Goals =  st.multiselect('Enter the goals',df_goals['title'].unique(),key = "one")
Interest = st.multiselect('Enter the interest',df_tags['name'].unique(),key = "two")
weight = [1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,1]
weight = st.multiselect('Enter the weight',weight,key = "three")
University = st.selectbox('Enter the university',universities,key = 'four')
Subject = st.selectbox('Enter the subject',subject,key = 'five')
Degree =  st.selectbox('Enter the degree',degree,key = 'six')
Year = st.selectbox('Enter the year',year,key = 'seven')

if st.button("Submit",key = "eight"):
  
  
  df = matching_algo(Goals,Interest,weight,University,Degree,Subject,Year)
  kind = df.groupby("kind")
  for group,df_1 in kind:
    df_1 = pd.DataFrame(df_1)
    n = df_1['value'].iloc[0]
    n = round(len(df_1)*(n/10))
    df = df_1.head(n)
    #df_touchpoints = pd.read_sql('select * from touchpoints', con=engine)
    
    st.write(df)
  if len(df['value'].unique()) > 1:
    group_0 = df.groupby(df.touchpointable_type)
    df_Events = group_0.get_group("Event")
    group_1 = df.groupby(df.touchpointable_type)
    df_Internship = group_1.get_group("Internship")
    group_2 = df.groupby(df.touchpointable_type)
    df_Job = group_2.get_group("Job")
    if 'Foundation' in Degree:
      if '1' in Year:
        n = 7
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 3
        n = round(len(df_Internship)*(n/10))
        df_Internship = df_Internship.head(n)
        df =  pd.concat([df_Events,df_Internship])
    if "Bachelor's" in Degree:
      if  "1" in Year:

        n = 4
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 6
        n = round(len(df_Events)*(n/10))
        df_Internship = df_Internship.head(n)
        df =  pd.concat([df_Events,df_Internship])
    if "Bachelor's" in Degree:
      if  "2" in Year:
        n = 3
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 6
        n = round(len(df_Events)*(n/10))
        df_Internship = df_Internship.head(n)
        n = 1
        df_Job = df_Job.head(n)
        df =  pd.concat([df_Events,df_Internship])
        df =  pd.concat([df,df_Job])
    if "Bachelor's" in Degree:
      if  "3" in Year:
        n = 2
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 2
        n = round(len(df_Events)*(n/10))
        df_Internship = df_Internship.head(n)
        n = 6
        n = round(len(df_Events)*(n/10))
        df_Job = df_Job.head(n)
        df =  pd.concat([df_Job,df_Internship])
        df =  pd.concat([df,df_Events])
    if "Bachelor's (Integrated Master's)" in Degree:
      if  "1" in Year:
        n = 5
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 5
        n = round(len(df_Events)*(n/10))
        df_Internship = df_Internship.head(n)
        df =  pd.concat([df_Events,df_Internship])
    if "Bachelor's (Integrated Master's)" in Degree:
      if  "2" in Year:
        n = 4
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 6
        n = round(len(df_Events)*(n/10))
        df_Internship = df_Internship.head(n)
        df =  pd.concat([df_Events,df_Internship])

    if "Bachelor's (Integrated Master's)" in Degree:
      if  "3" in Year:
        n = 2
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 4
        n = round(len(df_Events)*(n/10))
        df_Internship = df_Internship.head(n)
        n = 4
        n = round(len(df_Events)*(n/10))
        df_Job = df_Job.head(n)
        df =  pd.concat([df_Job,df_Internship])
        df =  pd.concat([df,df_Events])
    if "Bachelor's (Integrated Master's)" in Degree:
      if  "4" in Year:
        n = 2
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 2
        n = round(len(df_Events)*(n/10))
        df_Internship = df_Internship.head(n)
        n = 6
        n = round(len(df_Events)*(n/10))
        df_Job = df_Job.head(n)
        df =  pd.concat([df_Job,df_Internship])
        df =  pd.concat([df,df_Events])
    
    else:
        n = 2
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 3
        n = round(len(df_Events)*(n/10))
        df_Internship = df_Internship.head(n)
        n = 5 
        n = round(len(df_Events)*(n/10))
        df_Job = df_Job.head(n)
        df =  pd.concat([df_Job,df_Internship])
        df =  pd.concat([df,df_Events])
        df_touchpoints = pd.read_sql('select * from touchpoints', con=engine)
        df =  pd.merge(df_touchpoints, df, left_on='id',right_on='id',suffixes=('', '_x'),how = 'inner')
        df = df.loc[:,~df.columns.duplicated()]
        st.write(df)
