import streamlit as st
import pandas as pd
import psycopg2 as pg
import numpy as np

engine = pg.connect("dbname='huzzle_production' user='postgres' host='huzzle-production-db-read.ct4mk1ahmp9p.eu-central-1.rds.amazonaws.com' port='5432' password='S11mXHLGbA0Cb8z8uLfj'")
df_goals = pd.read_sql('select * from goals', con=engine)
df_tags = pd.read_sql('select * from tags', con=engine)
group_0 = df_tags.groupby(df_tags.type)
df_tag = group_0.get_group('Topic')
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
year = ['Select a Year','First Year','Second Year','Third Year','Final Year']


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
  df_touchpoints =  pd.merge(df_touchpoints, df_tagging, left_on='id',right_on='taggable_id',suffixes=('', '_x'))
  df_touchpoints = df_touchpoints.loc[:,~df_touchpoints.columns.duplicated()]
  df_touchpoints = pd.merge(df_touchpoints,df_tags,left_on='tag_id',right_on='id',suffixes=('', '_x'))
  df_touchpoints = df_touchpoints.loc[:,~df_touchpoints.columns.duplicated()]
  df_touchpoints = pd.merge(df_touchpoints,df_tc,left_on='id',right_on='touchpoint_id',suffixes=('', '_x'),how = 'inner')
  df_touchpoints = df_touchpoints.loc[:,~df_touchpoints.columns.duplicated()]
  df_touchpoints = pd.merge(df_touchpoints,df_cities,left_on='city_id',right_on='id',suffixes=('', '_x'),how = 'inner')
  df_touchpoints = df_touchpoints.loc[:,~df_touchpoints.columns.duplicated()]
  df_touchpoints = df_touchpoints[['id','touchpointable_id','type','touchpointable_type','kind','title','name','creatable_for_name','city_name']].copy()
  df_touchpoints.replace("Bachelors","Bachelor's", inplace=True)
  df_touchpoints.replace("Masters","Master's", inplace=True)
  df_touchpoints.replace("First Year","1",inplace = True)
  df_touchpoints.replace("Second Year","2",inplace = True)
  df_touchpoints.replace("Third Year","3",inplace = True)
  df_touchpoints.replace("Fourth Year","4",inplace = True)  
  
  df_goals = pd.read_sql('select * from goals', con=engine)
  df_matching_goal_weights = pd.read_sql('select * from matching_goal_weights', con=engine)
  df_goals_weights = pd.merge(df_goals, df_matching_goal_weights, left_on='id',right_on='goal_id',suffixes=('', '_x'),how = 'inner')
  df_goals_weights = df_goals_weights.loc[:,~df_goals_weights.columns.duplicated()]
  
  
  
  df_universities = pd.read_sql('select * from universities', con=engine)
      

  df_degrees = pd.read_sql('select * from degrees', con=engine)
  
  df_subjects = pd.read_sql('select * from subjects', con=engine)
  

  year = ['1','2','3','4']


  if len(Goals) > 0:
    goals_1 =  pd.DataFrame(Goals,columns =['Goals'])
    df_goals = pd.merge(df_goals_weights, goals_1, left_on='title',right_on='Goals',suffixes=('', '_x'),how = 'inner')
    df_goals = df_goals.loc[:,~df_goals.columns.duplicated()]
    df_goals_1 =  pd.merge(df_touchpoints, df_goals, left_on='kind',right_on='touchpointable_kind',suffixes=('', '_x'),how = 'inner')
    df_goals_1 = df_goals_1.loc[:,~df_goals_1.columns.duplicated()]
    df_goals_1 = df_goals_1.groupby('id', as_index=False).first()
    df_touchpoints =  pd.merge(df_touchpoints, df_goals_1, left_on='id',right_on='id',suffixes=('', '_x'),how = 'inner')
    df_touchpoints = df_touchpoints.loc[:,~df_touchpoints.columns.duplicated()]
    df_touchpoints = df_touchpoints[['id','touchpointable_id','type','touchpointable_type','kind','title','name','creatable_for_name','city_name','value']].copy()
    
    
  else:
    df_touchpoints = df_touchpoints[['id','touchpointable_id','type','touchpointable_type','kind','title','name','creatable_for_name','city_name']].copy()
    df_touchpoints['value'] = 0
  
  if len(Interest) > 0:

    interest = pd.DataFrame(Interest,columns = ['Interest'])
    Weight = pd.DataFrame(weight,columns = ['Weight'])
    df_interest = pd.concat([interest,Weight],axis = 1)
    
    group_2 = df_touchpoints.groupby(df_touchpoints.type)
    df_T = group_2.get_group('Topic')

    df_I =  pd.merge(df_T, df_interest, left_on='name',right_on='Interest',suffixes=('', '_x'),how = 'inner')
    df_I = df_I.loc[:,~df_I.columns.duplicated()]
    #df_I = df_I.groupby('id', as_index=False).first()
    
    
    col_list = df_I['name'].unique()
    df_I['idx'] = df_I.groupby(['touchpointable_id', 'name']).cumcount()
    df_I = df_I.pivot(index=['idx','touchpointable_id'], columns='name', values='Weight').sort_index(level=1).reset_index().rename_axis(None, axis=1)
    df_I = df_I.fillna(0)
    df_I['Weight'] = df_I[col_list].sum(axis=1)
    df_I = df_I.groupby('touchpointable_id', as_index=False).first()
    df_touchpoints = pd.merge(df_touchpoints, df_I, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
    df_touchpoints = df_touchpoints.loc[:,~df_touchpoints.columns.duplicated()]
  else:
    df_touchpoints['Weight'] = 0
  
  if University in df_universities['name'].unique():
    
    df_universities_1 = pd.merge(df_universities, df_cities, left_on='city_id',right_on='id',suffixes=('', '_x'),how = 'left')
    df_universities_1 = df_universities_1.loc[:,~df_universities_1.columns.duplicated()]
    
    df_universities_1 = df_universities_1.loc[df_universities_1['name'] == University]
    
    city_name = df_universities_1.iloc[0]['city_name']
    
    df_touchpoints['city score'] = np.where(df_touchpoints['city_name'] == city_name, 1,0)
    
  else:
    df_touchpoints['city score'] = 0
  
  if Degree in df_degrees['name'].unique():
    df_E = df_touchpoints.loc[df_touchpoints['type'] == 'EducationRequirement']
    id = df_E['id'].to_list()
    df_E = df_touchpoints[~df_touchpoints.id.isin(id)]
    
    df_O = df_touchpoints[df_touchpoints['name'] == 'Open to All Students']
    df_O = df_O.groupby('id', as_index=False).first()
    df_O = pd.merge(df_touchpoints, df_O, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
    df_O = df_O.loc[:,~df_O.columns.duplicated()]
    
    df_D = df_touchpoints[df_touchpoints['name'] == Degree]
    df_D['degree score'] = 1
    df_D = df_D.groupby('id', as_index=False).first()
    df_D = pd.merge(df_touchpoints, df_D, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
    df_D = df_D.loc[:,~df_D.columns.duplicated()]
    
    df_DO = pd.concat([df_D,df_O])
    df_touchpoints = pd.concat([df_DO,df_E])
    df_touchpoints = df_touchpoints[['id','touchpointable_id','type','touchpointable_type','kind','title','name','creatable_for_name','Weight','city_name','city score','degree score','value']].copy()
    
  
  else:
    df_touchpoints['degree score'] = 0
  
  if Subject in df_subjects['name'].unique():
    subject_topics = pd.read_sql('select * from subjects_topics', con=engine)
    df_subjects_1 = pd.merge(df_subjects, subject_topics, left_on='id',right_on='subject_id',suffixes=('', '_x'),how = 'left')
    df_subjects_1 = df_subjects_1.loc[:,~df_subjects_1.columns.duplicated()]
    df_subjects_1 = pd.merge(df_subjects_1,df_tags,left_on='topic_id',right_on='id',suffixes=('', '_x'),how = 'left')
    df_subjects_1 = df_subjects_1.loc[:,~df_subjects_1.columns.duplicated()]
    df_subjects_1 = df_subjects_1.loc[df_subjects_1['name'] == Subject]
    df_subjects_1['subject score'] = 0.5
    group_3 = df_touchpoints.groupby(df_touchpoints.type)
    df_T = group_3.get_group('Topic')
    df_S = pd.merge(df_T,df_subjects_1, left_on='name',right_on='name_x',suffixes=('', '_x'),how = 'inner')
    df_S = df_S.loc[:,~df_S.columns.duplicated()]
    df_S =  df_S.groupby('id', as_index=False).first()
    df_S = pd.merge(df_touchpoints, df_S, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
    df_S = df_S.loc[:,~df_S.columns.duplicated()]
    id = df_S['id'].to_list()
    df_touchpoints = df_touchpoints[~df_touchpoints.id.isin(id)]
    df_touchpoints = pd.concat([df_touchpoints,df_S])
    df_touchpoints = df_touchpoints[['id','touchpointable_id','type','touchpointable_type','kind','title','name','creatable_for_name','Weight','city_name','city score','degree score','subject score','value']].copy()
    
  else:
    df_touchpoints['subject score'] = 0
  
  if Year in year:
    df_Y = df_touchpoints.loc[df_touchpoints['name'] == Year]
    df_Y['year score'] = 1
    df_Y = df_Y.groupby('id', as_index=False).first()
    df_Y = pd.merge(df_touchpoints, df_Y, left_on='touchpointable_id',right_on='touchpointable_id',suffixes=('', '_x'),how = 'inner')
    df_Y = df_Y.loc[:,~df_Y.columns.duplicated()]
    id = df_Y['id'].to_list()
    df_touchpoints = df_touchpoints[~df_touchpoints.id.isin(id)]
    df_touchpoints =  pd.concat([df_Y,df_touchpoints])
  else:
    df_touchpoints['year score'] = 0
  
  df = df_touchpoints[['id','touchpointable_id','type','touchpointable_type','kind','title','name','creatable_for_name','Weight','city_name','city score','degree score','subject score','year score','value']].copy()
  col_list = ['Weight','city score','degree score','subject score','year score']
  #group_4 = df.groupby(df.type)
  #df_T = group_4.get_group('Topic')
  #df = df_T.set_index(['id', df.groupby('id').cumcount()])['name'].unstack().add_prefix('name').reset_index()
  
  
  df['matching score'] = df[col_list].sum(axis=1)
  #df = df.groupby(['id','touchpointable_id','type','touchpointable_type','kind','title','name','creatable_for_name','Weight','city_name','city score','degree score','subject score','year score','value']).sum()
 
  df['idx'] = df.groupby(['id', 'name']).cumcount()
  df_name = df.pivot_table(index=['idx','id'], columns='type', values='name').sort_index(level=1).reset_index().rename_axis(None, axis=1)
  df_name = pd.DataFrame(df_name.unstack()).transpose()
  df = pd.merge(df, df_name, left_on='id',right_on='id',suffixes=('', '_x'),how = 'left')
  df = df.loc[:,~df.columns.duplicated()]
  #df = df.drop(['name'],axis = 1)
  df = df.groupby('id', as_index=False).first()
  
  #df = df.loc[:,~df.columns.duplicated()]
  df = df.sort_values(by='matching score',ascending=False)
  return df_name
  
  
Goals =  st.multiselect('Enter the goals',df_goals['title'].unique(),key = "one")
Interest = st.multiselect('Enter the interest',df_tag['name'].unique(),key = "two")
weight = [1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,2,1,1]
weight = st.multiselect('Enter the weight',weight,key = "three")
University = st.selectbox('Enter the university',universities,key = 'four')
Degree =  st.selectbox('Enter the degree',degree,key = 'six')
Subject = st.selectbox('Enter the subject',subject,key = 'five')
Year = st.selectbox('Enter the year',year,key = 'seven')

if st.button("Submit",key = "eight"):
  
  
  df = matching_algo(Goals,Interest,weight,University,Degree,Subject,Year)
  if len(df['value'].unique()) > 1:
   kind = df.groupby("kind")
   for group,df_1 in kind:
    df_1 = pd.DataFrame(df_1)
    n = df_1['value'].iloc[0]
    n = round(len(df_1)*(n/10))
    df = df_1.head(n)
    id = df['touchpointable_id'].to_list()
    st.write(df)
    
  if len(df['value'].unique()) == 1:
    group_0 = df.groupby(df.touchpointable_type)
    df_Events = group_0.get_group("Event")
    group_1 = df.groupby(df.touchpointable_type)
    df_Internship = group_1.get_group("Internship")
    group_2 = df.groupby(df.touchpointable_type)
    df_Job = group_2.get_group("Job")
    if 'Foundation' in Degree:
      if 'First Year' in Year:
        n = 7
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 3
        n = round(len(df_Internship)*(n/10))
        df_Internship = df_Internship.head(n)
        #df =  pd.concat([df_Events,df_Internship])
    if "Bachelor's" in Degree:
      if  "First Year" in Year:
        n = 4
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 6
        n = round(len(df_Internship)*(n/10))
        df_Internship = df_Internship.head(n)
        #df =  pd.concat([df_Events,df_Internship])
    if "Bachelor's" in Degree:
      if  "Second Year" in Year:
        n = 3
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        
        n = 6
        n = round(len(df_Internship)*(n/10))
        df_Internship = df_Internship.head(n)
        
        n = 1
        n = round(len(df_Job)*(n/10))
        df_Job = df_Job.head(n)
        
        #df =  pd.concat([df_Events,df_Internship])
        #df =  pd.concat([df,df_Job])
       
    if "Bachelor's" in Degree:
      if  "Final Year" in Year:
        n = 2
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        
        n = 2
        n = round(len(df_Internship)*(n/10))
        df_Internship = df_Internship.head(n)
        
        n = 6
        n = round(len(df_Job)*(n/10))
        df_Job = df_Job.head(n)
        #df =  pd.concat([df_Job,df_Internship])
        #df =  pd.concat([df,df_Events])
    if "Bachelor's" in Degree:
      if  "Third Year" in Year:
        n = 2
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 2
        n = round(len(df_Internship)*(n/10))
        df_Internship = df_Internship.head(n)
        n = 6
        n = round(len(df_Job)*(n/10))
        df_Job = df_Job.head(n)
        #df =  pd.concat([df_Job,df_Internship])
        #df =  pd.concat([df,df_Events])
    if "Bachelor's (Integrated Master's)" in Degree:
      if  "First" in Year:
        n = 5
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 5
        n = round(len(df_Internship)*(n/10))
        df_Internship = df_Internship.head(n)
        #df =  pd.concat([df_Events,df_Internship])
    if "Bachelor's (Integrated Master's)" in Degree:
      if  "Second Year" in Year:
        n = 4
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 6
        n = round(len(df_Internship)*(n/10))
        df_Internship = df_Internship.head(n)
        #df =  pd.concat([df_Events,df_Internship])

    if "Bachelor's (Integrated Master's)" in Degree:
      if  "Third Year" in Year:
        n = 2
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 4
        n = round(len(df_Internship)*(n/10))
        df_Internship = df_Internship.head(n)
        n = 4
        n = round(len(df_Job)*(n/10))
        df_Job = df_Job.head(n)
        #df =  pd.concat([df_Job,df_Internship])
        #df =  pd.concat([df,df_Events])
    if "Bachelor's (Integrated Master's)" in Degree:
      if  "Final Year" in Year:
        n = 2
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 2
        n = round(len(df_Internship)*(n/10))
        df_Internship = df_Internship.head(n)
        n = 6
        n = round(len(df_Job)*(n/10))
        df_Job = df_Job.head(n)
        #df =  pd.concat([df_Job,df_Internship])
        #df =  pd.concat([df,df_Events])
    
    else:
        n = 2
        n = round(len(df_Events)*(n/10))
        df_Events = df_Events.head(n)
        n = 3
        n = round(len(df_Internship)*(n/10))
        df_Internship = df_Internship.head(n)
        n = 5 
        n = round(len(df_Job)*(n/10))
        df_Job = df_Job.head(n)
        #df =  pd.concat([df_Job,df_Internship])
        #df =  pd.concat([df,df_Events])
        #id = df['touchpointable_id'].to_list()
    st.write(df_Events)
    st.write(df_Internship)
    st.write(df_Job)
