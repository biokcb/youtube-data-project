import requests
import pandas as pd
import numpy as np
import sqlalchemy
from flask import Flask, render_template, request
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy_utils import database_exists, create_database
import psycopg2
from nltk.corpus import stopwords

## Functions required to run app
def load_db(dbname, username):
    """ Creates a connection to my PostgreSQL database """
    engine = create_engine('postgres://%s@localhost/%s'%(username,dbname))

    if not database_exists(engine.url):
        create_database(engine.url)

    connection = engine.connect()
    
    return engine, connection

def get_top_videos(mood, engine, table):
    """ Get the top ranking videos for a particular mood """
    
    # create a dataframe pulling all comment info from database for a video
    videos = sqlalchemy.Table(table, sqlalchemy.MetaData(), autoload=True, autoload_with=engine)
    
    if mood == 'excited':
        videos_query = sqlalchemy.select([videos]).order_by(videos.columns.excited_score.desc()).limit(30)
    elif mood == 'relaxed':
        videos_query = sqlalchemy.select([videos]).order_by(videos.columns.calm_score.desc()).limit(30)
    elif mood == 'joking':
        videos_query = sqlalchemy.select([videos]).order_by(videos.columns.joke_score.desc()).limit(30)
    elif mood == 'annoyed':
        videos_query = sqlalchemy.select([videos]).order_by(videos.columns.annoyed_score.desc()).limit(30)
    else:
        raise ValueError("Mood requested does not exist in database")
        
    videos_df = pd.read_sql(videos_query, engine)
    
    return videos_df

def get_top_tags(df):
    """ Grab the top 15 common video tags """
    stop_words = set(stopwords.words('english') +
                     ['buzzfeed', 'tasty', 'food'])
    tags = df['top_tag'].dropna().str.lower().unique()
    tags = [x for x in tags if x not in stop_words]

    return tags[:15]

def get_final_recs(df, tags):
    """ After mood & tags are selected, return best recommendations """
    
    if tags is None or tags == []:
        df_tags = df[:3]
    elif 'anything' in tags:
        df_tags = df[:3]
    else:
        df_tags = df[df['top_tag'].isin(tags)][:3]
    
    return df_tags
    

## Initialize app
app = Flask(__name__, static_url_path='/static')
engine, connection = load_db('cheftube', 'kristenbrown')

## Render templates

@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')

@app.route('/recommended_tags', methods=['GET', 'POST'])
def recommended_tags(engine=engine):
    if 'action' in request.form.keys():
        mood = request.form['action'].lower()
        
        df = get_top_videos(mood, engine, 'videos_model_3')
        tags = get_top_tags(df)

    return render_template('recommended_tags.html', tags=tags,
                           mood=mood) 

@app.route('/recommended_videos', methods=['GET', 'POST'])
def recommended_videos():
    if 'taglist' in request.form.keys():
        tags = request.form.getlist('taglist')
    else:
        tags = None

    if 'mood' in request.form.keys():
        mood = request.form['mood']
    else:
        raise Exception('Mood not selected')

    df = get_top_videos(mood, engine, 'videos_model_3')
    df_final = get_final_recs(df, tags)

    vid_titles = df_final['title']
    vid_urls = df_final['id']
    vid_thumbs = df_final['thumbnail']
    vid_descs = df_final['description']

    vid_data = zip(vid_titles,vid_urls,vid_thumbs,vid_descs)

    return render_template('recommended_videos.html', vid_data=vid_data)

if __name__ == '__main__':
    #this runs your app locally
    app.run(host='0.0.0.0', port=8080, debug=True)
