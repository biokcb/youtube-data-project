import argparse
import requests
import ast
import pandas as pd
import numpy as np
import sqlalchemy
from flask import Flask, render_template, request
from sqlalchemy import create_engine, MetaData, Table, and_
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.sql.expression import cast
import psycopg2
from nltk.corpus import stopwords

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', '-d', help='Name of database to load', required=True)
    parser.add_argument('--table', '-t', help='Table of video information', required=True)
    parser.add_argument('--user', '-u', help='Username for database connection', required=True)

    return parser.parse_args()

## Functions required to run app
def load_db(dbname, username):
    """ Creates a connection to my PostgreSQL database """
    engine = create_engine('postgres://%s@localhost/%s'%(username,dbname))

    if not database_exists(engine.url):
        create_database(engine.url)

    connection = engine.connect()
    
    return engine, connection

def get_meta_params(meta):
    """ Parse the metadata selections for querying """

    duration = meta.get('duration', ['any'])
    views = meta.get('views', ['any'])
    upload = meta.get('upload', ['any'])

    meta_new = {}

    # Figure out the max duration requested
    if len(duration) == 3 or 'any' in duration:
        # all options selected or non
        meta_new['duration'] = (0, 4950)
    # two options selected as a range
    elif set(duration).intersection(set(['short','long'])) == set(['short','long']):
        meta_new['duration'] = (0, 4950)
    elif set(duration).intersection(set(['medium','long'])) == set(['medium','long']):
        meta_new['duration'] = (600, 4950)
    elif set(duration).intersection(set(['medium','short'])) == set(['medium','short']):
        meta_new['duration'] = (0, 600)
    # only one option selected
    elif 'short' in duration:
        meta_new['duration'] = (0, 60)
    elif 'medium' in duration:
        meta_new['duration'] = (60, 600)
    elif 'long' in duration:
        meta_new['duration'] = (600, 4950)
    else:
        meta_new['duration'] = (0, 4950)

    # Figure out max views requested
    if len(views) == 3 or 'any' in views:
        # all options selected or none
        meta_new['views'] = (0, 100000000)
    # two options selected as a range
    elif set(views).intersection(set(['popular','undiscovered'])) == set(['popular','undiscovered']):
        meta_new['views'] = (0, 100000000)
    elif set(views).intersection(set(['average','undiscovered'])) == set(['average','undiscovered']):
        meta_new['views'] = (0, 500000)
    elif set(views).intersection(set(['popular','average'])) == set(['popular','average']):
        meta_new['views'] = (500000, 100000000)
    # only one option selected
    elif 'popular' in views:
        meta_new['views'] = (1000000, 100000000)
    elif 'average' in views:
        meta_new['views'] = (100000, 1000000)
    elif 'undiscovered' in views:
        meta_new['views'] = (0, 100000)
    else:
        meta_new['views'] = (0, 100000000)

    # Figure out the upload time frame requested
    if len(upload) == 2 or 'any' in upload:
        meta_new['upload'] = (20140101, 20191231)
    elif 'recent' in upload:
        meta_new['upload'] = (20180608, 20190608)
    else:
        meta_new['upload'] = (20140101, 20191231)

    return meta_new

def get_top_videos(mood, engine, table, meta):
    """ Get the top ranking videos for a particular mood """
    
    # get selection parameters
    meta_new = get_meta_params(meta)
    min_dur = int(meta_new['duration'][0])
    max_dur = int(meta_new['duration'][1])
    min_vie = int(meta_new['views'][0])
    max_vie = int(meta_new['views'][1])
    min_upl = int(meta_new['upload'][0])
    max_upl = int(meta_new['upload'][1])

    # create a dataframe pulling all comment info from database for a video
    videos = sqlalchemy.Table(table, sqlalchemy.MetaData(), autoload=True, autoload_with=engine)
    meta_filter = [cast(videos.columns.duration,sqlalchemy.Integer) > min_dur, 
                   cast(videos.columns.duration,sqlalchemy.Integer) < max_dur, 
                   cast(videos.columns.view_count,sqlalchemy.Integer) > min_vie, 
                   cast(videos.columns.view_count,sqlalchemy.Integer) < max_vie, 
                   cast(videos.columns.upload_date,sqlalchemy.Integer) > min_upl, 
                   cast(videos.columns.upload_date,sqlalchemy.Integer) < max_upl]

    if mood == 'excited':
        videos_query = sqlalchemy.select([videos]).where(and_(*meta_filter)).order_by(videos.columns.excited_score.desc()).limit(30)
    elif mood == 'relaxed':
        videos_query = sqlalchemy.select([videos]).where(and_(*meta_filter)).order_by(videos.columns.calm_score.desc()).limit(30)
    elif mood == 'joking':
        videos_query = sqlalchemy.select([videos]).where(and_(*meta_filter)).order_by(videos.columns.joke_score.desc()).limit(30)
    elif mood == 'annoyed':
        videos_query = sqlalchemy.select([videos]).where(and_(*meta_filter)).order_by(videos.columns.annoyed_score.desc()).limit(30)
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
args = get_args()
engine, connection = load_db(args.database, args.user)
vid_table = args.table

## Render templates

@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')

@app.route('/recommended_tags', methods=['GET', 'POST'])
def recommended_tags(engine=engine):
    meta = {}

    if 'timelist' in request.form.keys():
        meta['duration'] = request.form['timelist']
    if 'viewlist' in request.form.keys():
        meta['views'] = request.form['viewlist']
    if 'uploadlist' in request.form.keys():
        meta['upload'] = request.form['uploadlist']
    if 'action' in request.form.keys():
        mood = request.form['action'].lower()

        df = get_top_videos(mood, engine, vid_table, meta)
        tags = get_top_tags(df)

    return render_template('recommended_tags.html', tags=tags,
                           mood=mood, meta=meta) 

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
    if 'meta' in request.form.keys():
        meta = ast.literal_eval(request.form['meta'])
    else:
        raise Exception('Metadata not selected')

    df = get_top_videos(mood, engine, vid_table, meta)
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
