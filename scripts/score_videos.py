#!/usr/bin/env python
""" A script to aggregate comment labels and score videos """

from collections import Counter
import json
import pandas as pd
import numpy as np
from sqlalchemy import Table, MetaData
import sqlalchemy

def get_metadata(filename):
    with open(filename) as mtda:
        metadata = json.load(mtda)
    
    metadata = pd.DataFrame.from_dict(metadata, orient='index')

    keepers = ['id', 'uploader', 'uploader_id', 'uploader_url', 'channel_id', 'channel_url',
          'upload_date', 'title', 'thumbnail', 'description', 'categories', 'tags',
          'duration', 'age_limit', 'view_count', 'like_count', 'dislike_count', 'average_rating']
        
    clean_metadata = metadata.loc[keepers].transpose()

    return clean_metadata

def load_db(dbname, username):
    """ Creates a connection to my SQL database """
    engine = create_engine('postgres://%s@localhost/%s'%(username,dbname))

    if not database_exists(engine.url):
        create_database(engine.url)

    connection = engine.connect()
    
    return engine, connection

def get_vid_comments(video_id, engine):
    """ Return comments for a particular video """
    m = MetaData()
    comments = Table('comments', m, autoload=True, autoload_with=engine)
    video_coms = sqlalchemy.select([comments]).where(comments.columns.video_id == video_id)
    video_coms_df = pd.read_sql(video_coms, engine)

    return video_coms_df

def sel_class(num, prob):
    """ Set num to 1 with with sufficient probability """
    if num < prob:
        return 0
    else:
        return 1

def agg_scores(df, prob):
    """ Aggregate predictions in a df """

    # sum all within each class
    annoyed = np.sum(df['annoyed'].apply(lambda x: sel_class(x,prob))
    joke = np.sum(df['joke'].apply(lambda x: sel_class(x,prob))
    calm = np.sum(df['calm'].apply(lambda x: sel_class(x,prob))
    excited = np.sum(df['excited'].apply(lambda x: sel_class(x,prob))
    all_moods = sum([annoyed, joke, calm, excited])
    scores = {'annoyed': annoyed / all_moods, 
              'joke': joke / all_moods, 
              'calm': calm / all_moods ,
              'excited': excited / all_moods}

    return scores

def add_scores(df, scores):
    """ Add aggregated scores to metadata df """
    for key, val in scores.items():
        ind = key + '_score'
        df[ind] = val

def get_top_tagword(df):
    tags = df['tags'].values[0]
    tags = tags.replace(',',' ').replace('{', '')
    tags = tags.replace('\"','').replace('}','')
    tags = tags.split()
    tag_counts = Counter(tags)

    return tag_counts.most_common(1)[0][0]

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', '-d', help='Name of database to load', required=True)
    parser.add_argument('--table', '-t', help='Table to add videos to', required=True)
    parser.add_argument('--user', '-u', help='Username for database connection', required=True)
    parser.add_argument('--comments', '-c', help='Table containing comments', required=True)
    parser.add_argument('--directory', '-f', help='Directory containing files if not current directory')

    args = parser.parse_args()

    return args

def main():
    
    args = get_args()
    engine, connection = load_db(args.database, args.user)

    if args.directory:
        directory = args.directory
    else:
        directory = './'
    
    vid_files = os.listdir(directory)

    for vid in vid_files:
        # load comments
        df = get_metadata(directory+vid.strip())
        df_coms = get_vid_comments(vid, engine)

        # predict class
        scores = agg_scores(df_coms, 0.75)
        df_final = add_scores(df, scores)

        # get top keyword in tags
        df_final['top_tag'] = get_top_tagword(df)
        
        # add to db
        df_final.to_sql(args.table, con=connection, if_exists='append', index=False)

if __name__ == '__main__':
    main()
