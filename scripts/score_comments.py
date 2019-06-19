#!/usr/bin/env python

""" Script to score comments within emotional categories"""

from __future__ import print_function, division

import pandas as pd
import numpy as np
import argparse
import json
import keras
import examples.example_helper
from deepmoji.sentence_tokenizer import SentenceTokenizer
from deepmoji import attlayer
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy_utils import database_exists, create_database

def get_comment_table(filename):
    """ Loads a comment file as a dataframe """
    df = pd.read_csv(filename, index_col=0)
    return df

def load_model(model_path):
    model = keras.models.load_model(model_path, 
                  custom_objects={'AttentionWeightedAverage': attlayer.AttentionWeightedAverage})
    
    return model 

def process_text(df, vocab):
    """ Tokenizes the text for predictions """
    try:
        texts =  [unicode(x) for x in df['text']]
    except UnicodeDecodeError:
        texts = [x.decode('utf-8') for x in df['text']]
    
    st = SentenceTokenizer(vocab, 30)
    tokenized, _, _ = st.tokenize_sentences(texts)

    return tokenized

def top_scores(array, k):
    """ Returns the top k scores and labels """
    ind = np.argpartition(array, -k)[-k:]
    return ind[np.argsort(array[ind])][::-1]

def score_text(tokenized, model, df):
    """ Predicts text labels """
    prob = model.predict(tokenized)
    
    df_prob = pd.DataFrame(prob, columns=["annoyed", "joke", "calm", "excited"])
    df_final = pd.concat([df.reset_index(), df_prob], axis=1).drop('index',1)

    return df_final

def load_db(dbname, username):
    """ Creates a connection to my SQL database """
    engine = create_engine('postgres://%s@localhost/%s'%(username,dbname))

    if not database_exists(engine.url):
        create_database(engine.url)

    connection = engine.connect()
    
    return engine, connection

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', '-m', help='Path to model for prediction', required=True)
    parser.add_argument('--vocab', '-v', help='Path to vocab file for model', required=True)
    parser.add_argument('--database', '-d', help='Name of database to load', required=True)
    parser.add_argument('--table', '-t', help='Table to add comments to', required=True)
    parser.add_argument('--user', '-u', help='Username for database connection', required=True)
    parser.add_argument('--comments', '-c', help='File of filenames with comments to score', required=True)
    parser.add_argument('--directory', '-f', help='Directory containing files if not in --comments file')

    args = parser.parse_args()

    return args

def main():
    
    args = get_args()
    model = load_model(args.model)
    engine, connection = load_db(args.database, args.user)

    with open(args.vocab, 'r') as f:
        vocab = json.load(f)

    if args.directory:
        directory = args.directory
    else:
        directory = './'
    
    with open(args.comments) as com_files:
        for com in com_files:
            # load comments
            df = get_comment_table(directory+com.strip())

            # predict class
            tokenized = process_text(df, vocab)
            df_final = score_text(tokenized, model, df)

            # add to db
            df_final.to_sql(args.table, con=connection, if_exists='append', index=False)
            
            print('Processed file: %s' % com)

    connection.close()

if __name__ == '__main__':
    main()



