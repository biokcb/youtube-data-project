#!/usr/bin/env python
""" Script to process YouTube data """

import os
import time
import sys
import json
import pandas as pd
import numpy as np
import fastText

def get_metadata(infile):
    """ Opens a video JSON and returns relevant metadata """
    keepers = ['id', 'uploader', 'uploader_id', 'uploader_url',
               'channel_id', 'channel_url', 'upload_date', 
               'title', 'thumbnail', 'description', 'categories',
               'tags', 'duration', 'age_limit', 'view_count', 
               'like_count', 'dislike_count', 'average_rating']

    with open(infile) as mtda:
        metadata = json.load(mtda)
 
    metadata = pd.DataFrame.from_dict(metadata, orient='index')
    clean_metadata = metadata.loc[keepers]

    return clean_metadata

def get_videos(infile):
    """ Opens a file with a a list of YT video IDs"""
    
    # Get all the video metadata first 
    count = 0
    with open('youtube-dl-archive.txt') as ytdl:
        for line in ytdl:
            clean_meta = get_metadata('metadata/' + line.strip() + '.info.json')

            if count == 0:
                channels = pd.DataFram.from_dict({'channel_id': channelid, 
                                                  'videos': videoid}, orient='index').transpose()
            else:
                tempdf = pd.DataFrame.from_dict({'channel_id': channelid, 
                                             'videos': videoid}, orient='index').transpose()
                channels = channels.append(tempdf)
            count +=1

def get_comments(infile):
    """Returns a table of comment info"""

    count = 0
    lines = 0
    com_total = 0
    with open(infile) as ytdl:
        for vid in ytdl:
            print(vid)
            vid = vid.strip()
            with open('comments/'+vid+'\n_comments.json') as cm:
                for com in cm:
                    singlecom = pd.DataFrame.from_dict(json.loads(com), orient='index').transpose()
                    singlecom['video_id'] = vid
                    singlecom['desc'] = '-'
                    singlecom['category'] = '-'
                    if lines == 0:
                        comments = singlecom
                        lines +=1
                    elif lines % 500 == 0:
                        # Shuffle comments randomly before writing
                        comments = comments.sample(frac=1).reset_index(drop=True)
                        comments.to_csv('comments_set_' + str(count) + '.csv')
                        count += 1
                        lines = 0
                    else:
                        comments = comments.append(singlecom)
                        lines += 1
                    
                    com_total += 1
                    sys.stdout.write('Processed %d comments\r' % com_total)
                    time.sleep(0.1)
                    sys.stdout.flush()
        else:
            # Write whatever is left to a file
            comments.to_csv('comments_set_' + count + '.csv')

def pred_lang(text, model):
    """ Predict most likely language used"""
    
    return model.predict(text)[0][0].replace('__label__', '')

def filter_lang(files, lang, model, directory):
    """ Filter by predicted language """
    
    for f in files:
        print(f)
        try:
            comments = pd.read_csv(directory+f, index_col=0)
        except pd.errors.ParserError:
            comments = pd.read_csv(directory+f, index_col=0, lineterminator='\n')
        comments['text'] = comments['text'].apply(lambda x: str(x).replace('\n',' '))
        comments['lang'] = comments['text'].apply(lambda x: pred_lang(x, model))
        comments = comments[comments['lang'] == lang]
        comments.to_csv(directory+'filtered/filtered_'+f)

def main():
    """main routine"""
    # Get the comments and turn them into tables
    #get_comments('youtube-dl-archive.txt')

    # Filter non-english seeming comments
    lang_mod = fastText.load_model('lid.176.bin')
    directory = '/Users/kristenbrown/Documents/GitHub/youtube-data/label-app/data/'
    filter_lang(os.listdir('/Users/kristenbrown/Documents/GitHub/youtube-data/label-app/data/'), 'en', lang_mod, directory)
    
    # Upload comments to SQL DB

if __name__ == '__main__':
    main()


