import os
from flask import Flask, render_template, request
import requests
import pandas as pd
import numpy as np

#Initialize app
app = Flask(__name__, static_url_path='/static')

@app.route('/', methods=['GET', 'POST'])
def index():
      return render_template('index.html')

@app.route('/labeler', methods=['GET', 'POST'])
def labeler():
    if request.method == 'POST':
        infile = request.form['commentfile']
        comments = pd.read_csv(infile, index_col=0)

    try:
        if request.method == 'POST':
    
            if 'desc' in request.form.keys():
                desc = request.form.getlist('desc')
            else:
                desc = ['other']

            if 'category' in request.form.keys():
                category = request.form.getlist('category')
            else:
                category = ['other']
            
            try:
                next_item_index = int(request.form['next_item_index'])
        
                comments.iloc[next_item_index-1]['desc'] = desc
                comments.iloc[next_item_index-1]['category'] = category
            except TypeError:
                next_item_index = 0
    
            text = comments.iloc[next_item_index]['comments']
            video_id = comments.iloc[next_item_index]['videos']
        
            comments.to_csv(infile)
            
            # Thanks to mVChr for inspiration on loading new data
            # using next_item_index https://stackoverflow.com/questions/52121947
            return render_template('labeler.html',
                               infile=infile,
                               text=text,
                               next_item_index=next_item_index+1)
        
        else:
            next_item_index = 0
            text = comments.iloc[next_item_index]['comments']
            video_id = comments.iloc[next_item_index]['videos']

            comments.to_csv(infile)
            return render_template('labeler.html',
                               infile=infile,
                               text=text, 
                               next_item_index=next_item_index+1)

    except IndexError:
        comments.to_csv(infile)
        return render_template('index.html')


if __name__ == '__main__':
    #this runs your app locally
    app.run(host='0.0.0.0', port=8080, debug=True)
