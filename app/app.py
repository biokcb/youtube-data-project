from flask import Flask, render_template, request
import requests
import pandas as pd
import numpy as np


#Initialize app
app = Flask(__name__, static_url_path='/static')

@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')

@app.route('/recommendations', methods=['GET', 'POST'])
def recommendations():
    
    if request.form['action'] == 'Relax':
        channel_type = 'calm'
    
    elif request.form['action'] == 'Laugh':
        channel_type = 'funny'

    elif request.form['action'] == 'Something else':
        channel_type = 'upset'

    channels = pd.read_csv('data/channels.csv')
    channel_select = channels.loc[channels['label'] == channel_type]

    channel1 = channel_select.iloc[0]['uploader']
    channel2 = channel_select.iloc[1]['uploader']
    channel3 = channel_select.iloc[2]['uploader']
    
    link1 = channel_select.iloc[0]['uploader_url']
    link2 = channel_select.iloc[1]['uploader_url']
    link3 = channel_select.iloc[2]['uploader_url']

    desc1 = channel_select.iloc[0]['webpage_url']
    desc2 = channel_select.iloc[1]['webpage_url']
    desc3 = channel_select.iloc[2]['webpage_url']

    thumb1 = channel_select.iloc[0]['thumbnail']
    thumb2 = channel_select.iloc[1]['thumbnail']
    thumb3 = channel_select.iloc[2]['thumbnail']

    title1 = channel_select.iloc[0]['title']
    title2 = channel_select.iloc[1]['title']
    title3 = channel_select.iloc[2]['title']


    return render_template('recommendations.html', 
                            channel1 = channel1, channel2 = channel2, channel3 = channel3,
                            link1 = link1, link2 = link2, link3 = link3,
                            thumb1 = thumb1, thumb2=thumb2, thumb3=thumb3,
                            title1=title1, title2=title2, title3=title3,
                            desc1 = desc1, desc2=desc2, desc3=desc3)

if __name__ == '__main__':
    #this runs your app locally
    app.run(host='0.0.0.0', port=8080, debug=True)
