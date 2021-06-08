import requests
from flask import Flask, render_template, request, jsonify
from flask_sqlalchemy import SQLAlchemy
import json
import os
import operator
import re
import nltk
#from stop_words import stops
from collections import Counter
from bs4 import BeautifulSoup
#from rq import Queue
#from rq.job import Job
#from worker import conn
import psql



from flask import Response
from flask import flash
from flask import redirect
from flask import url_for
import requests
import sys

######################################################
#
# App instance
#
######################################################

app = Flask(__name__)


######################################################
#
# Routes
#
######################################################

@app.route('/')
def home():
    return render_template('index.html')


@app.route('/scrape')
def scrape():
    # flash(request.args.get('url'), 'success')
    url = request.args.get('url')

    try:
        response = requests.get(url)
        content = BeautifulSoup(response.text, 'lxml').prettify()

    except Exception as e:
        flash('Failed to retrieve URL because: ' + str(e))
        content = ''

    return render_template('scrape.html', content=content)


# render results to screen
@app.route('/results')
def results():
    args = []
    results = []

    for index in range(0, len(request.args.getlist('tag'))):
        args.append({
            'tag': request.args.getlist('tag')[index],
            'css': request.args.getlist('css')[index],
            'attr': request.args.getlist('attr')[index],
        })

    response = requests.get(request.args.get('url'))
    content = BeautifulSoup(response.text, 'lxml')

    # item to store scraped results
    item = {}

    # loop over request arguments
    for arg in args:
        # store item
        item[arg['css']] = [one.text for one in content.findAll(arg['tag'], arg['css'])]

    # loop over row indexes

    for index in range(0, len(item[next(iter(item))])):
        row = {}

        # loop over stored data
        for key, value in item.items():
            # append value to the new row
            row[key] = '"' + value[index] + '"'

        # append new row to results list
        results.append(row)

    # get url from get params
    request_url = request.args.get('url').replace("https://","").replace("http://","").replace("/","")
    # store data scraped to database
    psql.storeDataToDB(results, request_url)
    return render_template('results.html', results=results)

# app = Flask(__name__)
app.config.from_object(os.environ['FUN_MAKER_SETTINGS'])
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

q = Queue(connection=conn)


######################################################
#
# Run app
#
######################################################

if __name__ == '__main__':
    app.secret_key = 'secret123'
    app.run(debug=True, threaded=True)













