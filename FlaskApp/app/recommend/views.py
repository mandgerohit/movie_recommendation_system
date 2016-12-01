import subprocess, os
from flask import render_template,session
from flask import Blueprint
from flask import request, send_file    
from flask_wtf import Form
from wtforms import StringField, SelectField, SelectMultipleField, widgets, HiddenField
from sqlalchemy import create_engine
from twython import Twython

recommend = Blueprint('recommend', __name__,
                   template_folder='templates')

class MyForm(Form):
    userid = StringField("UserId")
    year = StringField("Year")

@recommend.route('/')
def home():
    form = MyForm()
    return render_template('recommend_movies.html', form = form)

@recommend.route('/submit', methods=('GET', 'POST'))
def submit():
    userid = request.form['userid']
    year = request.form['year']
    p = subprocess.Popen(["sh", "/home/rnmandge/run.sh", userid, year], stdout=subprocess.PIPE)
    result, err = p.communicate()
    result = result.split('\n')
    return render_template('result.html', par = result)

