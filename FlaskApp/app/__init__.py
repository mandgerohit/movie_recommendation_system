from flask import Flask
from app.recommend.views import recommend


app = Flask(__name__)
app.register_blueprint(recommend, url_prefix='/recommend')

app.config.from_pyfile('config.py')

from app import views