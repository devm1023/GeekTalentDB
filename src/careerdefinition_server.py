from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import flask_admin as admin
from flask_admin.contrib import sqla
from flask import request, Response, jsonify
from werkzeug.exceptions import HTTPException

import conf
from careerdefinitiondb import CareerDefinitionDB
from datetime import datetime
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--debug', action='store_true',
                    help='Enable debug mode.')
args = parser.parse_args()



# Create application
app = Flask(__name__)

# Create database session
cddb = CareerDefinitionDB(conf.CAREERDEFINITION_DB)


# Create dummy secrey key so we can use sessions
app.config['SECRET_KEY'] = '123456790'

# Configure app and create database view
app.config['SQLALCHEMY_DATABASE_URI'] = conf.CAREERDEFINITION_DB
app.config['SQLALCHEMY_ECHO'] = True
app.config['ADMIN_CREDENTIALS'] = ('geektalent', 'PythonRulez')
db = SQLAlchemy(app)

STR_MAX = 100000

# Flask views
@app.route('/')
def index():
    return '<a href="/admin/">Click me to get to Admin!</a>'

@app.route('/careers/', methods=['GET'])
def get_careers():
    start = datetime.now()
    sectors = request.args.getlist('sector')
    titles = request.args.getlist('title')
    results = cddb.getCareers(sectors, titles)
    end = datetime.now()
    response = jsonify({'results' : results,
                        'query_time' : (end-start).microseconds//1000,
                        'status' : 'OK'})
    print('response sent [{0:s}]' \
          .format(datetime.now().strftime('%d/%b/%Y %H:%M:%S')))
    return response

class Career(db.Model):
    __tablename__ = 'career'
    id            = db.Column(db.BigInteger, primary_key=True)
    title         = db.Column(db.Unicode(STR_MAX), nullable=False)
    linkedinSector = db.Column(db.Unicode(STR_MAX), nullable=False)
    description   = db.Column(db.Text)
    count         = db.Column(db.BigInteger)
    relevanceScore = db.Column(db.Float)

    skillCloud = db.relationship('CareerSkill', backref='career',
                                 cascade='all, delete-orphan')

    def __str__(self):
        return self.title

class CareerSkill(db.Model):
    __tablename__ = 'career_skill'
    id            = db.Column(db.BigInteger, primary_key=True)
    careerId      = db.Column(db.BigInteger,
                              db.ForeignKey('career.id',
                                            onupdate='CASCADE',
                                            ondelete='CASCADE'))
    skillName     = db.Column(db.Unicode(STR_MAX), nullable=False)
    description   = db.Column(db.Text)
    count         = db.Column(db.BigInteger)
    relevanceScore = db.Column(db.Float)

    def __str__(self):
        return self.skillName


class ModelView(sqla.ModelView):
    def is_accessible(self):
        auth = request.authorization or \
               request.environ.get('REMOTE_USER')  # workaround for APACHE
        if not auth or \
           (auth.username, auth.password) != app.config['ADMIN_CREDENTIALS']:
            raise HTTPException('', Response(
                "Please log in.", 401,
                {'WWW-Authenticate': 'Basic realm="Login Required"'}
            ))
        return True
    
_textAreaStyle = {
    'rows' : 5,
    'style' : 'font-family:"Lucida Console", Monaco, monospace;'
}
class CareerView(ModelView):
    form_widget_args = {
        'description' : _textAreaStyle,
        'count' : {'readonly' : True},
        'relevanceScore' : {'readonly' : True},
    }
    inline_models = [(CareerSkill,
                      {'form_widget_args' : {
                          'relevanceScore' : {'readonly' : True},
                          'count' : {'readonly' : True},
                          'description' : _textAreaStyle
                      }})]
    column_filters = ['linkedinSector', 'title']

class CareerSkillView(ModelView):
    column_filters = ['career', 'skillName']
    
    
# Create admin
admin = admin.Admin(app, name='CareerDefinitionDB', template_mode='bootstrap3')
admin.add_view(CareerView(Career, db.session))
admin.add_view(CareerSkillView(CareerSkill, db.session))

if __name__ == '__main__':
    if args.debug:
        app.run(debug=True)
    else:
        app.run(host='0.0.0.0')

