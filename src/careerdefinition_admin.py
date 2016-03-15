from flask import Flask
from flask_sqlalchemy import SQLAlchemy

import flask_admin as admin
from flask_admin.contrib import sqla
from flask import request, Response
from werkzeug.exceptions import HTTPException


import conf

# Create application
app = Flask(__name__)


# Create dummy secrey key so we can use sessions
app.config['SECRET_KEY'] = '123456790'

# Create in-memory database
app.config['SQLALCHEMY_DATABASE_URI'] = conf.CAREERDEFINITION_DB
app.config['SQLALCHEMY_ECHO'] = True
app.config['ADMIN_CREDENTIALS'] = ('geektalent', 'PythonRulez')
db = SQLAlchemy(app)

STR_MAX = 100000

# Flask views
@app.route('/')
def index():
    return '<a href="/admin/">Click me to get to Admin!</a>'

class Career(db.Model):
    __tablename__ = 'career'
    id            = db.Column(db.BigInteger, primary_key=True)
    name          = db.Column(db.Unicode(STR_MAX), nullable=False)
    sector        = db.Column(db.Unicode(STR_MAX), nullable=False)
    description   = db.Column(db.Text)

    skills = db.relationship('CareerSkill', backref='career',
                             cascade='all, delete-orphan')

    def __str__(self):
        return self.name

class CareerSkill(db.Model):
    __tablename__ = 'career_skill'
    id            = db.Column(db.BigInteger, primary_key=True)
    careerId      = db.Column(db.BigInteger,
                              db.ForeignKey('career.id',
                                            onupdate='CASCADE',
                                            ondelete='CASCADE'))
    name          = db.Column(db.Unicode(STR_MAX), nullable=False)
    description   = db.Column(db.Text)
    score         = db.Column(db.Float)

    def __str__(self):
        return self.name


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
    form_widget_args = {'description' : _textAreaStyle}
    inline_models = [(CareerSkill,
                      {'form_widget_args' : {
                          'score' : {'readonly' : True},
                          'description' : _textAreaStyle
                      }})]
    column_filters = ['sector', 'name']

class CareerSkillView(ModelView):
    column_filters = ['career', 'name']
    
    
# Create admin
admin = admin.Admin(app, name='CareerDefinitionDB', template_mode='bootstrap3')
admin.add_view(CareerView(Career, db.session))
admin.add_view(CareerSkillView(CareerSkill, db.session))

if __name__ == '__main__':

    # Start app
    app.run(debug=True)

