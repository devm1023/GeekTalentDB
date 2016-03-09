from flask import Flask
from flask_sqlalchemy import SQLAlchemy

import flask_admin as admin
from flask_admin.contrib import sqla

import conf

# Create application
app = Flask(__name__)


# Create dummy secrey key so we can use sessions
app.config['SECRET_KEY'] = '123456790'

# Create in-memory database
app.config['SQLALCHEMY_DATABASE_URI'] = conf.CAREERHACKER_DB
app.config['SQLALCHEMY_ECHO'] = True
db = SQLAlchemy(app)

STR_MAX = 100000

# Flask views
@app.route('/')
def index():
    return '<a href="/admin/">Click me to get to Admin!</a>'

class Career(db.Model):
    __tablename__ = 'career'
    id            = db.Column(db.BigInteger, primary_key=True)
    name          = db.Column(db.Unicode(STR_MAX))
    sector        = db.Column(db.Unicode(STR_MAX))
    description   = db.Column(db.Text)

    def __str__(self):
        return self.name

class CareerSkill(db.Model):
    __tablename__ = 'career_skill'
    id            = db.Column(db.BigInteger, primary_key=True)
    careerId      = db.Column(db.BigInteger, db.ForeignKey('career.id'))
    name          = db.Column(db.Unicode(STR_MAX))
    score         = db.Column(db.Float)

    career        = db.relation(Career, backref='skills')

    def __str__(self):
        return self.name

class CareerView(sqla.ModelView):
    inline_models = [CareerSkill]
    
# Create admin
admin = admin.Admin(app, name='CareeHackerDB', template_mode='bootstrap3')
admin.add_view(CareerView(Career, db.session))
admin.add_view(sqla.ModelView(CareerSkill, db.session))

if __name__ == '__main__':

    # Start app
    app.run(debug=True)

