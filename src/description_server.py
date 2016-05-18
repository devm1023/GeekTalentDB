from flask import Flask, flash
from flask_sqlalchemy import SQLAlchemy
from flask_admin import Admin
from flask_admin.contrib import sqla
from flask_admin.contrib.sqla.filters import BaseSQLAFilter
from flask_admin.babel import lazy_gettext, gettext, ngettext
from flask_admin.actions import action

from flask import request, Response, jsonify
from werkzeug.exceptions import HTTPException

import conf
from descriptiondb import DescriptionDB
from datetime import datetime

# Create application
app = Flask(__name__)

# Create database session
dscdb = DescriptionDB(conf.DESCRIPTION_DB)

# Create dummy secrey key so we can use sessions
app.config['SECRET_KEY'] = '123456790'

# Configure app and create database view
app.config['SQLALCHEMY_DATABASE_URI'] = conf.DESCRIPTION_DB
app.config['SQLALCHEMY_ECHO'] = True
app.config['ADMIN_CREDENTIALS'] = ('geektalent', 'PythonRulez')
db = SQLAlchemy(app)


STR_MAX = 100000


@app.route('/')
def index():
    return '<a href="/admin/">Click me to get to Admin!</a>'


@app.route('/api/', methods=['GET'])
def get_descriptions():
    start = datetime.now()
    tpe = request.args.get('type')
    if tpe not in ['sector', 'career', 'skill']:
        return jsonify({'message' : 'Invalid or missing `type` parameter.'})
    sector = request.args.get('sector')
    queries = request.args.getlist('q')
    results = []
    for query in queries:
        description = dscdb.get_description(tpe, sector, query)
        if description is None:
            description = {'name' : query}
        results.append(description)
    end = datetime.now()
    response = jsonify({'results' : results,
                        'query_time' : (end-start).microseconds//1000,
                        'message' : 'OK'})
    return response


class SectorDescription(db.Model):
    __tablename__ = 'sector_description'
    id            = db.Column(db.BigInteger, primary_key=True)
    name          = db.Column(db.Unicode(STR_MAX))
    short_text    = db.Column(db.Text)
    text          = db.Column(db.Text)
    url           = db.Column(db.String(STR_MAX))
    source        = db.Column(db.Unicode(STR_MAX))
    approved      = db.Column(db.String(20))

    __table_args__ = (db.UniqueConstraint('name'),)
    
    def __str__(self):
        return self.name


class CareerDescription(db.Model):
    __tablename__ = 'career_description'
    id            = db.Column(db.BigInteger, primary_key=True)
    sector        = db.Column(db.Unicode(STR_MAX))
    name          = db.Column(db.Unicode(STR_MAX))
    short_text    = db.Column(db.Text)
    text          = db.Column(db.Text)
    url           = db.Column(db.String(STR_MAX))
    source        = db.Column(db.Unicode(STR_MAX))
    approved      = db.Column(db.String(20))

    __table_args__ = (db.UniqueConstraint('sector', 'name'),)
    
    def __str__(self):
        sectorstr = self.linkedin_sector if self.linkedin_sector else '*'
        return '[{0:s}|{1:s}]'.format(sectorstr, self.name)


class SkillDescription(db.Model):
    __tablename__ = 'skill_description'
    id            = db.Column(db.BigInteger, primary_key=True)
    sector        = db.Column(db.Unicode(STR_MAX))
    name          = db.Column(db.Unicode(STR_MAX))
    short_text    = db.Column(db.Text)
    text          = db.Column(db.Text)
    url           = db.Column(db.String(STR_MAX))
    source        = db.Column(db.Unicode(STR_MAX))
    approved      = db.Column(db.String(20))

    __table_args__ = (db.UniqueConstraint('sector', 'name'),)

    def __str__(self):
        sectorstr = self.linkedin_sector if self.linkedin_sector else '*'
        return '[{0:s}|{1:s}]'.format(sectorstr, self.name)
    

def _approve(self, initials, ids):
    try:
        query = self.model.query.filter(self.model.id.in_(ids))
        count = 0
        for row in query:
            row.approved = initials
            count += 1
        db.session.commit()

        flash(ngettext('Row was successfully approved.',
                       '%(count)s rows were successfully approved.',
                       count,
                       count=count))
    except Exception as ex:
        if not self.handle_view_exception(ex):
            raise

        flash(gettext('Failed to approve rows. %(error)s', error=str(ex)),
              'error')


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

    action_approve_JP = action('approve_JP', 'Approve (JP)')(
        lambda self, ids: _approve(self, 'JP', ids))
    action_approve_PL = action('approve_PL', 'Approve (PL)')(
        lambda self, ids: _approve(self, 'PL', ids))
    action_approve_DM = action('approve_DM', 'Approve (DM)')(
        lambda self, ids: _approve(self, 'DM', ids))
    action_approve_DS = action('approve_DS', 'Approve (DS)')(
        lambda self, ids: _approve(self, 'DS', ids))
    action_approve_MW = action('approve_MW', 'Approve (MW)')(
        lambda self, ids: _approve(self, 'MW', ids))
    action_approve_RS = action('approve_RS', 'Approve (RS)')(
        lambda self, ids: _approve(self, 'RS', ids))
    action_approve_KW = action('approve_KW', 'Approve (KW)')(
        lambda self, ids: _approve(self, 'KW', ids))
    action_approve_KC = action('approve_KC', 'Approve (KC)')(
        lambda self, ids: _approve(self, 'KC', ids))
    action_approve_JA = action('approve_JA', 'Approve (JA)')(
        lambda self, ids: _approve(self, 'JA', ids))

    action_disapprove = action('disapprove', 'Disapprove')(
        lambda self, ids: _approve(self, None, ids))

    @action('clear', 'Clear', 'Are you sure you want to clear these records?')
    def action_clear(self, ids):
        try:
            query = self.model.query.filter(self.model.id.in_(ids))
            count = 0
            for row in query:
                row.text = None
                row.short_text = None
                row.url = None
                row.source = None
                count += 1
            db.session.commit()

            flash(ngettext('Row was successfully cleared.',
                           '%(count)s rows were successfully cleared.',
                           count,
                           count=count))
        except Exception as ex:
            if not self.handle_view_exception(ex):
                raise

            flash(gettext('Failed to approve rows. %(error)s', error=str(ex)),
                  'error')
        

class SectorDescriptionView(ModelView):
    column_list = ['name', 'short_text', 'text', 'source', 'approved']
    column_filters = ['name', 'approved']
    form_widget_args = {'text' : {
        'rows' : 10,
        'style' : 'font-family:"Lucida Console", Monaco, monospace;'
    }}


class CareerDescriptionView(ModelView):
    column_list = ['sector', 'name', 'short_text', 'text', 'source', 'approved']
    column_filters = ['name', 'sector', 'approved']
    form_widget_args = {'text' : {
        'rows' : 10,
        'style' : 'font-family:"Lucida Console", Monaco, monospace;'
    }}


class SkillDescriptionView(ModelView):
    column_list = ['sector', 'name', 'short_text', 'text', 'source', 'approved']
    column_filters = ['name', 'sector', 'approved']
    form_widget_args = {'text' : {
        'rows' : 10,
        'style' : 'font-family:"Lucida Console", Monaco, monospace;'
    }}


# Create admin
admin = Admin(app, name='DescriptionDB', template_mode='bootstrap3')
admin.add_view(SectorDescriptionView(SectorDescription, db.session))
admin.add_view(CareerDescriptionView(CareerDescription, db.session))
admin.add_view(SkillDescriptionView(SkillDescription, db.session))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5100, debug=True)

