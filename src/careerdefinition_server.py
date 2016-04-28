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
from careerdefinitiondb import CareerDefinitionDB
from datetime import datetime

class BooleanNotTrueFilter(BaseSQLAFilter):
    def __init__(self, column, name):
        BaseSQLAFilter.__init__(self, column, name,
                                options=(('1', lazy_gettext('Yes')),
                                         ('0', lazy_gettext('No'))))

    def apply(self, query, value, alias=None):
        if value == '1':
            return query.filter((self.column == None) | (self.column == False))
        else:
            return query.filter(self.column == True)

    def operation(self):
        return 'is not true'


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
    results = cddb.get_careers(sectors, titles)
    end = datetime.now()
    response = jsonify({'results' : results,
                        'query_time' : (end-start).microseconds//1000,
                        'status' : 'OK'})
    print('response sent [{0:s}]' \
          .format(datetime.now().strftime('%d/%b/%Y %H:%M:%S')))
    return response

class EntityDescription(db.Model):
    __tablename__ = 'entity_description'
    id            = db.Column(db.BigInteger, primary_key=True)
    entity_type   = db.Column(db.String(20))
    linkedin_sector = db.Column(db.Unicode(STR_MAX))
    entity_name   = db.Column(db.Unicode(STR_MAX))
    match_count   = db.Column(db.Integer)
    description   = db.Column(db.Text)
    description_url = db.Column(db.String(STR_MAX))
    description_source = db.Column(db.Unicode(STR_MAX))
    edited        = db.Column(db.Boolean, nullable=False)

    def __str__(self):
        typestr = self.entity_type if self.entity_type else '*'
        sectorstr = self.linkedin_sector if self.linkedin_sector else '*'
        return '[{0:s}|{1:s}|{2:s}]'.format(typestr, sectorstr,
                                            self.entity_name)

class Career(db.Model):
    __tablename__ = 'career'
    id            = db.Column(db.BigInteger, primary_key=True)
    title         = db.Column(db.Unicode(STR_MAX), nullable=False)
    linkedin_sector = db.Column(db.Unicode(STR_MAX), nullable=False)
    count         = db.Column(db.BigInteger)
    relevance_score = db.Column(db.Float)
    visible       = db.Column(db.Boolean, nullable=False)

    skill_cloud = db.relationship('CareerSkill', backref='career',
                                 order_by='desc(CareerSkill.relevance_score)',
                                 cascade='all, delete-orphan')
    company_cloud \
        = db.relationship('CareerCompany', backref='career',
                          order_by='desc(CareerCompany.relevance_score)',
                          cascade='all, delete-orphan')
    education_subjects = db.relationship(
        'CareerSubject', backref='career',
        order_by='desc(CareerSubject.count)', cascade='all, delete-orphan')
    education_institutes = db.relationship(
        'CareerInstitute', backref='career',
        order_by='desc(CareerInstitute.count)', cascade='all, delete-orphan')
    previous_titles = db.relationship(
        'PreviousTitle', backref='career',
        order_by='desc(PreviousTitle.count)', cascade='all, delete-orphan')
    next_titles = db.relationship(
        'NextTitle', backref='career',
        order_by='desc(NextTitle.count)', cascade='all, delete-orphan')

    def __str__(self):
        return self.title

class SectorSkill(db.Model):
    __tablename__ = 'sector_skill'
    id            = db.Column(db.BigInteger, primary_key=True)
    sector_name   = db.Column(db.Unicode(STR_MAX), nullable=False)
    skill_name    = db.Column(db.Unicode(STR_MAX), nullable=False)
    count         = db.Column(db.BigInteger)
    relevance_score = db.Column(db.Float)
    visible       = db.Column(db.Boolean, nullable=False)

class CareerSkill(db.Model):
    __tablename__ = 'career_skill'
    id            = db.Column(db.BigInteger, primary_key=True)
    career_id     = db.Column(db.BigInteger,
                              db.ForeignKey('career.id',
                                            onupdate='CASCADE',
                                            ondelete='CASCADE'))
    skill_name    = db.Column(db.Unicode(STR_MAX), nullable=False)
    count         = db.Column(db.BigInteger)
    relevance_score = db.Column(db.Float)
    visible       = db.Column(db.Boolean, nullable=False)

    def __str__(self):
        return self.skill_name

class CareerCompany(db.Model):
    __tablename__ = 'career_company'
    id            = db.Column(db.BigInteger, primary_key=True)
    career_id     = db.Column(db.BigInteger,
                              db.ForeignKey('career.id',
                                            onupdate='CASCADE',
                                            ondelete='CASCADE'))
    company_name  = db.Column(db.Unicode(STR_MAX), nullable=False)
    count         = db.Column(db.BigInteger)
    relevance_score = db.Column(db.Float)
    visible       = db.Column(db.Boolean, nullable=False)

    def __str__(self):
        return self.company_name

class CareerSubject(db.Model):
    __tablename__ = 'career_subject'
    id            = db.Column(db.BigInteger, primary_key=True)
    career_id     = db.Column(db.BigInteger,
                              db.ForeignKey('career.id',
                                            onupdate='CASCADE',
                                            ondelete='CASCADE'))
    subject_name  = db.Column(db.Unicode(STR_MAX), nullable=False)
    count         = db.Column(db.BigInteger)
    visible       = db.Column(db.Boolean, nullable=False)

    def __str__(self):
        return self.subject_name

class CareerInstitute(db.Model):
    __tablename__ = 'career_institute'
    id            = db.Column(db.BigInteger, primary_key=True)
    career_id     = db.Column(db.BigInteger,
                              db.ForeignKey('career.id',
                                            onupdate='CASCADE',
                                            ondelete='CASCADE'))
    institute_name   = db.Column(db.Unicode(STR_MAX), nullable=False)
    count         = db.Column(db.BigInteger)
    visible       = db.Column(db.Boolean, nullable=False)

    def __str__(self):
        return self.institute_name

class PreviousTitle(db.Model):
    __tablename__ = 'previous_title'
    id            = db.Column(db.BigInteger, primary_key=True)
    career_id     = db.Column(db.BigInteger,
                              db.ForeignKey('career.id',
                                            onupdate='CASCADE',
                                            ondelete='CASCADE'))
    previous_title = db.Column(db.Unicode(STR_MAX), nullable=False)
    count         = db.Column(db.BigInteger)
    visible       = db.Column(db.Boolean, nullable=False)

    def __str__(self):
        return self.previous_title

class NextTitle(db.Model):
    __tablename__ = 'next_title'
    id            = db.Column(db.BigInteger, primary_key=True)
    career_id     = db.Column(db.BigInteger,
                              db.ForeignKey('career.id',
                                            onupdate='CASCADE',
                                            ondelete='CASCADE'))
    next_title    = db.Column(db.Unicode(STR_MAX), nullable=False)
    count         = db.Column(db.BigInteger)
    visible       = db.Column(db.Boolean, nullable=False)

    def __str__(self):
        return self.next_title



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

    @action('hide', 'Hide')
    def action_hide(self, ids):
        try:
            query = self.model.query.filter(self.model.id.in_(ids))
            count = 0
            for row in query:
                row.visible = False
                count += 1
            db.session.commit()

            flash(ngettext('Row was successfully hidden.',
                           '%(count)s rows were successfully hidden.',
                           count,
                           count=count))
        except Exception as ex:
            if not self.handle_view_exception(ex):
                raise

            flash(gettext('Failed to hide rows. %(error)s', error=str(ex)),
                  'error')

    @action('reveal', 'Reveal')
    def action_reveal(self, ids):
        try:
            query = self.model.query.filter(self.model.id.in_(ids))
            count = 0
            for row in query:
                row.visible = True
                count += 1
            db.session.commit()

            flash(ngettext('Row was successfully revealed.',
                           '%(count)s rows were successfully revealed.',
                           count,
                           count=count))
        except Exception as ex:
            if not self.handle_view_exception(ex):
                raise

            flash(gettext('Failed to reveal rows. %(error)s', error=str(ex)),
                  'error')


class SectorSkillView(ModelView):
    column_filters = ['sector_name', 'skill_name']


_text_area_style = {
    'rows' : 5,
    'style' : 'font-family:"Lucida Console", Monaco, monospace;'
}
class CareerView(ModelView):
    form_widget_args = {
        'count' : {'readonly' : True},
        'relevance_score' : {'readonly' : True},
    }
    inline_models = [
        (CareerSkill, {'form_widget_args' : {
            'relevance_score' : {'readonly' : True},
            'count' : {'readonly' : True},
        }}),
        (CareerCompany, {'form_widget_args' : {
            'relevance_score' : {'readonly' : True},
            'count' : {'readonly' : True}
        }}),
        (CareerSubject,
         {'form_widget_args' : {'count' : {'readonly' : True}}}),
        (CareerInstitute,
         {'form_widget_args' : {'count' : {'readonly' : True}}}),
        (PreviousTitle,
         {'form_widget_args' : {'count' : {'readonly' : True}}}),
        (NextTitle,
         {'form_widget_args' : {'count' : {'readonly' : True}}}),
    ]
    column_filters = ['linkedin_sector', 'title', 'visible']

class EntityDescriptionView(ModelView):
    column_filters = ['entity_type', 'linkedin_sector', 'entity_name', 'edited']
    column_exclude_list = ['match_count', 'description_url',
                           'description_source']
    form_widget_args = {'description' : _text_area_style}

class CareerSkillView(ModelView):
    column_filters = ['career', 'skill_name', 'visible']

class CareerCompanyView(ModelView):
    column_filters = ['career', 'company_name', 'visible']

class CareerSubjectView(ModelView):
    column_filters = ['career', 'subject_name', 'visible']
    form_widget_args = {'count' : {'readonly' : True}}

class CareerInstituteView(ModelView):
    column_filters = ['career', 'institute_name', 'visible']
    form_widget_args = {'count' : {'readonly' : True}}

class PreviousTitleView(ModelView):
    column_filters = ['career', 'previous_title', 'visible']
    form_widget_args = {'count' : {'readonly' : True}}

class NextTitleView(ModelView):
    column_filters = ['career', 'next_title', 'visible']
    form_widget_args = {'count' : {'readonly' : True}}


# Create admin
admin = Admin(app, name='CareerDefinitionDB', template_mode='bootstrap3')
admin.add_view(SectorSkillView(SectorSkill, db.session))
admin.add_view(CareerView(Career, db.session))
admin.add_view(CareerSkillView(CareerSkill, db.session))
admin.add_view(CareerCompanyView(CareerCompany, db.session))
admin.add_view(CareerSubjectView(CareerSubject, db.session))
admin.add_view(CareerInstituteView(CareerInstitute, db.session))
admin.add_view(PreviousTitleView(PreviousTitle, db.session))
admin.add_view(NextTitleView(NextTitle, db.session))
admin.add_view(EntityDescriptionView(EntityDescription, db.session))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

