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
    results = cddb.getCareers(sectors, titles)
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
    entityType    = db.Column(db.String(20))
    linkedinSector = db.Column(db.Unicode(STR_MAX))
    entityName    = db.Column(db.Unicode(STR_MAX))
    matchCount    = db.Column(db.Integer)
    description   = db.Column(db.Text)
    descriptionUrl = db.Column(db.String(STR_MAX))
    descriptionSource = db.Column(db.Unicode(STR_MAX))
    edited        = db.Column(db.Boolean, nullable=False)

    def __str__(self):
        typestr = self.entityType if self.entityType else '*'
        sectorstr = self.linkedinSector if self.linkedinSector else '*'
        return '[{0:s}|{1:s}|{2:s}]'.format(typestr, sectorstr, self.entityName)
    
class Career(db.Model):
    __tablename__ = 'career'
    id            = db.Column(db.BigInteger, primary_key=True)
    title         = db.Column(db.Unicode(STR_MAX), nullable=False)
    linkedinSector = db.Column(db.Unicode(STR_MAX), nullable=False)
    count         = db.Column(db.BigInteger)
    relevanceScore = db.Column(db.Float)
    visible       = db.Column(db.Boolean, nullable=False)

    skillCloud = db.relationship('CareerSkill', backref='career',
                                 order_by='desc(CareerSkill.relevanceScore)',
                                 cascade='all, delete-orphan')
    companyCloud \
        = db.relationship('CareerCompany', backref='career',
                          order_by='desc(CareerCompany.relevanceScore)',
                          cascade='all, delete-orphan')
    educationSubjects = db.relationship('CareerSubject', backref='career',
                                        order_by='desc(CareerSubject.count)',
                                        cascade='all, delete-orphan')
    educationInstitutes = db.relationship('CareerInstitute', backref='career',
                                          order_by='desc(CareerInstitute.count)',
                                          cascade='all, delete-orphan')
    previousTitles = db.relationship('PreviousTitle', backref='career',
                                     order_by='desc(PreviousTitle.count)',
                                     cascade='all, delete-orphan')
    nextTitles = db.relationship('NextTitle', backref='career',
                                 order_by='desc(NextTitle.count)',
                                 cascade='all, delete-orphan')

    def __str__(self):
        return self.title

class SectorSkill(db.Model):
    __tablename__ = 'sector_skill'
    id            = db.Column(db.BigInteger, primary_key=True)
    sectorName    = db.Column(db.Unicode(STR_MAX), nullable=False)
    skillName     = db.Column(db.Unicode(STR_MAX), nullable=False)
    count         = db.Column(db.BigInteger)
    relevanceScore = db.Column(db.Float)    
    visible       = db.Column(db.Boolean, nullable=False)
    
class CareerSkill(db.Model):
    __tablename__ = 'career_skill'
    id            = db.Column(db.BigInteger, primary_key=True)
    careerId      = db.Column(db.BigInteger,
                              db.ForeignKey('career.id',
                                            onupdate='CASCADE',
                                            ondelete='CASCADE'))
    skillName     = db.Column(db.Unicode(STR_MAX), nullable=False)
    count         = db.Column(db.BigInteger)
    relevanceScore = db.Column(db.Float)
    visible       = db.Column(db.Boolean, nullable=False)

    def __str__(self):
        return self.skillName

class CareerCompany(db.Model):
    __tablename__ = 'career_company'
    id            = db.Column(db.BigInteger, primary_key=True)
    careerId      = db.Column(db.BigInteger,
                              db.ForeignKey('career.id',
                                            onupdate='CASCADE',
                                            ondelete='CASCADE'))
    companyName     = db.Column(db.Unicode(STR_MAX), nullable=False)
    count         = db.Column(db.BigInteger)
    relevanceScore = db.Column(db.Float)
    visible       = db.Column(db.Boolean, nullable=False)

    def __str__(self):
        return self.companyName
    
class CareerSubject(db.Model):
    __tablename__ = 'career_subject'
    id            = db.Column(db.BigInteger, primary_key=True)
    careerId      = db.Column(db.BigInteger,
                              db.ForeignKey('career.id',
                                            onupdate='CASCADE',
                                            ondelete='CASCADE'))
    subjectName   = db.Column(db.Unicode(STR_MAX), nullable=False)
    count         = db.Column(db.BigInteger)
    visible       = db.Column(db.Boolean, nullable=False)

    def __str__(self):
        return self.subjectName

class CareerInstitute(db.Model):
    __tablename__ = 'career_institute'
    id            = db.Column(db.BigInteger, primary_key=True)
    careerId      = db.Column(db.BigInteger,
                              db.ForeignKey('career.id',
                                            onupdate='CASCADE',
                                            ondelete='CASCADE'))
    instituteName   = db.Column(db.Unicode(STR_MAX), nullable=False)
    count         = db.Column(db.BigInteger)
    visible       = db.Column(db.Boolean, nullable=False)

    def __str__(self):
        return self.instituteName

class PreviousTitle(db.Model):
    __tablename__ = 'previous_title'
    id            = db.Column(db.BigInteger, primary_key=True)
    careerId      = db.Column(db.BigInteger,
                              db.ForeignKey('career.id',
                                            onupdate='CASCADE',
                                            ondelete='CASCADE'))
    previousTitle = db.Column(db.Unicode(STR_MAX), nullable=False)
    count         = db.Column(db.BigInteger)
    visible       = db.Column(db.Boolean, nullable=False)

    def __str__(self):
        return self.previousTitle

class NextTitle(db.Model):
    __tablename__ = 'next_title'
    id            = db.Column(db.BigInteger, primary_key=True)
    careerId      = db.Column(db.BigInteger,
                              db.ForeignKey('career.id',
                                            onupdate='CASCADE',
                                            ondelete='CASCADE'))
    nextTitle = db.Column(db.Unicode(STR_MAX), nullable=False)
    count         = db.Column(db.BigInteger)
    visible       = db.Column(db.Boolean, nullable=False)

    def __str__(self):
        return self.nextTitle

    

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
    column_filters = ['sectorName', 'skillName']
    
    
_textAreaStyle = {
    'rows' : 5,
    'style' : 'font-family:"Lucida Console", Monaco, monospace;'
}
class CareerView(ModelView):
    form_widget_args = {
        'count' : {'readonly' : True},
        'relevanceScore' : {'readonly' : True},
    }
    inline_models = [
        (CareerSkill, {'form_widget_args' : {
            'relevanceScore' : {'readonly' : True},
            'count' : {'readonly' : True},
        }}),
        (CareerCompany, {'form_widget_args' : {
            'relevanceScore' : {'readonly' : True},
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
    column_filters = ['linkedinSector', 'title', 'visible']

class EntityDescriptionView(ModelView):
    column_filters = ['entityType', 'linkedinSector', 'entityName', 'edited']
    column_exclude_list = ['matchCount', 'descriptionUrl', 'descriptionSource']
    form_widget_args = {'description' : _textAreaStyle}

class CareerSkillView(ModelView):
    column_filters = ['career', 'skillName', 'visible']

class CareerCompanyView(ModelView):
    column_filters = ['career', 'companyName', 'visible']
    
class CareerSubjectView(ModelView):
    column_filters = ['career', 'subjectName', 'visible']
    form_widget_args = {'count' : {'readonly' : True}}

class CareerInstituteView(ModelView):
    column_filters = ['career', 'instituteName', 'visible']
    form_widget_args = {'count' : {'readonly' : True}}

class PreviousTitleView(ModelView):
    column_filters = ['career', 'previousTitle', 'visible']
    form_widget_args = {'count' : {'readonly' : True}}

class NextTitleView(ModelView):
    column_filters = ['career', 'nextTitle', 'visible']
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
    app.run(debug=True)

