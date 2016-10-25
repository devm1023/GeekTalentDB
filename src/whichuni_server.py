from flask import Flask, jsonify, request, Response
from functools import wraps
from whichunidb import *
from dbtools import dict_from_row
from sqlalchemy import func
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
wudb = WhichUniDB()

def check_auth(username, password):
    """This function is called to check if a username /
    password combination is valid.
    """
    return username == 'keiran' and password == 'YwRHJ8kB'

def authenticate():
    """Sends a 401 response that enables basic auth"""
    return Response(
    'Could not verify your access level for that URL.\n'
    'You have to login with proper credentials', 401,
    {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated

@app.errorhandler(404)
def not_found(error=None):
    message = {
            'status': 404,
            'message': 'Not Found: ' + request.url,
    }
    resp = jsonify(message)
    resp.status_code = 404

    return resp

@app.route('/api/courses/<string:uni>')
@requires_auth
def courses_at_uni(uni):
    query = wudb.query(WUCourse) \
                .join(WUUniversity) \
                .filter(func.lower(WUUniversity.name) == func.lower(uni)) \
                .all()
    return jsonify([collapse_course(q) for q in dict_from_row(query)])

@app.route('/api/courses/<string:uni>/<string:course>')
@requires_auth
def courses(uni, course):
    query = wudb.query(WUCourse) \
                .join(WUUniversity) \
                .filter(func.lower(WUUniversity.name) == func.lower(uni)) \
                .filter(func.lower(WUCourse.title) == func.lower(course)) \
                .all()
    return jsonify([collapse_course(q) for q in dict_from_row(query)])

@app.route('/api/courses/<string:uni>/<string:course>/<string:code>')
@requires_auth
def course(uni, course, code):
    query = wudb.query(WUCourse) \
                .join(WUUniversity) \
                .filter(func.lower(WUUniversity.name) == func.lower(uni)) \
                .filter(func.lower(WUCourse.title) == func.lower(course)) \
                .filter(func.lower(WUCourse.ucas_code) == func.lower(code)) \
                .first()
    return jsonify(collapse_course(dict_from_row(query)))

@app.route('/api/universities')
@requires_auth
def universities():
    q = dict_from_row(wudb.query(WUUniversity) \
                          .all())
    return jsonify({
        "universities": [collapse_university(row) for row in q]
    })

@app.route('/api/universities/<string:university_name>')
@requires_auth
def university(university_name):
    q = wudb.query(WUUniversity) \
            .filter(func.lower(university_name) == func.lower(WUUniversity.name)) \
            .first()
    if q is None:
        return not_found()
    else:
        return jsonify(collapse_university(dict_from_row(q)))
    
@app.route('/api/subjects')
@requires_auth
def subjects():
    q = dict_from_row(wudb.query(WUSubject) \
            .all())
        
    return jsonify({
        "subjects": [collapse_subject(row) for row in q]
    })

@app.route('/api/subjects/<string:subject_title>')
@requires_auth
def subject(subject_title):
    q = dict_from_row(wudb.query(WUSubject) \
            .filter(func.lower(subject_title) == func.lower(WUSubject.title)) \
            .first())
    if q is None:
        return not_found()
    else:
        return jsonify(collapse_subject(q))

@app.route('/api/typeahead/careers/<string:partial_career_name>')
@requires_auth
def typeahead_careers(partial_career_name):
    q = wudb.query(WUCareer.title) \
            .filter(func.lower(WUCareer.title).contains(func.lower(partial_career_name))) \
            .all()
    if q is not None:
        q = [item for sublist in q for item in sublist]
    return jsonify(q)

@app.route('/api/careers')
@requires_auth
def careers():
    q = dict_from_row(wudb.query(WUCareer).all())
    return jsonify(q)

@app.route('/api/careers/<string:career_name>')
@requires_auth
def career(career_name):
    q = wudb.query(WUCareer) \
        .filter(func.lower(career_name) == func.lower(WUCareer.title)) \
        .first()
    if q is None:
        return not_found()
    subjects_query = wudb.query(WUSubject) \
                         .join(WUSubjectCareer) \
                         .join(WUCareer) \
                         .filter(WUSubjectCareer.career_id == q.id) \
                         .all()
    careerdict = dict(
        title = q.title,
        subject = dict_from_row(subjects_query)[0]
    )
    if q is not None:
        return jsonify(careerdict)
    else:
        return not_found()

def collapse_course(row):
    del row['id']
    del row['university_id']
    if 'entry_requirements' in row:
        row['entry_requirements'] = [
            {
                "name": entry['entry_requirement']['name'],
                "grades": entry['grades'],
                "text": entry['text']
            } for entry in row['entry_requirements']
        ]
    if 'study_types' in row:
        for s in row['study_types']:
            del s['course_id']
            del s['id']
            s['duration'] = int(s['duration']) if s['duration'] else None
    if 'university' in row:
        row['university'] = collapse_university(row['university'])
    if 'university_subjects' in row:
        row['university_subjects'] = [collapse_university_subject(s) for s in row['university_subjects']]
    return row

def collapse_university(row):
    del row['city']['id']
    del row['id']
    del row['city_id']
    if 'university_characteristics' in row:
        row['characteristics'] = [
            { 
                'name': c['characteristic']['name'], 
                'score': c['score'],
                'score_r': c['score_r']
            } for c in row['university_characteristics']]
        del row['university_characteristics']
    if 'university_league_tables' in row:
        row['league_tables'] = [
            { 
                'name': c['league_table']['name'],
                'total': c['league_table']['total'],
                'rating': c['rating']
            } for c in row['university_league_tables']
        ]
        del row['university_league_tables']
    if 'university_tags' in row:
        row['tags'] = [c['tag']['name'] for c in row['university_tags']]
        del row['university_tags']
    return row

def collapse_university_subject(row):
    del row['id']
    del row['course_id']
    del row['subject_id']
    for rating in row['ratings']:
        del rating['id']
        del rating['university_subject_id']
    if 'sectors_after' in row:
        row['sectors_after'] = [
            {
                "name": s['sector_after']['name'],
                "percent": s['percent']
            } for s in row['sectors_after']
        ]
    if 'studied_before' in row:
        row['studied_before'] = [
            {
                "common_grade": s['common_grade'],
                "common_grade_percent": s['common_grade_percent'],
                "percent": s['percent'],
                "name": s['studied_before']['name']
            } for s in row['studied_before']
        ]
    if 'subject' in row:
        row['subject'] = collapse_subject(row['subject'])
    return row

def collapse_subject(row):
    if 'alevels' in row:
        row['alevels'] = [a['alevel'] for a in row['alevels']]
    if 'careers' in row:
        row['careers'] = [c['career'] for c in row['careers']]
    return row


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)