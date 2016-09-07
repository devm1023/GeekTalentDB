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

@app.route('/api/subjects')
@requires_auth
def subjects():
    q = dict_from_row(wudb.query(WUSubject) \
            .all())
        
    return jsonify({
        "subjects": [collapse(row) for row in q]
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
        return jsonify(collapse(q))

@app.route('/api/careers')
@requires_auth
def careers():
    q = dict_from_row(wudb.query(WUCareer).all())
    return jsonify(q)

@app.route('/api/careers/<string:career_name>')
@requires_auth
def career(career_name):
    q = dict_from_row(wudb.query(WUCareer) \
        .filter(func.lower(career_name) == func.lower(WUCareer.title))
        .first())
    if q is None:
        return jsonify(q)
    else:
        return not_found()
    

def collapse(row):
    row['alevels'] = [a['alevel'] for a in row['alevels']]
    row['careers'] = [c['career'] for c in row['careers']]
    return row


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)