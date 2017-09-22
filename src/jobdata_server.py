from datetime import datetime

from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS

from sqlalchemy import func

import conf
from canonicaldb import CanonicalDB, ADZJob, LA, LEP, LAInLEP
from dbtools import dict_from_row

# Create application
app = Flask(__name__)
CORS(app)

# Create database sessions
cndb = CanonicalDB()


# Configure app and create database view
app.config['SQLALCHEMY_DATABASE_URI'] = conf.CANONICAL_DB
app.config['SQLALCHEMY_ECHO'] = True


# Flask views
@app.route('/')
def index():
    return ''


@app.route('/regional-breakdown/', methods=['GET'])
def get_ladata():
    start = datetime.now()
    category = request.args.get('category')
    titles = request.args.getlist('title')

    countcol = func.count().label('counts')
    q = cndb.query(LA.gid, LA.lau118cd, LA.lau118nm, ADZJob.merged_title, countcol) \
            .join(ADZJob)

    if category:
        q = q.filter(ADZJob.category == category)
    if titles:
        q = q.filter(ADZJob.merged_title.in_(titles))

    q = q.group_by(LA.gid, ADZJob.merged_title)

    # get leps
    lepq = cndb.query(LAInLEP.la_id, LEP).join(LEP)
    leps = {}

    for la_id, lep in lepq:
        if la_id not in leps:
            leps[la_id] = []
        leps[la_id].append(dict_from_row(lep))

    # build results
    results = {}
    total = 0

    for la_id, lau_code, lau_name, job_title, count in q:
        key = lau_code

        if job_title is None:
            job_title = "unknown"

        # new la
        if key not in results:
            results[key] = {}
            results[key]['name'] = lau_name
            if la_id in leps:
                results[key]['leps'] = leps[la_id]
            else:
                results[key]['leps'] = []
            results[key]['count'] = 0
            results[key]['merged_titles'] = {}

        results[key]['count'] += count

        # add title
        results[key]['merged_titles'][job_title] = count

        total += count

    end = datetime.now()
    response = jsonify({'results' : results,
                        'total': total,
                        'query_time' : (end-start).microseconds//1000,
                        'status' : 'OK'})
    print('response sent [{0:s}]' \
          .format(datetime.now().strftime('%d/%b/%Y %H:%M:%S')))
    return response

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

