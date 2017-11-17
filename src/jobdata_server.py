from datetime import datetime

from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS

from sqlalchemy import func
from sqlalchemy.sql.expression import literal_column

import conf
from canonicaldb import CanonicalDB, ADZJob, INJob, LA, LEP, LAInLEP
from dbtools import dict_from_row

# Create application
app = Flask(__name__)
CORS(app)

# Create database sessions
cndb = CanonicalDB()


# Configure app and create database view
app.config['SQLALCHEMY_DATABASE_URI'] = conf.CANONICAL_DB
app.config['SQLALCHEMY_ECHO'] = True


def get_breakdown_for_source(table, category, titles, region_type):
    countcol = func.count().label('counts')

    if region_type == 'la':
        q = cndb.query(LA.gid, LA.lau118cd, LA.lau118nm, table.merged_title, countcol) \
                .join(table)
        group_field = LA.gid
    elif region_type == 'lep':
        q = cndb.query(LEP.id, LEP.name, LEP.name, table.merged_title, countcol) \
                .join(LAInLEP).join(LA).join(table)
        group_field = LEP.id
    elif region_type == 'nuts0' or region_type == 'nuts1' \
        or region_type == 'nuts2' or region_type == 'nuts3':

        if region_type == 'nuts0':
            group_field = table.nuts0
        elif region_type == 'nuts1':
            group_field = table.nuts1
        elif region_type == 'nuts2':
            group_field = table.nuts2
        elif region_type == 'nuts3':
            group_field = table.nuts3

        null_column = literal_column("NULL")
        q = cndb.query(null_column, group_field, null_column, table.merged_title, countcol) \
                .filter(group_field.isnot(None))
    else:
        return None

    if category:
        q = q.filter(table.category == category)
    if titles:
        q = q.filter(table.merged_title.in_(titles))

    q = q.group_by(group_field, table.merged_title)

    return q

# Flask views
@app.route('/')
def index():
    return ''


@app.route('/regional-breakdown/', methods=['GET'])
def get_ladata():
    start = datetime.now()
    category = request.args.get('category')
    titles = request.args.getlist('title')
    region_type = request.args.get('region_type', 'la')

    # get leps
    leps = None
    if region_type == 'la':
        leps = {}
        lepq = cndb.query(LAInLEP.la_id, LEP).join(LEP)

        for la_id, lep in lepq:
            if la_id not in leps:
                leps[la_id] = []
            leps[la_id].append(dict_from_row(lep))

    # build results
    results = {}
    total = 0

    def build_results(q):
        nonlocal total
        nonlocal results

        if q is None:
            return

        for region_id, region_code, region_name, job_title, count in q:
            key = region_code

            if job_title is None:
                job_title = "unknown"

            # new region
            if key not in results:
                results[key] = {}
                if region_name is not None:
                    results[key]['name'] = region_name
                # leps for la
                if leps is not None:
                    if region_id in leps:
                        results[key]['leps'] = leps[region_id]
                    else:
                        results[key]['leps'] = []
                results[key]['count'] = 0
                results[key]['merged_titles'] = {}

            results[key]['count'] += count

            # add title
            if job_title not in results[key]['merged_titles']:
                results[key]['merged_titles'][job_title] = 0
            results[key]['merged_titles'][job_title] += count

            total += count

    build_results(get_breakdown_for_source(ADZJob, category, titles, region_type))
    build_results(get_breakdown_for_source(INJob, category, titles, region_type))

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

