from collections import Counter
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, desc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.expression import literal_column

import conf
from canonicaldb import ADZJob, ADZJobSkill, INJob, INJobSkill, LA, LEP, LAInLEP, SkillsIdf
from dbtools import dict_from_row

# Create application
app = Flask(__name__)
CORS(app)

# Configure app and create database view
app.config['SQLALCHEMY_DATABASE_URI'] = conf.CANONICAL_DB
app.config['SQLALCHEMY_ECHO'] = True
db = SQLAlchemy(app)

def get_region_field(table, region_type):
    if region_type == 'la':
        return LA.gid
    elif region_type == 'lep':
        return LEP.id
    elif region_type == 'nuts0':
        return table.nuts0
    elif region_type == 'nuts1':
        return table.nuts1
    elif region_type == 'nuts2':
        return table.nuts2
    elif region_type == 'nuts3':
        return table.nuts3

    return None

def get_breakdown_for_source(table, category, titles, region_type, start_date, end_date):
    countcol = func.count().label('counts')

    group_field = get_region_field(table, region_type)

    # (id, code, name, ...)
    if region_type == 'la':
        q = db.session.query(LA.gid, LA.lau118cd, LA.lau118nm, table.merged_title, countcol) \
                .join(table)
    elif region_type == 'lep':
        q = db.session.query(LEP.id, LEP.name, LEP.name, table.merged_title, countcol) \
                .join(LAInLEP).join(LA).join(table)
    elif group_field is not None:
        null_column = literal_column("NULL")
        q = db.session.query(null_column, group_field, null_column, table.merged_title, countcol) \
                .filter(group_field.isnot(None))
    else:
        return None

    if category:
        q = q.filter(table.category == category)
    if titles:
        if len(titles) == 1 and titles[0] == 'unknown':
            q = q.filter(table.merged_title.is_(None))
        else:
            q = q.filter(table.merged_title.in_(titles))

    if start_date is not None:
        q = q.filter(func.date(table.created) >= start_date)
    if end_date is not None:
        q = q.filter(func.date(table.created) <= end_date)

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
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    # get leps
    leps = None

    try:
        if region_type == 'la':
            leps = {}
            lepq = db.session.query(LAInLEP.la_id, LEP).join(LEP)

            for la_id, lep in lepq:
                if la_id not in leps:
                    leps[la_id] = []
                leps[la_id].append(dict_from_row(lep))

    except SQLAlchemyError as e:
        return jsonify({
            'error': 'Database error',
            'exception': repr(e) if app.debug else None
        }), 500

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

    try:
        build_results(get_breakdown_for_source(ADZJob, category, titles, region_type, start_date, end_date))
        build_results(get_breakdown_for_source(INJob, category, titles, region_type, start_date, end_date))
    except SQLAlchemyError as e:
        return jsonify({
            'error': 'Database error',
            'exception': repr(e) if app.debug else None
        }), 500


    end = datetime.now()
    response = jsonify({'results' : results,
                        'total': total,
                        'query_time' : (end-start).microseconds//1000,
                        'status' : 'OK'})
    print('response sent [{0:s}]' \
          .format(datetime.now().strftime('%d/%b/%Y %H:%M:%S')))
    return response


@app.route('/merged-title-skills/', methods=['GET'])
def get_mergedtitleskills():
    start = datetime.now()
    category = request.args.get('category')
    merged_title = request.args.get('mergedtitle')
    region = request.args.get('region')
    region_type = request.args.get('region_type', 'la')
    limit = request.args.get('limit', 20)
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    # build results
    results = {}
    total = 0

    def built_query(jobstable, skillstable ,category, mergedtitle, region, region_type):
        countcol = func.count().label('counts')

        q = db.session.query(skillstable.name, countcol) \
            .join(jobstable) \
            .filter(skillstable.language == 'en') \
            .filter(jobstable.language == 'en')

        if category:
            q = q.filter(jobstable.category == category)
        if merged_title:
            if merged_title == 'unknown':
                q = q.filter(jobstable.merged_title.is_(None))
            else:
                q = q.filter(jobstable.merged_title == merged_title)

        if start_date is not None:
            q = q.filter(func.date(jobstable.created) >= start_date)
        if end_date is not None:
            q = q.filter(func.date(jobstable.created) <= end_date)

        if region:
            if region_type == 'la':
                q = q.join(LA, jobstable.la_id == LA.gid)
            elif region_type == 'lep':
                q = q.join(LAInLEP, jobstable.la_id == LAInLEP.la_id) \
                        .join(LEP, LAInLEP.lep_id == LEP.id)

            region_field = get_region_field(jobstable, region_type)
            q = q.filter(region_field == region)

        q = q.group_by(skillstable.name) \
            .order_by(desc(countcol))

        return q

    def build_results(q):
        nonlocal total
        nonlocal results

        if q is None:
            return

        for skill_name, count in q:
            if skill_name not in results:
                results[skill_name] = 0
            results[skill_name] += count

        total = len(results)

    def attach_tfidfs(res):
        idfs = dict(db.session.query(SkillsIdf.name, SkillsIdf.idf).filter(SkillsIdf.name.in_(res.keys())))
        print('idfs size: {0:d}'.format(len(idfs)))
        return {skill: res[skill] * idfs.get(skill, 0) for skill in res.keys()}

    def sort_trim(res, size):
        names = list()
        tfidfs = list()
        for skill, tfidf in Counter(res).most_common(size):
            names.append(skill)
            tfidfs.append(tfidf)
        return {'skill_names': names, 'skill_tfidf': tfidfs}

    try:
        build_results(built_query(ADZJob, ADZJobSkill, category, merged_title, region, region_type))
        build_results(built_query(INJob, INJobSkill, category, merged_title, region, region_type))
        results = attach_tfidfs(results)
    except SQLAlchemyError as e:
        return jsonify({
            'error': 'Database error',
            'exception': repr(e) if app.debug else None
        }), 500

    results = sort_trim(results, limit)

    end = datetime.now()
    response = jsonify({'results': results,
                        'total': total,
                        'query_time': (end-start).microseconds//1000,
                        'status': 'OK'})
    print('response sent [{0:s}]' \
          .format(datetime.now().strftime('%d/%b/%Y %H:%M:%S')))
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
