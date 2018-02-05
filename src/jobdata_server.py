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


def get_breakdown_for_source(table, category, titles, region_type):
    countcol = func.count().label('counts')

    if region_type == 'la':
        q = db.session.query(LA.gid, LA.lau118cd, LA.lau118nm, table.merged_title, countcol) \
                .join(table)
        group_field = LA.gid
    elif region_type == 'lep':
        q = db.session.query(LEP.id, LEP.name, LEP.name, table.merged_title, countcol) \
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
        q = db.session.query(null_column, group_field, null_column, table.merged_title, countcol) \
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
        build_results(get_breakdown_for_source(ADZJob, category, titles, region_type))
        build_results(get_breakdown_for_source(INJob, category, titles, region_type))
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
            q = q.filter(jobstable.merged_title == merged_title)

        if region:
            if region_type == 'la':
                q = q.join(LA, jobstable.la_id == LA.gid) \
                    .filter(LA.lau118cd == region)
            elif region_type == 'lep':
                q = q.join(LAInLEP, jobstable.la_id == LAInLEP.la_id) \
                        .join(LEP, LAInLEP.lep_id == LEP.id) \
                        .filter(LEP.name == region)
            elif region_type == 'nuts0':
                q = q.filter(jobstable.nuts0 == region)
            elif region_type == 'nuts1':
                q = q.filter(jobstable.nuts1 == region)
            elif region_type == 'nuts2':
                q = q.filter(jobstable.nuts2 == region)
            elif region_type == 'nuts3':
                q = q.filter(jobstable.nuts3 == region)

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
