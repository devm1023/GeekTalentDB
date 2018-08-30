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

def get_region_field(table, region_type, code=False):
    if region_type == 'la':
        return LA.lau118cd if code else LA.gid
    elif region_type == 'lep':
        return LEP.name if code else LEP.id
    elif region_type == 'nuts0':
        return table.nuts0
    elif region_type == 'nuts1':
        return table.nuts1
    elif region_type == 'nuts2':
        return table.nuts2
    elif region_type == 'nuts3':
        return table.nuts3

    return None

def apply_common_filters(q, table):
    category = request.args.get('category')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if category:
        q = q.filter(table.category == category)
    if start_date is not None:
        q = q.filter(func.date(table.created) >= start_date)
    if end_date is not None:
        q = q.filter(func.date(table.created) <= end_date)

    return q

def get_breakdown_for_source(table, titles, region_type):
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

    q = apply_common_filters(q, table)

    if titles:
        if len(titles) == 1 and titles[0] == 'unknown':
            q = q.filter(table.merged_title.is_(None))
        else:
            q = q.filter(table.merged_title.in_(titles))

    q = q.group_by(group_field, table.merged_title)
    return q

@app.after_request
def add_cache_headers(response):
    response.cache_control.max_age = 60 * 60 * 12
    return response

# Flask views
@app.route('/')
def index():
    return ''


@app.route('/valid-dates/', methods=['GET'])
def get_valid_dates():
    start = datetime.now()
    titles = request.args.getlist('title')
    region_type = request.args.get('region_type', 'la')
    region = request.args.get('region')

    def do_query(table):
        q = db.session.query(func.min(table.created), func.max(table.created))

        # filters
        q = apply_common_filters(q, table)

        if region:
            if region_type == 'la':
                q = q.join(LA)
            elif region_type == 'lep':
                q = q.join(LAInLEP, table.la_id == LAInLEP.la_id) \
                     .join(LEP)

            region_field = get_region_field(table, region_type, code=True)
            q = q.filter(region_field == region)

        return q.one()

    results = {}

    try:
        adz_min, adz_max = do_query(ADZJob)
        in_min, in_max = do_query(INJob)

        min_valid = max(adz_min, in_min)
        max_valid = min(adz_max, in_max)

        results = {
            'min': min_valid.isoformat(),
            'max': max_valid.isoformat()
        }
    except SQLAlchemyError as e:
        return jsonify({
            'error': 'Database error',
            'exception': repr(e) if app.debug else None
        }), 500

    end = datetime.now()
    time_taken = end - start
    response = jsonify({'results' : results,
                        'query_time' : time_taken.seconds * 1000 + time_taken.microseconds//1000,
                        'status' : 'OK'})
    print('response sent [{0:s}]' \
          .format(datetime.now().strftime('%d/%b/%Y %H:%M:%S')))
    return response

@app.route('/regional-breakdown/', methods=['GET'])
def get_ladata():
    start = datetime.now()
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
        build_results(get_breakdown_for_source(ADZJob, titles, region_type))
        build_results(get_breakdown_for_source(INJob, titles, region_type))
    except SQLAlchemyError as e:
        return jsonify({
            'error': 'Database error',
            'exception': repr(e) if app.debug else None
        }), 500


    end = datetime.now()
    time_taken = end - start
    response = jsonify({'results' : results,
                        'total': total,
                        'query_time' : time_taken.seconds * 1000 + time_taken.microseconds//1000,
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

    def built_query(jobstable, skillstable, mergedtitle, region, region_type):
        countcol = func.count().label('counts')

        q = db.session.query(skillstable.name, countcol) \
            .join(jobstable) \
            .filter(skillstable.language == 'en') \
            .filter(jobstable.language == 'en')

        q = apply_common_filters(q, jobstable)

        if merged_title:
            if merged_title == 'unknown':
                q = q.filter(jobstable.merged_title.is_(None))
            else:
                q = q.filter(jobstable.merged_title == merged_title)

        if region:
            if region_type == 'la':
                q = q.join(LA, jobstable.la_id == LA.gid)
            elif region_type == 'lep':
                q = q.join(LAInLEP, jobstable.la_id == LAInLEP.la_id) \
                        .join(LEP, LAInLEP.lep_id == LEP.id)

            region_field = get_region_field(jobstable, region_type, code=True)
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
        build_results(built_query(ADZJob, ADZJobSkill, merged_title, region, region_type))
        build_results(built_query(INJob, INJobSkill, merged_title, region, region_type))
        results = attach_tfidfs(results)
    except SQLAlchemyError as e:
        return jsonify({
            'error': 'Database error',
            'exception': repr(e) if app.debug else None
        }), 500

    results = sort_trim(results, limit)

    end = datetime.now()
    time_taken = end - start
    response = jsonify({'results': results,
                        'total': total,
                        'query_time': time_taken.seconds * 1000 + time_taken.microseconds//1000,
                        'status': 'OK'})
    print('response sent [{0:s}]' \
          .format(datetime.now().strftime('%d/%b/%Y %H:%M:%S')))
    return response

@app.route('/salaries/', methods=['GET'])
def get_salaries():
    start = datetime.now()
    titles = request.args.getlist('title')
    region_type = request.args.get('region_type', 'la')
    region = request.args.get('region')
    period = request.args.get('period')
    group_period = request.args.get('group_period')

    if group_period is not None and group_period not in ['month', 'quarter']:
        return jsonify({'error': 'Invalid group_period. Valid values: month, quarter'}), 400

    # build results
    results = []
    total = 0

    def build_results(table):
        nonlocal total
        nonlocal results
        nonlocal period

        count_col = func.count()

        if group_period == 'month':
            date_cols = (func.extract('month', table.created), func.extract('year', table.created))
        elif group_period == 'quarter':
            date_cols = (func.floor((func.extract('month', table.created) - 1) / 3) + 1, func.extract('year', table.created))
        else:
            null_column = literal_column("NULL")
            date_cols = (null_column, null_column)

        q = db.session.query(table.merged_title, table.salary_period, count_col, func.min(table.salary_min),
                             func.max(table.salary_max), func.avg(table.salary_min), func.avg(table.salary_max),
                             *date_cols)

        # filters
        q = apply_common_filters(q, table)

        if region:
            if region_type == 'la':
                q = q.join(LA)
            elif region_type == 'lep':
                q = q.join(LAInLEP, table.la_id == LAInLEP.la_id) \
                     .join(LEP)

            region_field = get_region_field(table, region_type, code=True)
            q = q.filter(region_field == region)

        if titles:
            if len(titles) == 1 and titles[0] == 'unknown':
                q = q.filter(table.merged_title.is_(None))
            else:
                q = q.filter(table.merged_title.in_(titles))

        if period:
            q = q.filter(table.salary_period == period)

        q = q.group_by(table.merged_title, table.salary_period)

        if group_period:
            q = q.group_by(*date_cols)

        # format results
        for title, period, count, salary_min, salary_max, salary_min_avg, salary_max_avg, month, year in q:

            # all null - no data
            if salary_min is None and salary_max is None:
                continue

            total += count

            salary_min = round(float(salary_min), 2) if salary_min is not None else None
            salary_max = round(float(salary_max), 2) if salary_max is not None else None

            salary_min_avg = round(float(salary_min_avg), 2) if salary_min_avg is not None else None
            salary_max_avg = round(float(salary_max_avg), 2) if salary_max_avg is not None else None

            # merge with existing
            found = False
            for res in results:
                if (res['merged_title'], res['period'], res['year'], res['month_quarter']) == (title, period, year, month):
                    res['count'] += count

                    if res['min'] is None:
                        res['min'] = salary_min
                        res['min_avg'] = salary_min_avg
                    elif salary_min is not None:
                        res['min'] = min(res['min'], salary_min)
                        # there are at most two results to merge
                        res['min_avg'] = round((res['min_avg'] + salary_min_avg) / 2, 2)

                    if res['max'] is None:
                        res['max'] = salary_max
                        res['max_avg'] = salary_max_avg
                    elif salary_max is not None:
                        res['max'] = max(res['max'], salary_max)
                        res['max_avg'] = round((res['max_avg'] + salary_max_avg) / 2, 2)

                    found = True
                    break

            if found:
                continue

            results.append({
                'merged_title': title,
                'period': period,
                'count': count,
                'min': salary_min,
                'max': salary_max,
                'min_avg': salary_min_avg,
                'max_avg': salary_max_avg,
                'year': year,
                'month_quarter': month
            })

    try:
        build_results(ADZJob)
        build_results(INJob)
    except SQLAlchemyError as e:
        return jsonify({
            'error': 'Database error',
            'exception': repr(e) if app.debug else None
        }), 500


    end = datetime.now()
    time_taken = end - start
    response = jsonify({'results' : results,
                        'total': total,
                        'query_time' : time_taken.seconds * 1000 + time_taken.microseconds//1000,
                        'status' : 'OK'})
    print('response sent [{0:s}]' \
          .format(datetime.now().strftime('%d/%b/%Y %H:%M:%S')))
    return response

@app.route('/history/', methods=['GET'])
def get_history():
    start = datetime.now()
    titles = request.args.getlist('title')
    region_type = request.args.get('region_type', 'la')
    region = request.args.get('region')
    group_period = request.args.get('group_period')

    if group_period is not None and group_period not in ['month', 'quarter']:
        return jsonify({'error': 'Invalid group_period. Valid values: month, quarter'}), 400

    # build results
    results = []
    total = 0

    def build_results(table):
        nonlocal total
        nonlocal results

        count_col = func.count()
        null_column = literal_column("NULL")

        if group_period == 'month':
            date_cols = (func.extract('month', table.created), func.extract('year', table.created))
        elif group_period == 'quarter':
            date_cols = (func.floor((func.extract('month', table.created) - 1) / 3) + 1, func.extract('year', table.created))
        else:
            date_cols = (null_column, null_column)

        title_col = table.merged_title if titles else null_column

        q = db.session.query(title_col, count_col, *date_cols)

        # filters
        q = apply_common_filters(q, table)

        if region:
            if region_type == 'la':
                q = q.join(LA)
            elif region_type == 'lep':
                q = q.join(LAInLEP, table.la_id == LAInLEP.la_id) \
                     .join(LEP)

            region_field = get_region_field(table, region_type, code=True)
            q = q.filter(region_field == region)

        if titles:
            if len(titles) == 1 and titles[0] == 'unknown':
                q = q.filter(table.merged_title.is_(None))
            else:
                q = q.filter(table.merged_title.in_(titles))

            q = q.group_by(table.merged_title)

        if group_period:
            q = q.group_by(*date_cols)

        # format results
        for title, count, month, year in q:

            total += count

            # merge with existing
            found = False
            for res in results:
                if (res['merged_title'], res['year'], res['month_quarter']) == (title, year, month):
                    res['count'] += count

                    found = True
                    break

            if found:
                continue

            results.append({
                'merged_title': title,
                'count': count,
                'year': year,
                'month_quarter': month
            })

    try:
        build_results(ADZJob)
        build_results(INJob)
    except SQLAlchemyError as e:
        return jsonify({
            'error': 'Database error',
            'exception': repr(e) if app.debug else None
        }), 500


    end = datetime.now()
    time_taken = end - start
    response = jsonify({'results' : results,
                        'total': total,
                        'query_time' : time_taken.seconds * 1000 + time_taken.microseconds//1000,
                        'status' : 'OK'})
    print('response sent [{0:s}]' \
          .format(datetime.now().strftime('%d/%b/%Y %H:%M:%S')))
    return response

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
