from collections import Counter
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, logging
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, desc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.expression import literal_column

import conf
from canonicaldb import ADZJob, ADZJobSkill, INJob, INJobSkill, LA, LEP, LAInLEP, SkillsIdf, ReportFactJobs, \
    ReportDimDatePeriod, ReportDimRegionCode, ReportJobSkill
from dbtools import dict_from_row

# Create application
app = Flask(__name__)
CORS(app)

# Configure app and create database view
app.config['SQLALCHEMY_DATABASE_URI'] = conf.CANONICAL_DB
app.config['SQLALCHEMY_ECHO'] = True
db = SQLAlchemy(app)
db.init_app(app)


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


def apply_common_filters(q, table, source):
    category = request.args.get('category')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    qtr = None

    if start_date is not None and end_date is not None:
        qtr = db.session.query(ReportDimDatePeriod.period_name).filter(ReportDimDatePeriod.start_date == \
                                                                       datetime.strptime(start_date, '%Y-%m-%d')) \
            .filter(ReportDimDatePeriod.end_date == datetime.strptime(end_date, '%Y-%m-%d')).all()

    if not qtr or source is not "regional_breakdown":
        if start_date is not None:
            q = q.filter(table.created >= start_date)
        if end_date is not None:
            end_date = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
            q = q.filter(table.created < end_date)
        if category is not None:
            q = q.filter(table.category == category)
    elif qtr and source is "regional_breakdown":
        q = q.filter(ReportFactJobs.date_period == qtr[0])
        if category is not None:
            q = q.filter(ReportFactJobs.category == category)

    return q


def get_breakdown_for_source(table, titles, region_type, qtr_param):
    countcol = func.count().label('counts')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if request.args.get('category') == 'all_sectors':
        if qtr_param is None:  # For Quarterly Queries use the Fact Table - otherwise use core data tables ADZJob and INDJob
            group_field = get_region_field(table, region_type)

            # (id, code, name, ...)
            if region_type == 'la':
                q = db.session.query(LA.gid, LA.lau118cd, LA.lau118nm, table.category, countcol).join(table)
            elif region_type == 'lep':
                q = db.session.query(LEP.id, LEP.name, LEP.name, table.category, countcol) \
                    .join(LAInLEP).join(LA).join(table)
            elif group_field is not None:
                null_column = literal_column("NULL")
                q = db.session.query(null_column, group_field, null_column, table.category, countcol) \
                    .filter(group_field.isnot(None))
            else:
                return None
        else:
            if region_type == 'la' or region_type == 'lep':
                q = db.session.query(ReportDimRegionCode.region_ref.label("region_id"), \
                                     ReportFactJobs.region_code.label("region_code"), \
                                     ReportDimRegionCode.region_name.label("region_name"), \
                                     ReportFactJobs.category.label("job_title"), \
                                     func.sum(ReportFactJobs.total_jobs).label("count")) \
                    .filter(ReportFactJobs.region_type == region_type.upper()) \
                    .filter(ReportFactJobs.region_code == ReportDimRegionCode.region_code)

                q = q.group_by(ReportDimRegionCode.region_ref, ReportFactJobs.region_code, ReportDimRegionCode.region_name, ReportFactJobs.category)

            else:
                null_column = literal_column("NULL")
                q = db.session.query(null_column, \
                                     ReportFactJobs.region_code.label("region_code"), \
                                     null_column, \
                                     ReportFactJobs.category.label("job_title"), \
                                     func.sum(ReportFactJobs.total_jobs).label("count")) \
                    .filter(ReportFactJobs.region_type == region_type.upper()) \
                    .filter(ReportFactJobs.region_code == ReportDimRegionCode.region_code)

                q = q.group_by(ReportFactJobs.region_code, ReportFactJobs.category)
    else:
        if qtr_param is None:  # For Quarterly Queries use the Fact Table - otherwise use core data tables ADZJob and INDJob
            group_field = get_region_field(table, region_type)

            # (id, code, name, ...)
            if region_type == 'la':
                q = db.session.query(LA.gid, LA.lau118cd, LA.lau118nm, table.merged_title, countcol).join(table)
            elif region_type == 'lep':
                q = db.session.query(LEP.id, LEP.name, LEP.name, table.merged_title, countcol) \
                    .join(LAInLEP).join(LA).join(table)
            elif group_field is not None:
                null_column = literal_column("NULL")
                q = db.session.query(null_column, group_field, null_column, table.merged_title, countcol) \
                    .filter(group_field.isnot(None))
            else:
                return None
        else:
            if region_type == 'la' or region_type == 'lep':
                q = db.session.query(ReportDimRegionCode.region_ref.label("region_id"), \
                                    ReportFactJobs.region_code.label("region_code"), \
                                    ReportDimRegionCode.region_name.label("region_name"), \
                                    ReportFactJobs.merged_title.label("job_title"), \
                                    ReportFactJobs.total_jobs.label("count")) \
                    .filter(ReportFactJobs.region_type == region_type.upper()) \
                    .filter(ReportFactJobs.region_code == ReportDimRegionCode.region_code)
            else:
                null_column = literal_column("NULL")
                q = db.session.query(null_column, \
                                    ReportFactJobs.region_code.label("region_code"), \
                                    null_column, \
                                    ReportFactJobs.merged_title.label("job_title"), \
                                    ReportFactJobs.total_jobs.label("count")) \
                    .filter(ReportFactJobs.region_type == region_type.upper()) \
                    .filter(ReportFactJobs.region_code == ReportDimRegionCode.region_code)
        q = apply_common_filters(q, table, "regional_breakdown")

    if titles:
        if len(titles) == 1 and titles[0] == 'unknown':
            q = q.filter(table.merged_title.is_(None))
        else:
            q = q.filter(table.merged_title.in_(titles))

    if qtr_param is None:
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
        q = apply_common_filters(q, table, "valid_dates")

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
    response = jsonify({'results': results,
                        'query_time': time_taken.seconds * 1000 + time_taken.microseconds // 1000,
                        'status': 'OK'})
    print('response sent [{0:s}]' \
          .format(datetime.now().strftime('%d/%b/%Y %H:%M:%S')))
    return response


@app.route('/regional-breakdown/', methods=['GET'])
def get_ladata():
    start = datetime.now()
    titles = request.args.getlist('title')
    region_type = request.args.get('region_type', 'la')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    # get leps
    leps = None

    try:
        if region_type == 'la':
            leps = {}
            lepquery = db.session.query(LAInLEP.la_id, LEP).join(LEP)

            for la_id, lep in lepquery:
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

        results['merged_titles_count'] = {}

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
                    if int(region_id) in leps:
                        results[key]['leps'] = leps[int(region_id)]
                    else:
                        results[key]['leps'] = []
                results[key]['count'] = 0
                results[key]['merged_titles'] = {}
                results[key]['location_quotient'] = {}

            results[key]['count'] += count

            # add title
            if job_title not in results[key]['merged_titles']:
                results[key]['merged_titles'][job_title] = 0
            results[key]['merged_titles'][job_title] += count

            if job_title not in results['merged_titles_count']:
                results['merged_titles_count'][job_title] = 0
            results['merged_titles_count'][job_title] += count

            total += count

        for region_id, region_code, region_name, job_title, count in q:
            key = region_code
            if job_title is None:
                job_title = "unknown"

            if (job_title not in results[key]['location_quotient'] and
                    results[key]['count'] != 0 and
                    results['merged_titles_count'][job_title] != 0 and
                    total != 0):
                results[key]['location_quotient'][job_title] = \
                    (results[key]['merged_titles'][job_title] / results[key]['count']) / \
                    (results['merged_titles_count'][job_title] / total)

        del results['merged_titles_count']

    try:
        # parameter check for Quarterly Data which will use Fact Tables ReportFactJobs due to performance issues
        qtr = db.session.query(ReportDimDatePeriod.period_name).filter(ReportDimDatePeriod.start_date == \
                                                                       datetime.strptime(start_date, '%Y-%m-%d')) \
            .filter(ReportDimDatePeriod.end_date == datetime.strptime(end_date, '%Y-%m-%d')).all()

        if not qtr:
            build_results(get_breakdown_for_source(ADZJob, titles, region_type, None))
            build_results(get_breakdown_for_source(INJob, titles, region_type, None))
        else:
            build_results(get_breakdown_for_source(ReportFactJobs, titles, region_type, qtr[0]))

    except SQLAlchemyError as e:
        return jsonify({
            'error': 'Database error',
            'exception': repr(e) if app.debug else None
        }), 500

    end = datetime.now()
    time_taken = end - start
    response = jsonify({'results': results,
                        'total': total,
                        'query_time': time_taken.seconds * 1000 + time_taken.microseconds // 1000,
                        'status': 'OK'})
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
            .filter(jobstable.language == 'en')

        q = apply_common_filters(q, jobstable, "skills")

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
                        'query_time': time_taken.seconds * 1000 + time_taken.microseconds // 1000,
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
            date_cols = (
            func.floor((func.extract('month', table.created) - 1) / 3) + 1, func.extract('year', table.created))
        else:
            null_column = literal_column("NULL")
            date_cols = (null_column, null_column)

        q = db.session.query(table.merged_title, table.salary_period, count_col, func.min(table.salary_min),
                             func.max(table.salary_max), func.avg(table.salary_min), func.avg(table.salary_max),
                             *date_cols)

        # filters
        q = apply_common_filters(q, table, "salaries")

        if region:
            if region_type == 'la':
                q = q.join(LA)
            elif region_type == 'lep':
                q = q.join(LAInLEP, table.la_id == LAInLEP.la_id) \
                    .join(LEP)

            region_field = get_region_field(table, region_type, code=True)
            if region != '*':
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
                if (res['merged_title'], res['period'], res['year'], res['month_quarter']) == (
                title, period, year, month):
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
    response = jsonify({'results': results,
                        'total': total,
                        'query_time': time_taken.seconds * 1000 + time_taken.microseconds // 1000,
                        'status': 'OK'})
    print('response sent [{0:s}]' \
          .format(datetime.now().strftime('%d/%b/%Y %H:%M:%S')))
    return response


@app.route('/all-salaries/', methods=['GET'])
def get_all_salaries():
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

    def build_results(table, adzOrIn):
        nonlocal total
        nonlocal results
        nonlocal period

        if adzOrIn == 0:
            q = db.session.query(table.adz_salary_min, table.adz_salary_max, table.salary_period)
        else:
            q = db.session.query(table.salary_min, table.salary_max, table.salary_period)

        # filters
        q = apply_common_filters(q, table, "all_salaries")
        q = q.filter(table.salary_period == 'year')

        if region:
            if region_type == 'la':
                q = q.join(LA)
            elif region_type == 'lep':
                q = q.join(LAInLEP, table.la_id == LAInLEP.la_id) \
                    .join(LEP)

            region_field = get_region_field(table, region_type, code=True)
            if region !='*':
                q = q.filter(region_field == region)

        if titles:
            if len(titles) == 1 and titles[0] == 'unknown':
                q = q.filter(table.parsed_title.is_(None))
            else:
                q = q.filter(table.parsed_title.in_(titles))

        if period:
            q = q.filter(table.salary_period == period)

        # q = q.group_by(table.merged_title, table.salary_period)

        # if group_period:
        #    q = q.group_by(*date_cols)

        # format results
        if len(results) == 0:
            results.append({
                '<10k': 0,
                '20k': 0,
                '30k': 0,
                '40k': 0,
                '50k': 0,
                '60k': 0,
                '>70k': 0
            })
        for salary_min, salary_max, period in q:

            # all null - no data
            if salary_min is None and salary_max is None:
                continue
            if period != 'year':
                continue

            salary_measured = 0

            if salary_min is not None and salary_max is None:
                salary_measured = salary_min
            elif salary_min is None and salary_max is not None:
                salary_measured = salary_max
            else:
                salary_measured = (salary_max + salary_min) / 2

            if salary_measured < 10000:
                results[0]['<10k'] += 1
            elif salary_measured <20000:
                results[0]['20k'] += 1
            elif salary_measured <30000:
                results[0]['30k'] += 1
            elif salary_measured < 40000:
                results[0]['40k'] += 1
            elif salary_measured < 50000:
                results[0]['50k'] += 1
            elif salary_measured < 60000:
                results[0]['60k'] += 1
            else:
                results[0]['>70k']

    try:
        build_results(ADZJob, 0)
        build_results(INJob, 1)
    except SQLAlchemyError as e:
        return jsonify({
            'error': 'Database error',
            'exception': repr(e) if app.debug else None
        }), 500

    end = datetime.now()
    time_taken = end - start
    response = jsonify({'results': results,
                        'total': total,
                        'query_time': time_taken.seconds * 1000 + time_taken.microseconds // 1000,
                        'status': 'OK'})
    print('response sent [{0:s}]' \
          .format(datetime.now().strftime('%d/%b/%Y %H:%M:%S')))
    return response


@app.route('/all-salaries-by-skills/', methods=['GET'])
def get_all_salaries_by_skills():
    start = datetime.now()
    skills = request.args.getlist('skill')
    region_type = request.args.get('region_type', 'la')
    region = request.args.get('region')
    group_period = request.args.get('group_period')
    provided_and_or = request.args.get('and_or')

    if group_period is not None and group_period not in ['month', 'quarter']:
        return jsonify({'error': 'Invalid group_period. Valid values: month, quarter'}), 400

    # build results
    results = []
    total = 0

    def build_results(table, andOr):
        nonlocal total
        nonlocal results

        q = db.session.query(table.salary_min, table.salary_max)

        # filters
        q = apply_common_filters(q, table, "all_salaries_by_skill")

        if region and region != '*':
            if region_type == 'la':
                q = q.join(LA)
            elif region_type == 'lep':
                q = q.join(LAInLEP, table.la_id == LAInLEP.la_id) \
                    .join(LEP)

            region_field = get_region_field(table, region_type, code=True)
            q = q.filter(region_field == region)

        if skills:
            if len(skills) == 1 and skills[0] == 'unknown':
                q = q.filter(table.skill.is_(None))
            else:
                q = q.filter(func.lower(table.skill).in_(skills))

        q = q.group_by(table.id, table.salary_max, table.salary_min)
        if andOr == 'and':
            q = q.having(func.count() >= len(skills))

        # format results
        if len(results) == 0:
            results.append({
                '<10k': 0,
                '20k': 0,
                '30k': 0,
                '40k': 0,
                '50k': 0,
                '60k': 0,
                '>70k': 0
            })
        for salary_min, salary_max in q:

            # all null - no data
            if salary_min is None and salary_max is None:
                continue

            salary_measured = 0

            if salary_min is not None and salary_max is None:
                salary_measured = salary_min
            elif salary_min is None and salary_max is not None:
                salary_measured = salary_max
            else:
                salary_measured = (salary_max + salary_min) / 2

            if salary_measured < 10000:
                results[0]['<10k'] += 1
            elif salary_measured <20000:
                results[0]['20k'] += 1
            elif salary_measured <30000:
                results[0]['30k'] += 1
            elif salary_measured < 40000:
                results[0]['40k'] += 1
            elif salary_measured < 50000:
                results[0]['50k'] += 1
            elif salary_measured < 60000:
                results[0]['60k'] += 1
            else:
                results[0]['>70k']
    try:
        build_results(ReportJobSkill, provided_and_or)
    except SQLAlchemyError as e:
        return jsonify({
            'error': 'Database error',
            'exception': repr(e) if app.debug else None
        }), 500

    end = datetime.now()
    time_taken = end - start
    response = jsonify({'results': results,
                        'total': total,
                        'query_time': time_taken.seconds * 1000 + time_taken.microseconds // 1000,
                        'status': 'OK'})
    print('response sent [{0:s}]' \
          .format(datetime.now().strftime('%d/%b/%Y %H:%M:%S')))
    return response


@app.route('/top-advertisers/', methods=['GET'])
def get_top_advertisers():
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

        q = db.session.query(table.company, table.salary_period, count_col)

        # filters
        q = apply_common_filters(q, table, "top_advertisers")

        if region:
            if region_type == 'la':
                q = q.join(LA)
            elif region_type == 'lep':
                q = q.join(LAInLEP, table.la_id == LAInLEP.la_id) \
                    .join(LEP)

            region_field = get_region_field(table, region_type, code=True)
            if region !='*':
                q = q.filter(region_field == region)

        if titles:
            if len(titles) == 1 and titles[0] == 'unknown':
                q = q.filter(table.parsed_title.is_(None))
            else:
                q = q.filter(table.parsed_title.in_(titles))

        if period:
            q = q.filter(table.salary_period == period)

        q = q.group_by(table.company, table.salary_period)
        q = q.order_by(desc(count_col))

        # format results
        for company, period, count in q:

            total += count

            # merge with existing
            found = False
            for res in results:
                if res['company'] == company:
                    res['count'] += count

                    found = True
                    break

            if found:
                continue

            results.append({
                'company': company,
                'count': count
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
    response = jsonify({'results': results,
                        'total': total,
                        'query_time': time_taken.seconds * 1000 + time_taken.microseconds // 1000,
                        'status': 'OK'})
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
            date_cols = (
            func.floor((func.extract('month', table.created) - 1) / 3) + 1, func.extract('year', table.created))
        else:
            date_cols = (null_column, null_column)

        title_col = table.merged_title if titles else null_column

        q = db.session.query(title_col, count_col, *date_cols)

        # filters
        q = apply_common_filters(q, table, "history")

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
    response = jsonify({'results': results,
                        'total': total,
                        'query_time': time_taken.seconds * 1000 + time_taken.microseconds // 1000,
                        'status': 'OK'})
    print('response sent [{0:s}]' \
          .format(datetime.now().strftime('%d/%b/%Y %H:%M:%S')))
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8081, debug=True)
    # http://127.0.0.1:8081/
    logging.getLogger('flask_cors').level = logging.DEBUG
