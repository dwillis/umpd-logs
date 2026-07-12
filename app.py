import csv
import glob
import os
import re
from flask import Flask
from flask import abort
from flask import render_template

import learn_stats

app = Flask(__name__)
    
# module-level date parser (used by the views)
from datetime import datetime, timedelta
def parse_date_str(value):
    if not value:
        return ''
    val = value.replace('\u2013', '-').replace('\u2014', '-')
    val = val.replace(' - ', ' ')
    # try common explicit formats including seconds
    for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%y %H:%M", "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(val, fmt)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            continue
    # fallback to pandas flexible parser
    try:
        import pandas as _pd
        dt = _pd.to_datetime(val, errors='coerce', format='mixed')
        if _pd.notna(dt):
            return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        pass
    return ''
def get_arrest_csv():
    #should change this to get ALL files with start 'scraped-umd-police-arrest-log'
    csv_path = './data/all-police-arrests.csv'
    csv_file = open(csv_path, 'r')
    csv_obj = csv.DictReader(csv_file)
    csv_list = list(csv_obj)
    return csv_list
    
def get_activity_csv(arrest_list):
    #should change this to get ALL files with start 'scraped-umd-police-activity-log'
    csv_path = './data/all-police-activity.csv'
    csv_file = open(csv_path, 'r')
    csv_obj = csv.DictReader(csv_file)
    csv_list = list(csv_obj)

    arrest_cases = [arrest.get('UMPD Case Number') or arrest.get('UMPD CASE NUMBER') for arrest in arrest_list]

    # discover likely field names for occurred/report columns
    fnames = csv_obj.fieldnames or []
    occurred_field = None
    report_field = None
    occurred_candidates = ['Date Occurred', 'DateOccurred', 'Occurred', 'OCCURRED DATE TIMELOCATION', 'OCCURRED DATE TIME']
    report_candidates = ['Report Date', 'ReportDate', 'REPORT DATE TIME', 'REPORT DATE']
    for c in occurred_candidates:
        if c in fnames:
            occurred_field = c
            break
    for c in report_candidates:
        if c in fnames:
            report_field = c
            break
    # fallbacks to positional columns
    if not occurred_field and len(fnames) > 1:
        occurred_field = fnames[1]
    if not report_field and len(fnames) > 2:
        report_field = fnames[2]

    for activity in csv_list:
        activity['ARREST'] = "Yes" if (activity.get('UMPD Case Number') in arrest_cases) else "No"

        occurred = activity.get(occurred_field, '') if occurred_field else ''
        activity['CASE_DATE'] = parse_date_str(occurred)

        report = activity.get(report_field, '') if report_field else ''
        activity['REPORT_DATE'] = parse_date_str(report)
    return csv_list


def is_valid_row(r):
    case = (r.get('UMPD Case Number') or r.get('UMPD CASE NUMBER') or '').strip()
    if not case:
        return False
    up = case.upper()
    if up.startswith('UMPD') or ('CASE' in up and not case[0].isdigit()):
        return False
    return True


def activity_type(r):
    for k in ('Crime Type', 'TYPE', 'Type'):
        v = r.get(k)
        if v and v.strip():
            return v.strip()
    return 'Unknown'


def categorize_crime(crime_type):
    ct = (crime_type or '').upper()
    if any(k in ct for k in ('ASSAULT', 'ROBBERY', 'RAPE', 'SEXUAL ASSAULT', 'WEAPON', 'ARMED',
                              'CARJACKING', 'HOMICIDE', 'CUTTING', 'STABBING', 'RECKLESS')):
        return 'violent'
    if any(k in ct for k in ('THEFT', 'BURGLARY', 'VANDALISM', 'TRESPASS', 'BREAKING', 'STOLEN',
                              'DAMAGE', 'ARSON', 'FRAUD', 'EMBEZZLEMENT')):
        return 'property'
    if any(k in ct for k in ('CDS', 'DRUG', 'ALCOHOL', 'MARIJUANA', 'INTOXICATED', 'DUI', 'DWI')):
        return 'drugs'
    if any(k in ct for k in ('TRAFFIC', 'PARKING', 'VEHICLE', 'HIT AND RUN',
                              'ACCIDENT', 'HAZARDOUS', 'PEDESTRIAN')):
        return 'traffic'
    if any(k in ct for k in ('INJURED', 'SICK', 'EMERGENCY', 'WELFARE', 'DEATH',
                              'OVERDOSE', 'SUICIDE', 'FIRE', 'BOMB', 'MISSING')):
        return 'medical'
    if any(k in ct for k in ('HARASS', 'STALK', 'INDECENT', 'PEEPING', 'SEX OFFENSE',
                              'TELEPHONE', 'EMAIL', 'DOMESTIC', 'TITLE IX', 'PORNOGRAPHY',
                              'OBSCENE', 'EXTORTION', 'HATE')):
        return 'harassment'
    return 'other'


def compute_analytics(valid_activities, arrest_list):
    from collections import defaultdict
    now = datetime.now()

    def parse_dt(r):
        s = r.get('CASE_DATE') or r.get('REPORT_DATE') or ''
        try:
            return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
        except Exception:
            return None

    # yoy leads: current month MTD this year vs same MTD range last year
    current_month, current_year = now.month, now.year
    this_yr, last_yr = defaultdict(int), defaultdict(int)
    for r in valid_activities:
        dt = parse_dt(r)
        if not dt or dt.month != current_month or dt.day > now.day:
            continue
        ct = activity_type(r)
        if dt.year == current_year:
            this_yr[ct] += 1
        elif dt.year == current_year - 1:
            last_yr[ct] += 1

    yoy_leads = []
    for ct in set(this_yr) | set(last_yr):
        n_this, n_last = this_yr.get(ct, 0), last_yr.get(ct, 0)
        if n_this < 5 or n_last < 5:
            continue
        pct = round((n_this - n_last) / n_last * 100)
        if abs(pct) < 10:
            continue
        yoy_leads.append({'crime_type': ct, 'this_year': n_this, 'last_year': n_last,
                          'pct_change': pct, 'direction': 'up' if pct > 0 else 'down'})
    yoy_leads.sort(key=lambda x: abs(x['pct_change']), reverse=True)
    yoy_leads = yoy_leads[:2]

    # pending 90+ days but within 2 years (older cases are likely data artifacts)
    cutoff_90 = now - timedelta(days=90)
    cutoff_2yr = now - timedelta(days=730)
    pending_statuses = {'investigation pending', 'active/pending', 'pending'}
    pending_old = []
    for r in valid_activities:
        disp = (r.get('Disposition') or '').strip().lower()
        if disp not in pending_statuses:
            continue
        s = r.get('REPORT_DATE') or ''
        try:
            dt = datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
        except Exception:
            continue
        if cutoff_2yr <= dt < cutoff_90:
            pending_old.append({
                'case': r.get('UMPD Case Number', ''),
                'crime_type': activity_type(r),
                'days_old': (now - dt).days,
                'report_date': dt.strftime('%Y-%m-%d'),
            })
    pending_old.sort(key=lambda x: x['days_old'], reverse=True)

    # lowest arrest rate (only crime types with >= 5 total incidents)
    type_total, type_arrests = defaultdict(int), defaultdict(int)
    for r in valid_activities:
        ct = activity_type(r)
        type_total[ct] += 1
        if r.get('ARREST') == 'Yes':
            type_arrests[ct] += 1
    arrest_rate_list = [
        {'crime_type': ct, 'rate': round(type_arrests.get(ct, 0) / total * 100, 1), 'total': total}
        for ct, total in type_total.items() if total >= 5
    ]
    arrest_rate_list.sort(key=lambda x: x['rate'])
    lowest_arrest_rate = arrest_rate_list[0] if arrest_rate_list else None

    # location clusters (last 60 days, 3+ incidents)
    cutoff_60 = now - timedelta(days=60)
    loc_incidents = defaultdict(list)
    for r in valid_activities:
        dt = parse_dt(r)
        if not dt or dt < cutoff_60:
            continue
        loc = (r.get('LOCATION') or '').strip()
        if len(loc) >= 5:
            loc_incidents[loc].append(activity_type(r))
    clusters = sorted(
        [{'location': loc, 'count': len(v), 'types': list(set(v))}
         for loc, v in loc_incidents.items() if len(v) >= 3],
        key=lambda x: x['count'], reverse=True
    )
    top_cluster = clusters[0] if clusters else None

    # heatmap[weekday 0=Mon][hour 0-23] = count, all-time
    heatmap = [[0] * 24 for _ in range(7)]
    for r in valid_activities:
        dt = parse_dt(r)
        if dt:
            heatmap[dt.weekday()][dt.hour] += 1

    # monthly counts by crime category
    monthly_counts = defaultdict(lambda: defaultdict(int))
    for r in valid_activities:
        dt = parse_dt(r)
        if dt:
            monthly_counts[dt.strftime('%Y-%m')][categorize_crime(activity_type(r))] += 1
    sorted_months = [m for m in sorted(monthly_counts) if m >= '2015-01']
    cats = ['violent', 'property', 'drugs', 'traffic', 'medical', 'harassment', 'other']
    monthly_data = {
        'labels': sorted_months,
        'series': {cat: [monthly_counts[m].get(cat, 0) for m in sorted_months] for cat in cats},
    }

    # semester data: Spring=Feb-Apr, Summer=Jun-Aug, Fall=Sep-Nov
    def get_semester(dt):
        m = dt.month
        if m in (2, 3, 4): return f'Spring {dt.year}'
        if m in (6, 7, 8): return f'Summer {dt.year}'
        if m in (9, 10, 11): return f'Fall {dt.year}'
        return None

    sem_counts = defaultdict(lambda: defaultdict(int))
    for r in valid_activities:
        dt = parse_dt(r)
        if not dt:
            continue
        sem = get_semester(dt)
        if sem:
            sem_counts[sem][categorize_crime(activity_type(r))] += 1
    sem_order = {'Spring': 0, 'Summer': 1, 'Fall': 2}
    sorted_sems = sorted(sem_counts,
                         key=lambda s: (int(s.split()[1]), sem_order.get(s.split()[0], 3)))
    last_4_sems = sorted_sems[-4:] if len(sorted_sems) >= 4 else sorted_sems
    semester_data = {
        'labels': last_4_sems,
        'series': {cat: [sem_counts[s].get(cat, 0) for s in last_4_sems] for cat in cats},
    }

    # year-over-year totals by crime category
    year_counts = defaultdict(lambda: defaultdict(int))
    for r in valid_activities:
        dt = parse_dt(r)
        if dt:
            year_counts[str(dt.year)][categorize_crime(activity_type(r))] += 1
    years = sorted(year_counts)
    yoy_totals = {
        'labels': years,
        'series': {cat: [year_counts[yr].get(cat, 0) for yr in years] for cat in cats},
    }

    return {
        'per_1000': learn_stats.per_1000_rates(valid_activities),
        'days_to_report': learn_stats.days_to_report(valid_activities),
        'yoy_leads': yoy_leads,
        'pending_count': len(pending_old),
        'oldest_pending': pending_old[0] if pending_old else None,
        'lowest_arrest_rate': lowest_arrest_rate,
        'top_cluster': top_cluster,
        'heatmap': heatmap,
        'monthly_data': monthly_data,
        'semester_data': semester_data,
        'yoy_totals': yoy_totals,
        'current_month_name': now.strftime('%B'),
        'current_year': current_year,
        'last_year': current_year - 1,
    }


@app.route("/")
def index():
    template = 'index.html'
    
    arrest_list = get_arrest_csv()
    activity_list = get_activity_csv(arrest_list)
    
    valid_activities = [r for r in activity_list if is_valid_row(r)]

    # sort valid activities so newest records appear first on initial render
    def _parse_activity_dt(r):
        s = r.get('CASE_DATE') or r.get('REPORT_DATE') or ''
        try:
            return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
        except Exception:
            return datetime(1900, 1, 1)

    valid_activities.sort(key=_parse_activity_dt, reverse=True)

    # total incidents
    total_incidents = len(valid_activities)

    from collections import Counter
    all_types = [activity_type(r) for r in valid_activities]
    overall_counter = Counter(all_types)
    most_common_type = overall_counter.most_common(1)[0][0] if overall_counter else 'N/A'

    # most common in last 30 days (based on CASE_DATE normalized value)
    cutoff = datetime.now() - timedelta(days=30)
    recent_types = []
    for r in valid_activities:
        ds = r.get('CASE_DATE') or ''
        if not ds:
            continue
        try:
            dt = datetime.strptime(ds, '%Y-%m-%d %H:%M:%S')
        except Exception:
            # if parse fails, skip
            continue
        if dt >= cutoff:
            recent_types.append(activity_type(r))
    recent_counter = Counter(recent_types)
    most_common_30 = recent_counter.most_common(1)[0][0] if recent_counter else 'N/A'

    # expose aggregates to template
    analytics = compute_analytics(valid_activities, arrest_list)
    return render_template(template,
                           activity_list=valid_activities,
                           arrest_list=arrest_list,
                           total_incidents=total_incidents,
                           most_common_type=most_common_type,
                           most_common_30=most_common_30,
                           analytics=analytics)

# @app.route('/<case_number>/')
# def detail(case_number):
    # template = 'detail.html'
    
    # arrest_list = get_arrest_csv()
    # activity_list = get_activity_csv(arrest_list)
    
    # for activity in activity_list:
        # #if (activity['DISPOSITION'] == "Arrest"):
        # arrest_matches = [arrest for arrest in arrest_list if arrest['UMPD CASE NUMBER'] == case_number]
        # if (len(arrest_matches) > 0):
            # return render_template(template, activity = activity, arrest = arrest_matches[0])
        # return render_template(template, activity = activity, arrest = None)
    # abort(404)


@app.route('/trends/')
def trends():
    arrest_list = get_arrest_csv()
    activity_list = get_activity_csv(arrest_list)
    valid_activities = [r for r in activity_list if is_valid_row(r)]
    analytics = compute_analytics(valid_activities, arrest_list)
    return render_template('trends.html', analytics=analytics)


def load_valid_activities():
    arrest_list = get_arrest_csv()
    activity_list = get_activity_csv(arrest_list)
    return [r for r in activity_list if is_valid_row(r)]


def _learn_page(template):
    learn = learn_stats.compute_learn_data(load_valid_activities())
    return render_template(template, learn=learn)


@app.route('/learn/')
def learn_index():
    return _learn_page('learn/index.html')


@app.route('/learn/averages/')
def learn_averages():
    return _learn_page('learn/averages.html')


@app.route('/learn/moving-averages/')
def learn_moving_averages():
    return _learn_page('learn/moving_averages.html')


@app.route('/learn/cherry-picking/')
def learn_cherry_picking():
    return _learn_page('learn/cherry_picking.html')


@app.route('/learn/spikes/')
def learn_spikes():
    return _learn_page('learn/spikes.html')


EXERCISES_DIR = './exercises'
EXERCISE_SLUG_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def list_exercises():
    """Exercise files as [{'slug', 'title'}], newest first."""
    items = []
    for path in sorted(glob.glob(os.path.join(EXERCISES_DIR, '*.md')), reverse=True):
        slug = os.path.splitext(os.path.basename(path))[0]
        if not EXERCISE_SLUG_RE.match(slug):
            continue
        title = slug
        with open(path) as f:
            for line in f:
                if line.startswith('# '):
                    title = line[2:].strip()
                    break
        items.append({'slug': slug, 'title': title})
    return items


@app.route('/exercises/')
def exercises_index():
    return render_template('exercises/index.html', exercises=list_exercises())


@app.route('/exercises/<slug>/')
def exercise_detail(slug):
    if not EXERCISE_SLUG_RE.match(slug):
        abort(404)
    path = os.path.join(EXERCISES_DIR, slug + '.md')
    if not os.path.exists(path):
        abort(404)
    import markdown as md
    with open(path) as f:
        text = f.read()
    body = md.markdown(text, extensions=['fenced_code', 'tables'])
    github_url = f'https://github.com/dwillis/umpd-logs/blob/master/exercises/{slug}.md'
    return render_template('exercises/detail.html', body=body, slug=slug,
                           github_url=github_url)


if __name__ == '__main__':
    app.run(debug=True, use_reloader=True)
