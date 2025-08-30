import csv
from flask import Flask
from flask import abort
from flask import render_template
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
        dt = _pd.to_datetime(val, errors='coerce', infer_datetime_format=True)
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

@app.route("/")
def index():
    template = 'index.html'
    
    arrest_list = get_arrest_csv()
    activity_list = get_activity_csv(arrest_list)
    
    # filter out accidental embedded header rows (some CSVs contain repeated header lines)
    def is_valid_row(r):
        case = (r.get('UMPD Case Number') or r.get('UMPD CASE NUMBER') or '').strip()
        if not case:
            return False
        # header-like values often contain 'UMPD' or 'CASE' in uppercase; treat those as invalid
        up = case.upper()
        if up.startswith('UMPD') or 'CASE' in up and not case[0].isdigit():
            return False
        return True

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

    # helper to extract crime type robustly
    def activity_type(r):
        for k in ('Crime Type', 'TYPE', 'Type'):
            v = r.get(k)
            if v and v.strip():
                return v.strip()
        return 'Unknown'

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
    return render_template(template,
                           activity_list=valid_activities,
                           arrest_list=arrest_list,
                           total_incidents=total_incidents,
                           most_common_type=most_common_type,
                           most_common_30=most_common_30)

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

if __name__ == '__main__':
    app.run(debug=True, use_reloader=True)
