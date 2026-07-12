"""Statistics helpers for the /learn/ pages, trends annotations and the
weekly exercise generator.

Conventions match what journalism students will get in R with the tidyverse:
- mean/median/mode come from the `statistics` stdlib module, which agrees
  with R's mean() and median() for these data
- stdev() is the SAMPLE standard deviation (n-1 denominator), the same as
  R's sd() -- not the population SD
- published numbers are rounded to 1 decimal place with round(), which
  rounds half to even, the same as R's round()
"""
from collections import defaultdict
from datetime import datetime, date, timedelta
import statistics

# Rows earlier than this are scraping artifacts (a handful of cases dated
# 1986/1999); meaningful coverage starts in 2015.
DATA_START = '2015-01-01'


def parse_dt(r):
    s = r.get('CASE_DATE') or r.get('REPORT_DATE') or ''
    try:
        return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
    except Exception:
        return None


# ── averages, spelled out so the exercise generator and labs agree ──────

def mean(xs):
    return statistics.fmean(xs) if xs else 0.0


def median(xs):
    return statistics.median(xs) if xs else 0.0


def mode(xs):
    return statistics.mode(xs) if xs else 0


def stdev(xs):
    # sample SD (n-1), matching R's sd(); static/learn.js must use n-1 too
    return statistics.stdev(xs) if len(xs) > 1 else 0.0


def rolling_mean(xs, window):
    """Trailing rolling mean; positions before a full window are None."""
    out = [None] * len(xs)
    if window <= 0:
        return out
    running = 0
    for i, x in enumerate(xs):
        running += x
        if i >= window:
            running -= xs[i - window]
        if i >= window - 1:
            out[i] = running / window
    return out


# ── time series ─────────────────────────────────────────────────────────

def daily_series(valid_activities, start=DATA_START):
    """Zero-filled incidents-per-day counts from `start` through today.

    Rows outside [start, today] are dropped and counted so the labs can
    show students that real datasets need cleaning before averaging.
    """
    start_day = date.fromisoformat(start)
    end_day = date.today()
    counts = [0] * ((end_day - start_day).days + 1)
    dropped = 0
    for r in valid_activities:
        dt = parse_dt(r)
        if not dt:
            continue
        d = dt.date()
        if d < start_day or d > end_day:
            dropped += 1
            continue
        counts[(d - start_day).days] += 1
    return {'start': start, 'counts': counts, 'dropped_out_of_range': dropped}


def weekly_series(daily):
    """Sum the daily series into complete Monday-Sunday weeks.

    The trailing partial week is discarded -- comparing a 3-day week
    against full weeks is one of the traps the labs teach.
    """
    start_day = date.fromisoformat(daily['start'])
    counts = daily['counts']
    offset = (7 - start_day.weekday()) % 7  # days until the first Monday
    first_monday = start_day + timedelta(days=offset)
    weeks = []
    i = offset
    while i + 7 <= len(counts):
        weeks.append(sum(counts[i:i + 7]))
        i += 7
    last_start = first_monday + timedelta(days=7 * (len(weeks) - 1)) if weeks else first_monday
    return {
        'start_monday': first_monday.isoformat(),
        'counts': weeks,
        'last_week_start': last_start.isoformat(),
    }


def monthly_totals(valid_activities, start='2015-01'):
    """Total incidents per month. The current (partial) month is excluded
    so the cherry-picking lab never compares a full month to a fragment."""
    by_month = defaultdict(int)
    for r in valid_activities:
        dt = parse_dt(r)
        if dt:
            key = dt.strftime('%Y-%m')
            if key >= start:
                by_month[key] += 1
    current = date.today().strftime('%Y-%m')
    labels = [m for m in sorted(by_month) if m < current]
    return {'labels': labels, 'counts': [by_month[m] for m in labels]}


# ── enrollment denominator ──────────────────────────────────────────────

# University of Maryland, College Park total fall headcount (undergrad +
# graduate). Sources: IPEDS fall-enrollment surveys (unitid 163286) for
# 2015-2024; UMD IRPA Campus Counts (irpa.umd.edu) for 2025, where the
# federal release is still pending. Update the newest year each fall.
ENROLLMENT = {
    2015: 38140,
    2016: 39083,
    2017: 40521,
    2018: 41200,
    2019: 40743,
    2020: 40709,
    2021: 41272,
    2022: 40792,
    2023: 40813,
    2024: 41725,
    2025: 42290,
}


def enrollment_for(year):
    """Fall enrollment for `year`, falling back to the latest known year."""
    if year in ENROLLMENT:
        return ENROLLMENT[year], False
    latest = max(ENROLLMENT)
    return ENROLLMENT[latest], True


def per_1000_rates(valid_activities):
    """Yearly incident counts alongside incidents per 1,000 enrolled
    students -- the rates-vs-counts lesson."""
    year_counts = defaultdict(int)
    for r in valid_activities:
        dt = parse_dt(r)
        if dt and dt.year >= int(DATA_START[:4]):
            year_counts[dt.year] += 1
    current_year = date.today().year
    rows = []
    for y in sorted(year_counts):
        n = year_counts[y]
        enrolled, estimated = enrollment_for(y)
        rows.append({
            'year': y,
            'label': f'{y} (partial)' if y == current_year else str(y),
            'incidents': n,
            'enrollment': enrolled,
            'enrollment_estimated': estimated,
            'rate_per_1000': round(n / enrolled * 1000, 1),
            'partial': y == current_year,
        })
    return rows


# ── days from occurrence to report ──────────────────────────────────────

def days_to_report(valid_activities, min_n=30):
    """Median and mean calendar days between Date Occurred and Report Date
    by crime type.

    `data/updated-activities.csv` (disposition changes) is still empty, so
    true time-to-resolution isn't computable yet; upgrade this once that
    file accumulates history. The occurred-to-report gap still teaches the
    key lesson: a few long-delayed reports drag the mean up while the
    median stays put.
    """
    gaps = defaultdict(list)
    skipped_negative = 0
    for r in valid_activities:
        occurred, reported = None, None
        try:
            occurred = datetime.strptime(r.get('CASE_DATE') or '', '%Y-%m-%d %H:%M:%S')
            reported = datetime.strptime(r.get('REPORT_DATE') or '', '%Y-%m-%d %H:%M:%S')
        except Exception:
            continue
        gap = (reported.date() - occurred.date()).days
        if gap < 0:  # report predates occurrence: a data-entry error
            skipped_negative += 1
            continue
        ct = (r.get('Crime Type') or '').strip() or 'Unknown'
        gaps[ct].append(gap)

    rows = []
    for ct, xs in gaps.items():
        if len(xs) < min_n:
            continue
        rows.append({
            'crime_type': ct,
            'n': len(xs),
            'median_days': round(median(xs), 1),
            'mean_days': round(mean(xs), 1),
            'pct_delayed': round(sum(1 for g in xs if g >= 1) / len(xs) * 100, 1),
        })
    rows.sort(key=lambda x: x['mean_days'], reverse=True)
    return {'rows': rows, 'skipped_negative': skipped_negative}


# ── semester baseline for the exercise generator ────────────────────────

def semester_window(as_of):
    """(label, start_date) for the semester containing `as_of`, using the
    site's definitions (Spring Feb-Apr, Summer Jun-Aug, Fall Sep-Nov), or
    None during break months."""
    m = as_of.month
    if m in (2, 3, 4):
        return f'Spring {as_of.year}', date(as_of.year, 2, 1)
    if m in (6, 7, 8):
        return f'Summer {as_of.year}', date(as_of.year, 6, 1)
    if m in (9, 10, 11):
        return f'Fall {as_of.year}', date(as_of.year, 9, 1)
    return None


def baseline_daily_mean(daily, as_of):
    """Semester-to-date mean incidents per day; outside a semester, the
    past-90-days mean. Returns (label, mean)."""
    start_day = date.fromisoformat(daily['start'])
    counts = daily['counts']
    sem = semester_window(as_of)
    if sem:
        label, window_start = sem
        label = f'{label} to date'
    else:
        label, window_start = 'the past 90 days', as_of - timedelta(days=90)
    i0 = max(0, (window_start - start_day).days)
    i1 = (as_of - start_day).days  # exclude as_of itself (day may be partial)
    xs = counts[i0:i1]
    return label, (round(mean(xs), 1) if xs else 0.0)


# ── payload for the /learn/ pages ────────────────────────────────────────

def compute_learn_data(valid_activities):
    """Everything the client-side labs need, pre-aggregated (never raw
    rows): embedded as `window.LEARN` at freeze time."""
    daily = daily_series(valid_activities)
    return {
        'daily': daily,
        'weekly': weekly_series(daily),
        'monthly': monthly_totals(valid_activities),
        'generated_at': date.today().isoformat(),
    }
