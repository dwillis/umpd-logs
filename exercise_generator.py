"""Generate a weekly statistics exercise from the latest UMPD data.

Run by .github/workflows/exercises.yml every Monday, or by hand:

    python exercise_generator.py [--date YYYY-MM-DD] [--sha COMMIT] [--force]

Writes exactly one file, exercises/<monday-of-last-week>.md, built around
the most recent complete Monday-Sunday week. Data URLs are pinned to a
commit SHA so students' R output matches the numbers printed here.
"""
import argparse
import os
import subprocess
import sys
from datetime import date, datetime, timedelta

import learn_stats
from app import get_arrest_csv, get_activity_csv, is_valid_row

REPO_RAW = 'https://raw.githubusercontent.com/dwillis/umpd-logs'


def resolve_sha(cli_sha):
    if cli_sha:
        return cli_sha
    if os.environ.get('GITHUB_SHA'):
        return os.environ['GITHUB_SHA']
    try:
        return subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'], text=True).strip()
    except Exception:
        return 'master'  # unpinned fallback; numbers may drift as data updates


def week_slice(daily, week_start):
    start_day = date.fromisoformat(daily['start'])
    i0 = (week_start - start_day).days
    if i0 < 0 or i0 + 7 > len(daily['counts']):
        return None
    return daily['counts'][i0:i0 + 7]


# Question bank. Four are chosen each week, rotated by ISO week number so
# reruns on the same date are idempotent and consecutive weeks differ.
AVERAGE_QUESTIONS = [
    'The mean and median daily counts above are different. Which days of last week '
    'pulled the mean away from the median? Run the starter code, look at the daily '
    'counts, and explain in two sentences which number you would put in a story.',
    'If one day last week had seen a big spike — say 30 incidents — how would the '
    'mean have changed? How about the median? (You can test this in R by editing '
    'one value.) What does that tell you about which average to trust on skewed data?',
    'Write one sentence for a campus-news brief using the mean, and one using the '
    'median. Do they leave the reader with different impressions? Which is fairer?',
]

CHANGE_QUESTIONS = [
    'Using the percent change above: is this a real shift or ordinary week-to-week '
    'noise? The Learn page\'s spike detector uses mean ± 2 standard deviations of '
    'weekly counts as its "normal" band — compute that band in R (sd() on the weekly '
    'totals) and say whether last week falls outside it.',
    'The comparison above uses the same calendar week last year. Why is that fairer '
    'than comparing last week to the week before? Name one rhythm in campus life '
    'that would fool a week-over-week comparison.',
    'Rewrite the percent change above as a plain-language sentence a reader can '
    'check ("about X incidents a day, up from Y a year earlier"). Why might raw '
    'numbers serve readers better than percentages when counts are small?',
]

CRITIQUE_QUESTIONS = [
    'A press release claims "campus crime fell 30% since 2020." Using what you know '
    'about 2020 (check the yearly chart on the Trends page), explain why that '
    'baseline is misleading — and what baseline you would use instead.',
    'Find the crime type with the biggest percent change between the last two full '
    'months (group by month and `Crime Type` in R). Would you report it? Apply the '
    'small-number test: what are the raw counts behind the percentage?',
    'The Trends page compares semesters (Spring = Feb–Apr, Fall = Sep–Nov) instead '
    'of calendar quarters. What error does that choice avoid? What would a '
    'January-to-March "quarter" mix together on a university campus?',
]

R_TASKS = [
    'In R, compute the mean and median incidents per day for each of the last four '
    'complete weeks (hint: `floor_date(day, "week", week_start = 1)` then '
    '`group_by(week)`). Is last week unusual among them?',
    'In R, count last week\'s incidents by `Crime Type` and sort descending. What '
    'share of the week is the top type? (`mutate(share = n / sum(n))`.) Does the '
    '"most common incident" match what you\'d guess from campus-crime coverage?',
    'In R, compute incidents per day for last week and for the same week last year, '
    'then the percent change between their means. Confirm you get the number printed '
    'above (both use `round(x, 1)`).',
]


def pick(bank, iso_week):
    return bank[iso_week % len(bank)]


def build_markdown(run_date, sha):
    arrest_list = get_arrest_csv()
    activity_list = get_activity_csv(arrest_list)
    valid = [r for r in activity_list if is_valid_row(r)]
    daily = learn_stats.daily_series(valid)

    # last complete Mon-Sun week strictly before run_date
    this_monday = run_date - timedelta(days=run_date.weekday())
    week_start = this_monday - timedelta(days=7)
    week_end = week_start + timedelta(days=6)
    week = week_slice(daily, week_start)
    if week is None:
        sys.exit(f'No complete data for week starting {week_start}')

    total = sum(week)
    mean_day = round(learn_stats.mean(week), 1)
    median_day = round(learn_stats.median(week), 1)

    baseline_label, baseline_mean = learn_stats.baseline_daily_mean(daily, run_date)

    ly_start = week_start - timedelta(weeks=52)  # same ISO weekday alignment
    ly_week = week_slice(daily, ly_start)
    ly_total = sum(ly_week) if ly_week else None

    small_numbers = ly_total is None or total < 5 or ly_total < 5
    if ly_total and ly_total > 0:
        pct_change = round((total - ly_total) / ly_total * 100, 1)
    else:
        pct_change = None

    iso_week = week_start.isocalendar()[1]
    questions = [
        pick(AVERAGE_QUESTIONS, iso_week),
        pick(CHANGE_QUESTIONS, iso_week),
        pick(CRITIQUE_QUESTIONS, iso_week),
        pick(R_TASKS, iso_week),
    ]
    if small_numbers and pct_change is not None:
        questions.insert(1, (
            f'Careful: at least one of the two weeks being compared has fewer than 5 '
            f'incidents ({total} vs. {ly_total}). Explain in two sentences why the '
            f'{pct_change}% figure above should NOT appear in a story, and what you '
            f'would report instead.'))

    activity_url = f'{REPO_RAW}/{sha}/data/all-police-activity.csv'

    fmt = '%B %-d, %Y' if os.name != 'nt' else '%B %#d, %Y'
    week_label = f'{week_start.strftime(fmt)} – {week_end.strftime(fmt)}'

    if pct_change is None:
        change_line = '- Same week last year: no comparable data'
    else:
        arrow = 'up' if pct_change > 0 else 'down' if pct_change < 0 else 'flat at'
        change_line = (f'- Same week last year ({ly_start.strftime(fmt)} on): '
                       f'**{ly_total}** incidents — {arrow} '
                       f'**{abs(pct_change)}%**' + (
                           ' *(small numbers — see the questions)*' if small_numbers else ''))

    q_lines = '\n'.join(f'{i}. {q}' for i, q in enumerate(questions, 1))

    return week_start.isoformat(), f'''# UMPD Data Exercise — Week of {week_label}

*Generated {run_date.isoformat()}. The data URL below is pinned to commit `{sha[:9]}`, a
snapshot of the dataset as of this exercise — so your numbers should match these exactly.*

## This week's numbers

- Incidents last week (Mon–Sun): **{total}**
- Mean per day: **{mean_day}** &nbsp;&nbsp; Median per day: **{median_day}**
- Daily mean over {baseline_label}: **{baseline_mean}**
{change_line}

## Questions

{q_lines}

## Starter code (R)

```r
library(tidyverse)
library(lubridate)

activity <- read_csv("{activity_url}")

week_start <- ymd("{week_start.isoformat()}")

daily <- activity |>
  mutate(day = as_date(`Date Occurred`)) |>
  filter(day >= week_start, day < week_start + days(7)) |>
  count(day, name = "incidents") |>
  complete(day = seq(week_start, week_start + days(6), by = "day"),
           fill = list(incidents = 0))

daily |>
  summarize(total = sum(incidents),
            mean_per_day = round(mean(incidents), 1),
            median_per_day = median(incidents))
```

## Check your work

Your `summarize()` output should match "This week's numbers" above: total **{total}**,
mean **{mean_day}**, median **{median_day}**. All values here use `round(x, 1)`, which
rounds halves to the nearest even digit — the same rule as R's `round()`.

*Stuck on the concepts? The site's [Learn pages](../../learn/) walk through every
technique used here, with this same dataset.*
'''


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', help='run date YYYY-MM-DD (default: today)')
    ap.add_argument('--sha', help='commit SHA to pin data URLs to')
    ap.add_argument('--force', action='store_true', help='overwrite an existing file')
    args = ap.parse_args()

    run_date = datetime.strptime(args.date, '%Y-%m-%d').date() if args.date else date.today()
    sha = resolve_sha(args.sha)

    slug, text = build_markdown(run_date, sha)
    os.makedirs('exercises', exist_ok=True)
    path = os.path.join('exercises', f'{slug}.md')
    if os.path.exists(path) and not args.force:
        print(f'{path} already exists; use --force to regenerate')
        return
    with open(path, 'w') as f:
        f.write(text)
    print(f'Wrote {path}')


if __name__ == '__main__':
    main()
