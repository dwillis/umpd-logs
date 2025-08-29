import csv
from csv import reader
import requests
from bs4 import BeautifulSoup
from datetime import date
from pathlib import Path
import pandas as pd


def scrape_month(year, month):
    """Scrape data for a specific year and month."""
    url = f'https://umpd.umd.edu/statistics-reports/daily-crime-and-incident-logs/{year}/{month:02d}'
    response = requests.get(url, headers={'User-Agent': 'Rachel Logan'})
    response.raise_for_status()
    html = response.content
    soup = BeautifulSoup(html, features="html.parser")
    return soup


def parse_table(soup, row_list, first):
    """Parse the HTML table and append rows to row_list."""
    table = soup.find('table')
    if not table:
        return first

    rows = table.find_all('tr')
    for row_index in range(len(rows)):
        if first:
            first = False
            header_row = [cell.text.strip() for cell in rows[0].find_all('th')]
            header_row.append('LOCATION')
            row_list.append(header_row)
        elif row_index % 2 == 1:
            cell_list = [cell.text.strip() for cell in rows[row_index].find_all('td')]
        elif row_index > 0:
            cell_list.append(rows[row_index].find('td').text.strip())
            row_list.append(cell_list)
            cell_list = []
    return first


def process_and_save_data(row_list, data_dir):
    """Process scraped data and save to CSV files."""
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / 'all-police-activity.csv'

    if not path.is_file():
        with open(path, "w", newline="") as outfile:
            writer = csv.writer(outfile)
            writer.writerows(row_list)
        return

    with open(path, 'r') as prev_data_stream:
        csv_reader = reader(prev_data_stream)
        prev_data = list(csv_reader)

    everything = pd.DataFrame(row_list + prev_data)
    pd_all_data = everything.drop_duplicates(subset=0, keep='first')
    pd_all_data.to_csv(path, index=False, header=False)

    open_cases = everything[everything[4] != "CBE"]
    case_filter = open_cases.duplicated(subset=0, keep='last')
    rescrape_only_filter = open_cases.duplicated(keep='last')
    update_filter = [(tup[0] and not tup[1]) for tup in zip(case_filter, rescrape_only_filter)]

    new_all_data = everything[~everything.duplicated(subset=0, keep=False)]
    new_now_list = []
    this_year = date.today().year
    for row in new_all_data.itertuples():
        row_data = row[1:]
        date_str = row_data[2]
        if '/' in date_str:
            year_part = date_str.split('/')[2].split()[0]
            if (len(year_part) == 4 and int(year_part) >= (this_year - 1)) or \
               (len(year_part) == 2 and int(year_part) >= (this_year - 2001)):
                new_now_list.append(row_data)

    new_now_data = pd.DataFrame(new_now_list)
    if len(new_now_data) > 0:
        new_now_data.to_csv(data_dir / 'new_cases.csv', index=False, header=False, sep=";")
    else:
        pd.DataFrame().to_csv(data_dir / 'new_cases.csv', index=False, header=False)

    dupes = open_cases.iloc[update_filter]
    if len(dupes) > 0:
        dupes.to_csv(data_dir / 'updated-activities.csv', index=False, header=False)
    else:
        pd.DataFrame().to_csv(data_dir / 'updated-activities.csv', index=False, header=False, sep=";")


def main():
    """Main scraping function."""
    row_list = []
    this_year = date.today().year
    first = True
    data_dir = Path('./data')

    for year in range(this_year - 1, this_year + 1):
        for month in range(1, 13):
            try:
                soup = scrape_month(year, month)
                first = parse_table(soup, row_list, first)
            except Exception as e:
                print(f"Error scraping {year}-{month}: {e}")
                continue

    if row_list:
        process_and_save_data(row_list, data_dir)


if __name__ == "__main__":
    main()
    
