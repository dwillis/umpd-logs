import csv
from csv import reader
import requests
from bs4 import BeautifulSoup
from datetime import date
from pathlib import Path
import pandas as pd


def scrape_year(year):
    """Scrape data for a specific year."""
    url = f'https://umpd.umd.edu/statistics-reports/arrest-report-ledgers/{year}'
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
    cell_list = []  # Initialize cell_list
    for row_index in range(len(rows)):
        if first:
            first = False
            header_row = [cell.text.strip() for cell in rows[0].find_all('th')]
            header_row.append('DESCRIPTION')
            row_list.append(header_row)
        elif row_index % 2 == 1:
            cell_list = [cell.text.strip() for cell in rows[row_index].find_all('td')]
        else:
            if rows[row_index].find('td'):
                cell_list.append(rows[row_index].find('td').text.strip())
            row_list.append(cell_list)
            cell_list = []
    return first


def process_and_save_data(row_list, data_dir):
    """Process scraped data and save to CSV file."""
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / 'all-police-arrests.csv'

    def _is_blank_row(r):
        if not r or len(r) == 0:
            return True
        return all((str(c).strip() == '' for c in r))

    def _is_header_row(r):
        # detect header-like rows by checking for known header tokens
        if not r:
            return False
        joined = ' '.join((str(c).strip().upper() for c in r))
        # common header indicators
        if 'ARREST' in joined and 'UMPD' in joined:
            return True
        if 'ARREST NUMBER' in joined or 'ARRESTNUMBER' in joined:
            return True
        if 'UMPD CASE NUMBER' in joined or 'UMPD CASE' in joined:
            return True
        return False

    # If the file doesn't exist yet, write cleaned rows (remove blanks/header duplicates)
    if not path.is_file():
        cleaned = [r for r in row_list if not _is_blank_row(r)]
        # keep only a single header if present at the start
        # ensure header is first row
        header = None
        for r in cleaned:
            if _is_header_row(r):
                header = r
                break
        if header:
            # remove other header-like rows
            cleaned = [r for r in cleaned if not (_is_header_row(r) and r != header)]
            if cleaned[0] != header:
                cleaned.insert(0, header)

        with open(path, "w", newline="") as outfile:
            writer = csv.writer(outfile)
            writer.writerows(cleaned)
        return

    with open(path, 'r') as prev_data_stream:
        csv_reader = reader(prev_data_stream)
        prev_data = list(csv_reader)

    combined = row_list + prev_data

    # remove blank rows and duplicate header rows, keep the first header encountered
    header = None
    cleaned = []
    for r in combined:
        if _is_blank_row(r):
            continue
        if _is_header_row(r):
            if header is None:
                header = r
                cleaned.append(r)
            else:
                # skip additional header rows
                continue
        else:
            cleaned.append(r)

    # if no header was found but prev_data existed, try to treat the first row of prev_data as header
    if header is None and prev_data:
        possible = prev_data[0]
        if _is_header_row(possible):
            header = possible
        # if header still None, do nothing; rows will be written without a header

    everything = pd.DataFrame(cleaned)
    # drop duplicates by UMPD Case Number column (index 2 if present)
    if everything.shape[1] > 2:
        pd_all_data = everything.drop_duplicates(subset=2, keep='first')  # case number
    else:
        pd_all_data = everything.drop_duplicates(keep='first')
    pd_all_data.to_csv(path, index=False, header=False)


def main():
    """Main scraping function."""
    row_list = []
    this_year = date.today().year
    first = True
    data_dir = Path('./data')

    for year in range(this_year - 1, this_year + 1):
        try:
            soup = scrape_year(year)
            first = parse_table(soup, row_list, first)
        except Exception as e:
            print(f"Error scraping {year}: {e}")
            continue

    if row_list:
        process_and_save_data(row_list, data_dir)


if __name__ == "__main__":
    main()

