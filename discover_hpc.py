#!/usr/bin/env python3
"""Discover HPC conferences and workshops from WikiCFP search results.

Usage:
    python3 discover_hpc.py           # discover and write hpc_extra.json
    python3 discover_hpc.py --dry-run # print what would be written
"""

import json
import time
import re
import datetime
from pathlib import Path

import requests
import bs4

from scrape_locations import fetch_soup, location_to_continent

_HPC_CATEGORIES = [
    'HPC',
    'parallel+computing',
    'supercomputing',
    'high+performance+computing',
    'distributed+computing',
    'cluster+computing',
]

_HPC_KEYWORDS = [
    'HPC', 'supercomputing', 'parallel computing', 'exascale', 'cluster computing',
    'many-task', 'MPI', 'OpenMP', 'PGAS', 'GPU computing', 'heterogeneous computing',
    'high performance computing', 'parallel processing',
]

_HPC_TITLE_KEYWORDS = [
    'hpc', 'supercomputing', 'exascale', 'parallel', 'cluster', 'mpi', 'openmp',
    'many-task', 'pgas', 'gpu', 'heterogeneous', 'high performance',
    'risc-v', 'container', 'virtualization', 'workflow', 'task-based',
    'in situ', 'visualization', 'i/o', 'storage', 'scalable', 'distributed',
    'grid computing', 'cloud computing', 'edge computing', 'fog computing',
]


def is_hpc_relevant(title: str) -> bool:
    title_lower = title.lower()
    return any(kw in title_lower for kw in _HPC_TITLE_KEYWORDS)


def parse_search_results(soup: bs4.BeautifulSoup) -> list[dict]:
    results = []
    table = soup.find('table', cellpadding='2', cellspacing='1')
    if not table:
        for candidate in soup.find_all('table'):
            if candidate.find('a', href=re.compile(r'/cfp/servlet/event\.showcfp\?eventid=\d+')):
                table = candidate
                break
    if not table:
        return results

    rows = list(table.find_all('tr'))
    i = 0
    while i < len(rows):
        row = rows[i]
        link_a = row.find('a', href=re.compile(r'/cfp/servlet/event\.showcfp\?eventid=\d+'))
        if not link_a:
            i += 1
            continue

        link_text = link_a.get_text(strip=True)
        parts = link_text.rsplit(' ', 1)
        if len(parts) != 2:
            i += 1
            continue
        acronym, year_str = parts
        try:
            year = int(year_str)
        except ValueError:
            i += 1
            continue

        title_td = row.find('td', colspan='3')
        if not title_td:
            title_td = row.find_all('td')[-1] if row.find_all('td') else None
        title = title_td.get_text(strip=True) if title_td else ''

        href = str(link_a['href'])
        m = re.search(r'eventid=(\d+)', href)
        if not m:
            i += 1
            continue
        event_id = int(m.group(1))

        if i + 1 >= len(rows):
            i += 1
            continue
        detail_row = rows[i + 1]
        detail_tds = detail_row.find_all('td')
        when = detail_tds[0].get_text(strip=True) if len(detail_tds) > 0 else ''
        where = detail_tds[1].get_text(strip=True) if len(detail_tds) > 1 else ''
        deadline = detail_tds[2].get_text(strip=True) if len(detail_tds) > 2 else ''

        results.append({
            'acronym': acronym,
            'year': year,
            'title': title,
            'event_id': event_id,
            'when': when,
            'where': where,
            'deadline': deadline,
        })
        i += 2

    return results


def parse_cfp_dates(soup: bs4.BeautifulSoup) -> tuple[dict, dict]:
    dates: dict = {
        'abstract': None,
        'submission': None,
        'notification': None,
        'camera_ready': None,
        'start': None,
        'end': None,
        'link': None,
    }
    orig: dict = {
        'abstract': None,
        'submission': None,
        'notification': None,
        'camera_ready': None,
        'start': None,
        'end': None,
    }

    metadata = {}
    for tag in soup.find_all(lambda t: any(str(a).startswith('xmlns:') for a in t.attrs)):
        for attr in tag.attrs:
            if not str(attr).startswith('xmlns:'):
                continue
            pfx = str(attr)[len('xmlns:'):] + ':'
            attr_val = tag[attr]
            if isinstance(attr_val, list):
                attr_val = ' '.join(attr_val)
            if 'purl.org/dc/' in str(attr_val):
                for child in tag.find_all(property=lambda v: isinstance(v, str) and v.startswith(pfx)):
                    key = child['property'][len(pfx):]
                    val = child.get('content', child.get_text(strip=True))
                    metadata[key] = val
            elif 'rdf.data-vocabulary.org' in str(attr_val):
                children = {}
                for child in tag.find_all(property=lambda v: isinstance(v, str) and v.startswith(pfx)):
                    key = child['property'][len(pfx):]
                    val = child.get('content', child.get_text(strip=True))
                    children[key] = val

                if children.keys() == {'summary', 'startDate'}:
                    # This is a pair of tags that contain just a date, use summary value as key
                    metadata[children['summary']] = children['startDate']
                elif children.get('eventType') == 'Conference':
                    # Conference event tags with startDate/endDate
                    for key, val in children.items():
                        if key not in metadata and key.endswith('Date'):
                            metadata[key] = val

    _date_map = {
        'abstract': 'Abstract Registration Due',
        'submission': 'Submission Deadline',
        'notification': 'Notification Due',
        'camera_ready': 'Final Version Due',
    }
    for key, name in _date_map.items():
        val = metadata.get(name)
        if val:
            try:
                dt = datetime.datetime.strptime(val, '%Y-%m-%dT%H:%M:%S')
                dates[key] = dt.strftime('%Y%m%d')
                orig[key] = True
            except ValueError:
                pass

    for key in ['start', 'end']:
        val = metadata.get(f'{key}Date')
        if val:
            try:
                dt = datetime.datetime.strptime(val, '%Y-%m-%dT%H:%M:%S')
                dates[key] = dt.strftime('%Y%m%d')
                orig[key] = True
            except ValueError:
                pass

    if metadata.get('source'):
        dates['link'] = metadata['source'].strip()

    return dates, orig


def discover_from_category(category: str, max_pages: int = 5) -> list[dict]:
    all_results = []
    for page in range(1, max_pages + 1):
        url = f'http://www.wikicfp.com/cfp/call?conference={category}&page={page}'
        print(f"Category {category!r} page {page} ...")
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            soup = bs4.BeautifulSoup(r.text, 'lxml')
            results = parse_search_results(soup)
            if not results:
                break
            all_results.extend(results)
            time.sleep(1.0)
        except Exception as e:
            print(f"  ERROR: {e}")
            break
    return all_results


def discover_hpc_conferences(keywords: list[str], years: list[int], categories: list[str] | None = None) -> list[dict]:
    all_results = []
    seen = set()
    now_year = datetime.datetime.now().year

    for cat in (categories or []):
        for res in discover_from_category(cat):
            key = (res['acronym'], res['year'])
            if key in seen:
                continue
            if not is_hpc_relevant(res['title']):
                continue
            seen.add(key)
            all_results.append(res)

    for year in years:
        for keyword in keywords:
            url = 'http://www.wikicfp.com/cfp/servlet/tool.search'
            if year == now_year:
                yparam = 't'
            elif year == now_year + 1:
                yparam = 'n'
            elif year >= now_year:
                yparam = 'f'
            else:
                yparam = str(year)
            params = {'q': keyword, 'year': yparam}

            print(f"Searching: {keyword!r} year={year} ...")
            try:
                r = requests.get(url, params=params, timeout=30)
                r.raise_for_status()
                soup = bs4.BeautifulSoup(r.text, 'lxml')
                results = parse_search_results(soup)
                for res in results:
                    key = (res['acronym'], res['year'])
                    if key in seen:
                        continue
                    if not is_hpc_relevant(res['title']):
                        continue
                    seen.add(key)
                    all_results.append(res)
                time.sleep(1.0)
            except Exception as e:
                print(f"  ERROR: {e}")

    return all_results


def fetch_cfp_details(conf: dict) -> dict:
    url = f"http://www.wikicfp.com/cfp/servlet/event.showcfp?eventid={conf['event_id']}"
    try:
        soup = fetch_soup(url, delay=0.8)
        dates, orig = parse_cfp_dates(soup)
        conf['dates'] = dates
        conf['orig'] = orig
        print(f"  {conf['acronym']} {conf['year']}: abstract={dates['abstract']} sub={dates['submission']} notif={dates['notification']} cam={dates['camera_ready']} start={dates['start']} end={dates['end']}")
    except Exception as e:
        print(f"  ERROR fetching {conf['acronym']} {conf['year']}: {e}")
    return conf


def load_cfp_acronyms() -> set[str]:
    try:
        with open('cfp.json') as f:
            cfp = json.load(f)
        return {row[cfp['columns'].index('Acronym')] for row in cfp.get('data', [])}
    except Exception:
        return set()


def build_hpc_extra_json(conferences: list[dict]) -> list[list]:
    output = []
    existing = load_cfp_acronyms()

    # Group by cleaned acronym to deduplicate
    by_acronym: dict[str, dict] = {}
    for conf in conferences:
        acronym = conf['acronym'].strip()
        if not acronym or acronym in existing:
            continue
        d = conf.get('dates', {})
        if not d:
            continue
        if not d.get('start') and not d.get('end') and not d.get('submission'):
            continue
        # Keep the one with the most dates
        prev = by_acronym.get(acronym)
        if prev is None or sum(1 for v in d.values() if v) > sum(1 for v in prev.get('dates', {}).values() if v):
            by_acronym[acronym] = conf

    for conf in by_acronym.values():
        d = conf['dates']
        years_data = []
        for offset in [0, 1]:
            shifted = {}
            for key in ['abstract', 'submission', 'notification', 'camera_ready', 'start', 'end']:
                val = d.get(key)
                if val:
                    try:
                        dt = datetime.datetime.strptime(val, '%Y%m%d')
                        shifted[key] = (dt.replace(year=dt.year + offset)).strftime('%Y%m%d')
                    except ValueError:
                        shifted[key] = val
                else:
                    shifted[key] = None

            is_orig = offset == 0
            years_data.append([
                shifted.get('abstract'),
                shifted.get('submission'),
                shifted.get('notification'),
                shifted.get('camera_ready'),
                shifted.get('start'),
                shifted.get('end'),
                is_orig if d.get('abstract') else None,
                is_orig if d.get('submission') else None,
                is_orig if d.get('notification') else None,
                is_orig if d.get('camera_ready') else None,
                is_orig if d.get('start') else None,
                is_orig if d.get('end') else None,
                d.get('link') or '(missing)',
                None,
            ])

        row = [
            conf['acronym'].strip(),
            conf['title'],
            [None],
            ['Custom'],
            'Distributed computing and systems software',
        ]
        row.extend(years_data)
        output.append(row)

    return output


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--keywords', nargs='+', default=_HPC_KEYWORDS)
    parser.add_argument('--categories', nargs='+', default=_HPC_CATEGORIES)
    parser.add_argument('--years', nargs='+', type=int, default=[2026, 2027])
    parser.add_argument('--min-year', type=int, default=2026)
    args = parser.parse_args()

    conferences = discover_hpc_conferences(args.keywords, args.years, args.categories)
    print(f"\nFound {len(conferences)} HPC-relevant conferences before year filter")

    conferences = [c for c in conferences if c['year'] >= args.min_year]
    print(f"After min-year filter: {len(conferences)}")

    for conf in conferences:
        fetch_cfp_details(conf)

    hpc_data = build_hpc_extra_json(conferences)
    print(f"\nBuilt {len(hpc_data)} entries for hpc_extra.json")

    if args.dry_run:
        print(json.dumps(hpc_data, indent=2))
    else:
        out_path = Path('hpc_extra.json')
        with open(out_path, 'w') as f:
            json.dump(hpc_data, f, indent=2)
        print(f"Wrote {out_path}")


if __name__ == '__main__':
    main()
