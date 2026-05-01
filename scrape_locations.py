#!/usr/bin/env python3
"""Scrape conference locations from WikiCFP pages linked in cfp.json,
convert city/country to continents, and write locations.json."""

import json
import time
import re
from pathlib import Path

import requests
import bs4


def fetch_soup(url: str, delay: float = 1.0) -> bs4.BeautifulSoup:
    time.sleep(delay)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return bs4.BeautifulSoup(r.text, 'lxml')


_COUNTRY_TO_CONTINENT = {
    'usa': 'North America', 'united states': 'North America', 'canada': 'North America',
    'mexico': 'North America',
    'brazil': 'South America', 'argentina': 'South America', 'chile': 'South America',
    'colombia': 'South America', 'peru': 'South America', 'uruguay': 'South America',
    'ecuador': 'South America', 'venezuela': 'South America',
    'uk': 'Europe', 'united kingdom': 'Europe', 'england': 'Europe', 'scotland': 'Europe',
    'germany': 'Europe', 'france': 'Europe', 'italy': 'Europe', 'spain': 'Europe',
    'netherlands': 'Europe', 'belgium': 'Europe', 'switzerland': 'Europe', 'austria': 'Europe',
    'sweden': 'Europe', 'norway': 'Europe', 'denmark': 'Europe', 'finland': 'Europe',
    'poland': 'Europe', 'czech republic': 'Europe', 'czechia': 'Europe', 'hungary': 'Europe',
    'romania': 'Europe', 'bulgaria': 'Europe', 'croatia': 'Europe', 'serbia': 'Europe',
    'greece': 'Europe', 'portugal': 'Europe', 'ireland': 'Europe', 'slovenia': 'Europe',
    'slovakia': 'Europe', 'estonia': 'Europe', 'latvia': 'Europe', 'lithuania': 'Europe',
    'ukraine': 'Europe', 'russia': 'Europe', 'belarus': 'Europe', 'moldova': 'Europe',
    'bosnia and herzegovina': 'Europe', 'montenegro': 'Europe', 'north macedonia': 'Europe',
    'albania': 'Europe', 'kosovo': 'Europe', 'iceland': 'Europe', 'malta': 'Europe',
    'cyprus': 'Europe', 'luxembourg': 'Europe', 'monaco': 'Europe', 'liechtenstein': 'Europe',
    'andorra': 'Europe', 'san marino': 'Europe', 'vatican': 'Europe',
    'india': 'Asia', 'china': 'Asia', 'japan': 'Asia', 'south korea': 'Asia', 'korea': 'Asia',
    'singapore': 'Asia', 'thailand': 'Asia', 'vietnam': 'Asia', 'malaysia': 'Asia',
    'indonesia': 'Asia', 'philippines': 'Asia', 'taiwan': 'Asia', 'hong kong': 'Asia',
    'macau': 'Asia', 'mongolia': 'Asia', 'nepal': 'Asia', 'bangladesh': 'Asia',
    'sri lanka': 'Asia', 'pakistan': 'Asia', 'myanmar': 'Asia', 'cambodia': 'Asia',
    'laos': 'Asia', 'brunei': 'Asia', 'bhutan': 'Asia', 'maldives': 'Asia',
    'turkey': 'Asia', 'israel': 'Asia', 'saudi arabia': 'Asia', 'uae': 'Asia',
    'united arab emirates': 'Asia', 'qatar': 'Asia', 'kuwait': 'Asia', 'bahrain': 'Asia',
    'oman': 'Asia', 'jordan': 'Asia', 'lebanon': 'Asia', 'syria': 'Asia', 'iraq': 'Asia',
    'iran': 'Asia', 'kazakhstan': 'Asia', 'uzbekistan': 'Asia', 'turkmenistan': 'Asia',
    'kyrgyzstan': 'Asia', 'tajikistan': 'Asia', 'afghanistan': 'Asia', 'azerbaijan': 'Asia',
    'armenia': 'Asia', 'georgia': 'Asia',
    'australia': 'Australia', 'new zealand': 'Australia', 'fiji': 'Australia',
    'papua new guinea': 'Australia', 'samoa': 'Australia', 'tonga': 'Australia',
    'vanuatu': 'Australia', 'solomon islands': 'Australia',
    'south africa': 'Africa', 'egypt': 'Africa', 'nigeria': 'Africa', 'kenya': 'Africa',
    'morocco': 'Africa', 'tunisia': 'Africa', 'algeria': 'Africa', 'libya': 'Africa',
    'ethiopia': 'Africa', 'ghana': 'Africa', 'uganda': 'Africa', 'tanzania': 'Africa',
    'zimbabwe': 'Africa', 'zambia': 'Africa', 'botswana': 'Africa', 'namibia': 'Africa',
    'mozambique': 'Africa', 'madagascar': 'Africa', 'mauritius': 'Africa',
    'senegal': 'Africa', 'ivory coast': 'Africa', 'cameroon': 'Africa', 'angola': 'Africa',
    'democratic republic of the congo': 'Africa', 'rwanda': 'Africa',
}


_CITY_TO_CONTINENT = {
    'tokyo': 'Asia', 'dublin': 'Europe', 'seoul': 'Asia', 'london': 'Europe',
    'philadelphia': 'North America', 'budapest': 'Europe', 'bologna': 'Europe',
    'san diego': 'North America', 'lisbon': 'Europe', 'melbourne': 'Australia',
    'shanghai': 'Asia', 'amsterdam': 'Europe', 'barcelona': 'Europe',
    'ningbo': 'Asia', 'mainz': 'Europe', 'freiburg': 'Europe',
    'nashville': 'North America', 'palermo': 'Europe', 'naples': 'Europe',
    'lisboa': 'Europe', 'rome': 'Europe', 'helsinki': 'Europe',
    'malaga': 'Europe', 'málaga': 'Europe', 'warsaw': 'Europe',
    'vienna': 'Europe', 'edinburgh': 'Europe', 'santa clara': 'North America',
    'islamabad': 'Asia', 'leiden': 'Europe', 'burgas': 'Europe',
    'marco island': 'North America', 'san jose': 'North America',
    'zhengzhou': 'Asia', 'maui': 'North America', 'sydney': 'Australia',
    'new york': 'North America', 'limassol': 'Europe', 'bremen': 'Europe',
    'nha trang': 'Asia', 'honolulu': 'North America', 'rochester': 'North America',
    'ottawa': 'North America', 'delft': 'Europe', 'rennes': 'Europe',
    'manchester': 'Europe', 'washington': 'North America', 'houston': 'North America',
    'ras al-khaimah': 'Asia', 'austin': 'North America', 'baltimore': 'North America',
    'tampere': 'Europe', 'pittsburgh': 'North America', 'tucson': 'North America',
    'raleigh': 'North America', 'settat': 'Africa', 'jaipur': 'Asia',
    'laguna hills': 'North America', 'paris': 'Europe', 'chengdu': 'Asia',
    'san francisco': 'North America', 'turin': 'Europe', 'torino': 'Europe',
    'clermont-ferrand': 'Europe', 'charlotte': 'North America', 'beijing': 'Asia',
    'florianópolis': 'South America', 'florianopolis': 'South America',
    'guatemala': 'North America', 'prague': 'Europe', 'hong kong': 'Asia',
    'lansing': 'North America', 'anaheim': 'North America',
    'albuquerque': 'North America', 'los angeles': 'North America',
    'trondheim': 'Europe', 'vaasa': 'Europe', 'cluj-napoca': 'Europe',
    'pisa': 'Europe', 'delhi': 'Asia', 'modena': 'Europe',
    'hradec': 'Europe', 'kraków': 'Europe', 'krakow': 'Europe',
    'columbus': 'North America', 'timisoara': 'Europe', 'guiyang': 'Asia',
    'montreal': 'North America', 'mexico city': 'North America',
    'siena': 'Europe', 'toronto': 'North America', 'athens': 'Europe',
    'puebla': 'North America', 'linköping': 'Europe',
}


def location_to_continent(location: str) -> str | None:
    if not location or location in {'N/A', 'TBA', '???'}:
        return None

    loc_lower = location.lower()
    for city, cont in _CITY_TO_CONTINENT.items():
        if city in loc_lower:
            return cont

    parts = [p.strip().lower() for p in location.split(',')]
    for part in reversed(parts):
        if part in _COUNTRY_TO_CONTINENT:
            return _COUNTRY_TO_CONTINENT[part]
        cleaned = re.sub(r'\s+republic$|\s+islands?$', '', part).strip()
        if cleaned in _COUNTRY_TO_CONTINENT:
            return _COUNTRY_TO_CONTINENT[cleaned]
        cleaned = re.sub(r'\s*\(.*\)', '', part).strip()
        if cleaned in _COUNTRY_TO_CONTINENT:
            return _COUNTRY_TO_CONTINENT[cleaned]

    for country, cont in _COUNTRY_TO_CONTINENT.items():
        if country in loc_lower:
            return cont

    return None


def parse_location_from_soup(soup: bs4.BeautifulSoup) -> str | None:
    for span in soup.find_all(property='v:locality'):
        if span.has_attr('content'):
            content = span['content']
            return content.strip() if isinstance(content, str) else content[0].strip()
        text = span.get_text(strip=True)
        if text:
            return text

    for th in soup.find_all('th'):
        if th.get_text(strip=True).lower() == 'where':
            td = th.find_next_sibling('td')
            if td:
                return td.get_text(strip=True)

    return None


def main():
    cfp_path = Path('cfp.json')
    if not cfp_path.exists():
        print("cfp.json not found in current directory")
        return

    with open(cfp_path, 'r') as f:
        cfp_data = json.load(f)

    columns = cfp_data['columns']
    cfp_columns = cfp_data['cfp_columns']
    data = cfp_data['data']

    conf_idx = columns.index('Acronym')
    cfp_link_idx = cfp_columns.index('CFP url')

    acronym_to_url: dict[str, str] = {}
    for row in data:
        acronym = row[conf_idx]
        if acronym in acronym_to_url:
            continue
        for cfp in row[len(columns):]:
            if not isinstance(cfp, list):
                continue
            url = cfp[cfp_link_idx]
            if url and url != '(missing)' and 'wikicfp.com' in url:
                acronym_to_url[acronym] = url
                break

    out_path = Path('locations.json')
    locations: dict[str, dict] = {}
    if out_path.exists():
        with open(out_path, 'r') as f:
            locations = json.load(f)
        print(f"Loaded {len(locations)} existing locations")

    hpc_path = Path('hpc_extra.json')
    if hpc_path.exists():
        with open(hpc_path, 'r') as f:
            hpc_data = json.load(f)
        for row in hpc_data:
            acronym = row[0]
            if acronym in acronym_to_url:
                continue
            for cfp in row[5:]:
                if not isinstance(cfp, list):
                    continue
                cfp_url = cfp[-1]
                if cfp_url and cfp_url != '(missing)' and 'wikicfp.com' in cfp_url:
                    acronym_to_url[acronym] = cfp_url
                    break

        hpc_without_url = []
        for row in hpc_data:
            acronym = row[0]
            if acronym not in acronym_to_url and acronym not in locations:
                hpc_without_url.append(acronym)

        if hpc_without_url:
            print(f"Searching WikiCFP for {len(hpc_without_url)} HPC conferences without URLs...")
            for acronym in hpc_without_url:
                try:
                    search_url = f"http://www.wikicfp.com/cfp/servlet/tool.search?q={acronym}&year=t"
                    soup = fetch_soup(search_url, delay=0.8)
                    link = soup.find('a', href=re.compile(r'/cfp/servlet/event\.showcfp\?eventid=\d+'))
                    if link:
                        href = link['href']
                        if isinstance(href, list):
                            href = href[0]
                        cfp_url = 'http://www.wikicfp.com' + href
                        acronym_to_url[acronym] = cfp_url
                except Exception:
                    pass

    print(f"Found {len(acronym_to_url)} conferences with WikiCFP URLs")

    errors: list[str] = []

    for idx, (acronym, url) in enumerate(acronym_to_url.items(), 1):
        if acronym in locations and locations[acronym].get('raw') and locations[acronym]['raw'] != '(unknown)':
            continue
        print(f"[{idx}/{len(acronym_to_url)}] Fetching {acronym} ...", end=' ', flush=True)
        try:
            soup = fetch_soup(url, delay=0.8)
            loc = parse_location_from_soup(soup)
            if loc:
                continent = location_to_continent(loc)
                locations[acronym] = {'raw': loc, 'continent': continent or '(unknown)'}
                print(f"-> {loc} ({continent or 'unmapped'})")
            else:
                errors.append(f"{acronym}: no location found")
                print("-> no location")
        except Exception as e:
            errors.append(f"{acronym}: {e}")
            print(f"-> ERROR: {e}")

    with open(out_path, 'w') as f:
        json.dump(locations, f, indent=2, sort_keys=True)

    print(f"\nWrote {len(locations)} locations to {out_path}")
    if errors:
        print(f"{len(errors)} errors:")
        for e in errors[:20]:
            print(f"  {e}")
        if len(errors) > 20:
            print(f"  ... and {len(errors) - 20} more")


if __name__ == '__main__':
    main()
