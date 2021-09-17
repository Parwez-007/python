from typing import Optional, Iterable, Dict, Tuple

from bays_common.persistence import DataFrameWithMeta
from bays.mtn.geography import *
from bays_common.meta import MetaData

import logging
import pandas as pd
import re
import requests

from bs4 import element, BeautifulSoup
from urllib.parse import urlencode

LOGGER = logging.getLogger(__name__)

LOCATION_URL = "https://autocomplete.indeed.com/api/v0/suggestions/location?country=GB&language=en&count=1&%s"
URL = "https://www.indeed.co.uk/jobs?q=&l=%s&radius=%d"

PAT_FILTER_TEXT = re.compile("(.*)\\s+\\(([0-9]+)\\)", re.MULTILINE)

RADII = (0, 5, 10, 15, 25)

FILTERS = {
    "occupationDescription": ["taxo1"],
    "company": ["company", "cmp"],
    "flexibility": ["job-type", "jobtype"],
    "location": ["location", "loc"]
}

MAIN_TOWNS = {
    AUTHORITY_SCARBOROUGH: "Whitby, North Yorkshire",
    AUTHORITY_SHEFFIELD: "Sheffield, South Yorkshire",
    AUTHORITY_HAMBLETON: "Northallerton, North Yorkshire",
    AUTHORITY_MOLE_VALLEY: "Dorking, Surrey",
    AUTHORITY_BRIGHTON: "Brighton, East Sussex",
    AUTHORITY_KIRKLEES: "Huddersfield, West Yorkshire",
    AUTHORITY_SHROPSHIRE: "Shrewsbury, Shropshire",
    AUTHORITY_EAST_RIDING: "East Riding of Yorkshire",
    AUTHORITY_N_LINCS: "North Lincolnshire, Lincolnshire",
    AUTHORITY_NE_LINCS: "North East Lincolnshire, Lincolnshire",
    AUTHORITY_KINGSTON_ON_HULL: "Kingston upon Hull, East Riding of Yorkshire",
    AUTHORITY_ISLE_OF_ANGLESEY: "Anglesey",
    AUTHORITY_RICHMONDSHIRE: "Richmond, North Yorkshire",
    AUTHORITY_STRATFORD_AVON: "Stratford-upon-Avon, Warwickshire",
    AUTHORITY_COVENTRY: "Coventry, West Midlands",
    AUTHORITY_HEREFORDSHIRE: "Hereford, Herefordshire",
    AUTHORITY_S_STAFFS: "Codsall, Staffordshire",
    AUTHORITY_N_WARWICKS: "Coleshill, Warwickshire",
    AUTHORITY_NUNEATON: "Nuneaton, Warwickshire",
    AUTHORITY_E_CHESHIRE: "Crewe, Cheshire",
    AUTHORITY_W_CHESHIRE: "Chester, Cheshire",
    AUTHORITY_S_LAKES: "Kendal, Cumbria",
    AUTHORITY_HYNDBURN: "Accrington, Lancashire",
    AUTHORITY_RIBBLE_VALLEY: "Clitheroe, Lancashire",
    AUTHORITY_DURHAM: "Durham, Durham",
    AUTHORITY_DERBYSHIRE_DALES: "Matlock, Derbyshire",
    AUTHORITY_EREWASH: "Long Eaton, Derbyshire",
    AUTHORITY_S_DERBYSHIRE: "Swadlincote, Derbyshire",
    AUTHORITY_HINCKLEY: "Hinckley, Leicestershire",
    AUTHORITY_E_LINDSEY: "Manby, Lincolnshire",
    AUTHORITY_W_LINDSEY: "Gainsborough, Lincolnshire",
    AUTHORITY_S_NORTHANTS: "Brackley, Northamptonshire",
    AUTHORITY_BASSETLAW: "Worksop, Nottinghamshire",
    AUTHORITY_NEWARK: "Newark-on-Trent, Nottinghamshire",
    AUTHORITY_BOURNEMOUTH_CHCH_POOLE: "Bournemouth, Dorset",
    AUTHORITY_W_SOMERSET: "Taunton, Somerset",
    AUTHORITY_C_BEDS: "Leighton Buzzard, Bedfordshire",
    AUTHORITY_CASTLE_POINT: "Thundersley, Essex",
    AUTHORITY_EPPING_FOREST: "Chipping Ongar, Essex",
    AUTHORITY_HERTSMERE: "Borehamwood, Hertfordshire",
    AUTHORITY_W_NORFOLK: "King's Lynn, Norfolk",
    AUTHORITY_N_NORFOLK: "Cromer, Norfolk",
    AUTHORITY_S_NORFOLK: "Long Stratton, Norfolk",
    AUTHORITY_BABERGH: "Sudbury, Suffolk",
    AUTHORITY_MID_SUFFOLK: "Stowmarket, Suffolk",
    AUTHORITY_BASINGSTOKE: "Basingstoke, Hampshire",
    AUTHORITY_FOLKESTONE: "Folkestone, Kent",
    AUTHORITY_TONBRIDGE: "Tonbridge, Kent",
    AUTHORITY_EPSOM: "Epsom, Surrey",
    AUTHORITY_RUNNYMEDE: "Addlestone, Surrey",
    AUTHORITY_MID_SUSSEX: "East Grinstead, West Sussex",
}


def _scrape_filters_legacy(filters: element.Tag) -> Iterable[Tuple[str, Dict[str, int]]]:
    def _map_item(item: element.Tag) -> Tuple[str, int]:
        m = PAT_FILTER_TEXT.match(item.text)

        if m:
            return m.group(1).replace(" Occupations", ''), int(m.group(2))
        else:
            raise ValueError('The filter text is unexpectedly formatted: {}', item.text)

    for cat, ids in FILTERS.items():
        for i in ids:
            menu = filters.find('ul', id=f"filter-{i}-menu")

            if menu is not None:
                items = menu.find_all('li')
                yield cat, dict(_map_item(item) for item in items)


def _lookup_location(geography: Geography) -> str:
    if geography in MAIN_TOWNS:
        return MAIN_TOWNS[geography]
    else:
        location_url = LOCATION_URL % urlencode({'query': geography.name})
        with requests.get(location_url) as r:
            j = r.json()
            if len(j):
                return r.json()[0]

    raise ValueError(f"Couldn't find starting location for Indeed search of {geography.name}")


def vacancies(geography: Geography) -> Optional[DataFrameWithMeta]:
    series = {v: [] for v in FILTERS.keys()}
    town = _lookup_location(geography)

    for r in RADII:
        url = URL % (town, r)
        page = requests.get(url)
        soup = BeautifulSoup(page.content, "lxml")

        filters = soup.find('table', id='jobsearch_nav')

        if filters is not None:
            for cat, mapped in _scrape_filters_legacy(filters):
                series[cat] += [pd.Series(mapped, name=f'jobsCount{r}')]
        else:
            print(soup)
            LOGGER.error("Cannot find Indeed filter bar at %s", url)
            return

    combined = pd.concat({k: pd.concat(v, axis=1) for k, v in series.items() if len(v) > 0}, axis=0)

    for r in RADII:
        if f'jobsCount{r}' not in combined.columns:
            combined[f'jobsCount{r}'] = 0

    combined = combined.fillna(0).astype(int).rename_axis(['categoryName', 'categoryValue'])

    combined['date'] = pd.Timestamp.now()
    combined['town'] = town
    combined['geographyCode'] = geography.code
    combined['geographyName'] = geography.name

    meta = MetaData(data_src_org='Indeed',
                    data_src_name='Job vacancies',
                    collect_src='Indeed',
                    collect_url='https://www.indeed.co.uk',
                    see_also='https://www.indeed.co.uk/about')

    return combined.reset_index(drop=False).set_index('date'), meta


if __name__ == '__main__':
    pd.options.display.max_columns = 100
    pd.options.display.max_rows = 500
    pd.options.display.width = 10000

    from bays_common.persistence import Store

    _store = Store('s3', 'bays-mtn-dev-pipeline-data')

    df, _ = _store.read_parquet('cleaned', 'la_to_region', regionCode=REGION_SOUTH_EAST.code)
    for _, r in df.iterrows():
        _g = Geography(geo_type=GeographyType.LocalAuthority, code=r.laCode, name=r.laName)
        try:
            _lookup_location(_g)
        except Exception as e:
            print(_g.name, _g.code)
