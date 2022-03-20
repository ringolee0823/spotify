'''
This script scrapes the spotifycharts.com site for all daily data
available for all countries. Many dates are missing from the database on
Spotify's end, so nothing we can do about that.
'''

from Spotify_Project import get_data_dir

import numpy as np
import pandas as pd
import dask, sqlite3, time, re

from pathlib import Path
from requests_html import HTMLSession
from dask.diagnostics import ProgressBar
from sqlalchemy import create_engine


class Rankings:

    def __init__(self):
        self.data_dir = get_data_dir()
        self.not_downloaded = ""
        self.newly_downloaded_path = ""

    def check_if_downloaded(self):

        session = HTMLSession()
        r = session.get('https://spotifycharts.com/regional/global/daily/latest')

        dict_available_country = {'code': [], 'name':[]}
        for i in r.html.find("div[data-type=country]"):
            for j in i.find("li[data-value]"):
                dict_available_country['code'].append(j.attrs['data-value'])
                dict_available_country['name'].append(j.text)
        df_available_country = pd.DataFrame(dict_available_country)

        dict_available_date = {'date': []}
        for i in r.html.find("div[data-type=date]"):
            for j in i.find("li[data-value]"):
                dict_available_date['date'].append(j.text)
        df_available_date = pd.DataFrame(dict_available_date)
        df_available_date['date'] = pd.to_datetime(df_available_date['date'], errors="coerce")
        df_available_date['date'] = df_available_date['date'].dt.strftime('%Y-%m-%d')

        downloaded = []
        for i in self.data_dir.glob('**/*.csv'):
            downloaded.append(i.stem)

        available = [f'{code} {date}' 
                     for code in df_available_country['code'].tolist() 
                     for date in df_available_date['date'].tolist()]

        not_downloaded = set(available) - set(downloaded)

        if not_downloaded:
            print(f"----------Outstanding {len(not_downloaded)} Records----------")
        else:
            print("----------All Records Downloaded---------")

        self.not_downloaded = not_downloaded

    def download_new_data(self):

        self.newly_downloaded_path = [] 

        for to_download in sorted(self.not_downloaded):
            
            country, date = to_download.split()
            print(f"---------To download {country} {date}---------", end='\r')
            
            country_dir = (self.data_dir / f'{country}')
            country_dir.mkdir(exist_ok=True)
            
            session = HTMLSession()
            r = session.get(f'https://spotifycharts.com/regional/{country}/daily/{date}')
            
            data = {'date': [], 'url': [], 'image': [], 'position': [], 'track': [], 'streams': []}

            for i in r.html.find('td.chart-table-image'):
                data['date'].append(date)
                data['url'].append(i.find('a', first=True).attrs['href'])
                data['image'].append(i.find('img', first=True).attrs['src'])

            for i in r.html.find('td.chart-table-position'):
                data['position'].append(i.text)

            for i in r.html.find('td.chart-table-track'):
                data['track'].append(i.text)
                
            for i in r.html.find('td.chart-table-streams'):
                data['streams'].append(i.text)
                
            pd.DataFrame(data).to_csv(country_dir / f'{country} {date}.csv')

            self.newly_downloaded_path.append(country_dir / f'{country} {date}.csv')

        print("----------All Records Downloaded---------")

    def format_ranking_df(self, df_ranking: pd.DataFrame):
        col = ['date', 'country_code', 'position', 'artist', 'track', 'track_id', 'url', 'image', 'streams']
        if df_ranking.empty:
            return pd.DataFrame(columns=col)
        df_ranking[['track', 'artist']] = df_ranking['track'].str.split(' by ', n=1, expand=True)
        df_ranking['track_id'] = df_ranking['url'].str.extract(r"([^\/]+$)")
        df_ranking = df_ranking[~(df_ranking['track_id'] == '#')]
        df_ranking = df_ranking[col]
        
        df_ranking.sort_values(['date', 'country_code', 'position'], inplace=True)
        df_ranking.reset_index(drop=True, inplace=True)

        df_ranking['date'] = pd.to_datetime(df_ranking['date'], errors='coerce')
        df_ranking['position'] = pd.to_numeric(df_ranking['position'], errors='coerce')
        df_ranking['streams'] = pd.to_numeric(df_ranking['streams'].str.replace(',', ''), errors='coerce')

        return df_ranking

    def import_ranking_csv(self, file_path: Path):
        col = ['date', 'country_code', 'url', 'image', 'position', 'track', 'streams']
        df_ranking = pd.read_csv(file_path)
        if df_ranking.empty:
            return pd.DataFrame(columns=col)
        try:
            country_code, _ = file_path.stem.split()
        except:
            print(file_path.stem)
        df_ranking['country_code'] = country_code
        df_ranking = df_ranking[col]
        return self.format_ranking_df(df_ranking)

    def import_ranking_csvs(self, list_file_path=None):
        if not list_file_path:
            list_file_path = self.data_dir.glob('**/* *-*-*.csv')
        list_ranking = []
        for file_path in list_file_path:
            list_ranking.append(dask.delayed(self.import_ranking_csv)(file_path))
        delayed_list = dask.delayed(pd.concat)(list_ranking, axis=0, sort=False)
        with dask.diagnostics.ProgressBar():
            df_ranking = delayed_list.compute()
        return df_ranking

    def update_ranking_db(self, database='spotify'):
        if (self.newly_downloaded_path != []) or (not (self.data_dir / f'{database}.db').is_file()):
            df_to_update = self.import_ranking_csvs(self.newly_downloaded_path)
            df_to_update = df_to_update[~(df_to_update['track_id'] == "#")].reset_index(drop=True)
            df_to_update.sort_values(['date', 'country_code', 'position'], inplace=True)
            self.export_to_db(df_to_update, table='Ranking', database=database, if_exists='append')
        else:
            print("----------No oustanding record to update to db----------")

    def import_from_db(self, query_string=None, query_params=None, database='spotify', verbose=True):
        start_time = time.time()
        db_dir = self.data_dir / f'{database}.db'
        engine = create_engine(f'sqlite:///{db_dir}')
        df = pd.read_sql(query_string, engine, params=query_params)
        engine.dispose()
        end_time = time.time()
        if verbose: 
            print(f'{database:.<50} ready by {end_time - start_time:.3f}s' )
        return df

    def export_to_db(self, df, table, database='spotify', if_exists='append'):
        sqlite3.register_adapter(np.int64, lambda val: int(val))
        start_time = time.time()
        db_dir = self.data_dir / f'{database}.db'
        engine = create_engine(f'sqlite:///{db_dir}')
        df.to_sql(table, engine, if_exists=if_exists, index=False)
        engine.dispose()
        end_time = time.time()
        print(f'{database:.<50} ready by {end_time - start_time:.3f}s' )