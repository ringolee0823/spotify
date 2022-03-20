'''
This scripts scrapes the Spotify API for song features from a list of song IDs.
In this case, I'm scraping features from all songs scraped from the Top 200
daily charts on spotifycharts.com.
'''

import API_KEY
from Spotify_Project import access_api, get_data_dir

import numpy as np
import pandas as pd

import time, re

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

class Features:
    
    def __init__(self):
        self.sp = access_api(API_KEY.CLIENT_ID, API_KEY.CLIENT_SECRET)
        self.data_dir = get_data_dir()

    def save_csv(self, df, file_name='features', mode='a', header=False):
        with open(self.data_dir / f'df_{file_name}.csv', mode, newline='', encoding="utf-8") as f:
            df.to_csv(f, index=False, header=header)

    def check_if_downloaded(self, df_source, file_name):
        imported_track_ids = df_source['track_id'].unique().tolist()
        if (self.data_dir / f'df_{file_name}.csv').is_file():
            df_downloaded = pd.read_csv(self.data_dir / f'df_{file_name}.csv')
            downloaded_track_ids = df_downloaded['track_id'].unique().tolist()
            new_track_ids = list(set(imported_track_ids) - set(downloaded_track_ids))
            downloaded = True
            return new_track_ids, downloaded
        else:
            new_track_ids = list(set(imported_track_ids))
            downloaded = False
            return new_track_ids, downloaded

    def get_track_info_by_track_list(self, track_id_list):
        if len(track_id_list) > 50:
            track_id_list = track_id_list[:50]
        track_list = ['spotify:track:{}'.format(track_id) for track_id in track_id_list]
        info = self.sp.tracks(track_list)
        return info['tracks']

    def get_features_by_track_list(self, track_id_list):
        if len(track_id_list) > 100:
            track_id_list = track_id_list[:100]
        track_list = ['spotify:track:{}'.format(track_id) for track_id in track_id_list]
        features = self.sp.audio_features(track_list)
        return features

    def loop_by_track_list(self, track_id_list, func_, loop=100):
        idx = 0
        df = pd.DataFrame()
        while idx < len(track_id_list):
            track_ids = track_id_list[idx: idx + loop]
            print(f'Scraping set {int(idx / loop + 1)} of {int(len(track_id_list) / loop)}', end='\r')
            result = func_(track_ids)
            corrected_result = []
            for i in range(len(result)):
                if result[i] is None:
                    print('No result for track {}'.format(track_id_list[idx + i]))
                else:
                    corrected_result.append(result[i])
            idx += loop
            # time.sleep(np.random.uniform(2, 4))
            df_temp = pd.DataFrame.from_dict(corrected_result)
            df = pd.concat([df, df_temp])
        df.rename(columns={'id': 'track_id'}, inplace=True)
        print("")
        return df

    def get_track_info_by_df(self, df_source):
        print('Loading info...')
        track_id_list, downloaded = self.check_if_downloaded(df_source, 'track_info')
        result = self.loop_by_track_list(track_id_list, func_=self.get_track_info_by_track_list, loop=50)
        if not result.empty:
            result = result[['track_id', 'artists', 'album', 'popularity']]
            list_info = []
            for item in result.itertuples(index=True):
                list_info.append((
                    getattr(item, 'track_id'),
                    getattr(item, 'popularity'),
                    getattr(item, 'artists')[0]['name'],
                    getattr(item, 'artists')[0]['id'],
                    getattr(item, 'album')['id'],
                    getattr(item, 'album')['release_date'],
                    [i['url'] for i in list(getattr(item, 'album')['images']) if i != ''],
                ))
            col_features = [
                'track_id', 'track_popularity', 
                'artists_name', 'artists_id',
                'album_id', 'album_release_date', 
                'image_640'
            ]
            df = pd.DataFrame(list_info, columns=col_features)
            df['image_640'] = df['image_640'].apply(lambda x: x[0] if x != [] else "")
            if downloaded:
                self.save_csv(df, file_name='track_info', mode='a', header=False)
            else:
                self.save_csv(df, file_name='track_info', mode='w', header=True)
            return df

    def get_features_by_df(self, df_source, image_640=False):
        print('Loading features...')
        track_id_list, downloaded = self.check_if_downloaded(df_source, 'features')
        df = self.loop_by_track_list(track_id_list, func_=self.get_features_by_track_list, loop=100)
        if not df.empty:
            df = df[~(df['track_id'] == "#")].reset_index(drop=True)
            if downloaded:
                self.save_csv(df, file_name='features', mode='a', header=False)
            else:
                self.save_csv(df, file_name='features', mode='w', header=True)
            return df

    def get_genres_by_random(self, loop=10):
        list_genres = self.sp.recommendation_genre_seeds()['genres']
        dict_genres = {genre: [] for genre in list_genres}
        count = 0
        for genre in list_genres:
            for i in range(loop):
                count+=1
                print(f'{genre}:#{i:<20}', end='\r')
                df = pd.json_normalize(
                    self.sp.recommendations(
                        seed_genres=[genre], 
                        limit=100, 
                        min_popularity=0
                    )['tracks']
                )
                df['genre'] = genre
                if 'id' in df.columns:
                    dict_genres[genre].append(df[['id', 'genre']])
                    df.rename(columns={'id': 'track_id'}, inplace=True)
                    df = df[['track_id', 'genre']]
                    with open(self.data_dir / 'df_genre.csv', 'a', newline='', encoding="utf-8") as f:
                        df.to_csv(f, index=False, header=True if count==1 else False)
        df = pd.read_csv(self.data_dir / 'df_genre.csv')
        df.drop_duplicates(keep='first', inplace=True, ignore_index=True)
        df.to_csv(self.data_dir / 'df_genre.csv', index=False)