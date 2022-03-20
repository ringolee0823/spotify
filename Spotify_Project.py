'''
This script provide generic spotify api access
'''

from pathlib import Path

import pandas as pd

import spotipy, re
from spotipy.oauth2 import SpotifyClientCredentials

def access_api(client_id: str, client_secret: str):

    # initialize spotify client credentials
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    
    # initialize spotify object to access API
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager) 
    
    return sp

def get_data_dir():
    # Install external packages for Google Colab
    from pathlib import Path
    if 'google.colab' in str(get_ipython()):
        import subprocess
        # from google.colab import drive
        from IPython.display import clear_output
        subprocess.run('pip install requests-html spotipy', shell=True)
        subprocess.run('wget https://www.dropbox.com/s/pfllnj5fynfge8o/df_rankings?dl=1 -O "/content/STAT5106/Spotify Chart Data/df_rankings"', shell=True)
        subprocess.run('wget https://www.dropbox.com/s/3zf0qdlv7mqx55s/df_streams?dl=1 -O "/content/STAT5106/Spotify Chart Data/df_streams"', shell=True)
        subprocess.run('wget https://www.dropbox.com/s/dk3me33yd1ea8nk/df_features.csv?dl=1 -O "/content/STAT5106/Spotify Chart Data/df_features.csv"', shell=True)
        subprocess.run('wget https://www.dropbox.com/s/652mcsqk71ja5kx/df_happiness.csv?dl=1 -O "/content/STAT5106/Spotify Chart Data/df_happiness.csv"', shell=True)
        subprocess.run('wget https://www.dropbox.com/s/ujnztbuvir3mu1m/df_track_info.csv?dl=1 -O "/content/STAT5106/Spotify Chart Data/df_track_info.csv"', shell=True)
        # drive.mount('/content/drive')
        data_dir = Path('/content/STAT5106/Spotify Chart Data')
        clear_output()
    else:
        data_dir = Path.cwd() / "Spotify Chart Data"
    data_dir.mkdir(exist_ok=True)    
    return data_dir


def get_country_codes():

    url_country_codes = "https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv"
    df = pd.read_csv(url_country_codes)
    df.dropna(subset=['ISO3166-1-Alpha-2'], inplace=True)
    df.columns = df.columns.str.lower()

    df['country_code'] = df['iso3166-1-alpha-2'].str.lower()
    df['country_code_alpha3'] = df['iso3166-1-alpha-3'].str.upper()
    df['country'] = df['unterm english short'].apply(lambda text: str(text).split(' (')[0])
    df['country'].replace('United States of America', 'United States', inplace=True)
    df['country'].replace('United Kingdom of Great Britain and Northern Ireland', 'United Kingdom', inplace=True)
    df.loc[df['iso3166-1-alpha-2'] == 'TW', 'country'] = 'Taiwan'
    df.loc[df['iso3166-1-alpha-2'] == 'TW', 'region name'] = 'Asia'
    df.loc[df['iso3166-1-alpha-2'] == 'TW', 'sub-region name'] = 'Eastern Asia'
    df.loc[df['iso3166-1-alpha-2'] == 'HK', 'country'] = 'Hong Kong'


    url_iso_639 = "https://raw.githubusercontent.com/ringolee0823/STAT5106/main/iso-639-3.tab"
    df_iso_639 = pd.read_csv(url_iso_639, delimiter='\t')
    df_iso_639.columns = df_iso_639.columns.str.lower()
    df_iso_639 = df_iso_639.dropna(subset=['part1'])[['part1', 'ref_name']]

    list_languages = [(i[1]['part1'], i[1]['ref_name']) for i in df_iso_639.iterrows()]

    df['languages_list'] = df['languages'].apply(lambda x: [re.sub('-.*', '', i) for i in str(x).split(',')])
    df['languages_list'] = df['languages_list'].apply(lambda x: [x[0]] if x else [""])

    df['languages_name'] = df['languages_list'].apply(lambda x: [k[1] for k in list_languages if k[0] in x])
    df['languages_name'] = df['languages_name'].apply(lambda x: x[0] if x else "")

    df = df[['country_code', 'country_code_alpha3', 'region name', 'sub-region name', 'languages_name', 'country']]

    df.columns = df.columns.str. \
                    replace(" ", "_").str. \
                    replace("-", "_")
    
    return df

def get_happiness_index(year=None):

    dict_happiness_index = {
        "2017": ["https://s3.amazonaws.com/happiness-report/2018/WHR2018Chapter2OnlineData.xls", 1],
        "2018": ["https://s3.amazonaws.com/happiness-report/2019/Chapter2OnlineData.xls", 1],
        "2019": ["https://happiness-report.s3.amazonaws.com/2020/WHR20_DataForFigure2.1.xls", 0],
        "2020": ["https://happiness-report.s3.amazonaws.com/2021/DataForFigure2.1WHR2021C2.xls", 0]
    }

    list_field = [
        'country',
        'year',
        'happiness_score',
        '_log_gdp_per_capita',
        '_social_support',
        '_healthy_life_expectancy',
        '_freedom_to_make_life_choices',
        '_generosity',
        '_perceptions_of_corruption',
        'dystopia_+_residual'
    ]

    list_df = []

    for k, v in dict_happiness_index.items():
        df_temp = pd.read_excel(v[0], sheet_name=v[1])
        df_temp.columns = \
            df_temp.columns.str. \
                lower().str. \
                replace("explained by: ", "_", regex=True).str. \
                replace(" \(.*\)", "", regex=True).str. \
                replace("^unnamed.*", "", regex=True).str. \
                replace(" ", "_", regex=True)
        if "" in df_temp.columns:
            df_temp.drop(columns="", inplace=True)
        df_temp['year'] = k
        df_temp.columns = \
            df_temp.columns.str. \
                replace('^country_name$', 'country', regex=True).str. \
                replace('^ladder_score$', 'happiness_score', regex=True).str. \
                replace('^_gdp_per_capita$', '_log_gdp_per_capita', regex=True)
        df_temp = df_temp[list_field]
        df_temp.columns = \
            df_temp.columns.str.replace('^_', '', regex=True)
        list_df.append(df_temp)

    df = pd.concat(list_df, axis=0).drop_duplicates()

    df = df.sort_values(['year', 'happiness_score']).reset_index(drop=True)

    if year:
        df = df[df['year'] == year]

    df.loc[df['country'] == 'Hong Kong S.A.R. of China', 'country'] = 'Hong Kong' 
    df.loc[df['country'] == 'Vietnam', 'country'] = 'Viet Nam'
    df.loc[df['country'] == 'Tanzania', 'country'] = 'United Republic of Tanzania'
    df.loc[df['country'] == 'Laos', 'country'] = 'Lao People\'s Democratic Republic'
    df.loc[df['country'] == 'Congo (Brazzaville)', 'country'] = 'Democratic Republic of the Congo'
    df.loc[df['country'] == 'Russia', 'country'] = 'Russian Federation'
    df.loc[df['country'] == 'Moldova', 'country'] = 'Republic of Moldova'
    df.loc[df['country'] == 'South Korea', 'country'] = 'Republic of Korea'
    df.loc[df['country'] == 'Taiwan Province of China', 'country'] = 'Taiwan'

    return df