import pandas as pd
import numpy as np
import os


class JHUC_Data(object):
    '''
    Data from repository maintained by Johns Hopkins University Center
    for Systems Science and Engineering (CSSE) by collating data from around the world
    '''
    def __init__(self):
        url_base = 'https://raw.githubusercontent.com/'
        url_base += 'CSSEGISandData/COVID-19/master/'
        url_base += 'csse_covid_19_data/csse_covid_19_daily_reports/{}.csv'
        self.url_base = url_base

        today = pd.to_datetime('today').date()

        dates = pd.date_range('2020-01-22', today)
        dates = dates.map(lambda x: x.strftime('%m-%d-%Y'))
        self.dates = dates.tolist()[:-1] # Drop today because
                                         # perhaps data not updated

        self.dir = os.listdir('data')
        self.df_build = pd.DataFrame()

        self.names = {  'Province/State':  'Province_State',
                        'Country/Region':  'Country_Region',
                        'Last Update':     'Last_Update',
                        'Latitude':        'Lat',
                        'Long_':           'Long',
                        'Longitude':       'Long',           }

    def get_data(self):
        for d in self.dates:
            # Check file not in dir
            if d+'.csv' in self.dir:
                print(d, ':', 'already downloaded')
                continue
            # Otherwise, get
            try:
                url = self.url_base.format(d)
                df = pd.read_csv(url)
                for old, new in self.names.items():
                    if old in df.columns:
                        df.rename(columns={old: new}, inplace=True)
                df['Date'] = d
                f = '{}.csv'.format(d)
                p = os.path.join('data', f)
                df.to_csv(p, index=False)
                print(d, ':', 'newly downloaded')
            except:
                print(d, ':', 'error')


class Data(object):

    def __init__(self):
        self.dir = os.listdir('data')

    def get_va_data(self):
        states = ['Virginia',]
        df = pd.DataFrame()
        for f in self.dir:
            p = os.path.join('data', f)
            df_ = pd.read_csv(p)
            df_ = df_[df_.Province_State.notna()]
            filter = df_.Province_State.isin(states)
            df_ = df_[filter]
            df = df.append(df_, sort=True)
            #
        select = ['Date','Admin2','Confirmed','Deaths',]
        self.va_df = df[select]

    def get_va_situ(self):
        select = ['Confirmed', 'Deaths']
        df = self.va_df.groupby('Date').sum()[select]
        df = df.reset_index()

        def get_new(column, new_column):
            new = list()
            for index, row in df.iterrows():
                if index != df.shape[0]-1:
                    x = df.iloc[index+1][column] - df.iloc[index][column]
                    new.append(x)
            new.insert(0, np.nan)
            df[new_column] = new

        get_new('Confirmed', 'Confirmed_New')
        get_new('Deaths', 'Deaths_New')

        '''new_rate = list()
        for index, row in df.iterrows():
            if index != df.shape[0]-1:
                x = df.iloc[index+1]['Confirmed_New'] / df.iloc[index]['Confirmed_New'] * 1
                new_rate.append(x)
        new_rate.insert(0, np.nan)
        df['Conf_New_Rate'] = new_rate'''

        select = ['Date', 'Confirmed', 'Confirmed_New',  'Deaths', 'Deaths_New'] #'Conf_New_Rate',
        df = df[select]

        print('\nVirginia Situation')
        print(df.to_string(index=False))

    def get_ffx_situ(self):
        filter = self.va_df.Admin2=='Fairfax'
        df = self.va_df[filter]
        select = ['Confirmed', 'Deaths']
        df = df.groupby('Date').sum()[select]
        df = df.reset_index()

        def get_new(column, new_column):
            new = list()
            for index, row in df.iterrows():
                if index != df.shape[0]-1:
                    x = df.iloc[index+1][column] - df.iloc[index][column]
                    new.append(x)
            new.insert(0, np.nan)
            df[new_column] = new

        get_new('Confirmed', 'Confirmed_New')
        get_new('Deaths', 'Deaths_New')

        select = ['Date', 'Confirmed', 'Confirmed_New', 'Deaths', 'Deaths_New']
        df = df[select]

        print('\nFairfax Situation')
        print(df.to_string(index=False))



if __name__ == '__main__':

    JD = JHUC_Data()
    JD.get_data()

    D = Data()
    D.get_va_data()
    D.get_va_situ()
    D.get_ffx_situ()
