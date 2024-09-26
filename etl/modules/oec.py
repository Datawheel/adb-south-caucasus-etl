import os

import requests
import pandas as pd

from time import sleep

class OEC:
    def __init__(self) -> None:
        pass

    def get_members(self, payload:dict):
        """
        payload must be a dict like:
        payload = {
            'cube': 'trade_i_baci_a_92',
            'level': 'Year'
        }
        """
        base_url = 'https://oec.world/olap-proxy/members?'

        while True:    
            try: 
                r = requests.get(base_url, params = payload)
                # Print requested url
                print(r.url)
            except:
                print('Error performing request {}'.format(r.status_code))
                sleep(10)
            else:
                print('Successful data call to OEC {}'.format(r.status_code))
                break

        df = pd.DataFrame(r.json()['data'])
        df.sort_values('ID').reset_index(drop=True)
        df.columns = df.columns.map(lambda x: x.replace(' ', '_').lower())
        return df

    def get_data(self, auth:bool, cube:str, drilldown:list, measure:list, cut:dict, properties:dict=None, token:str = ''):
        """
        usage example:
        cut = {
            'Year': '2020',
            'Trade Flow': '2'
        }
        drilldown = ['Year', 'Subnat Geography', 'Country', 'Product']
        measure = ['Trade Value']
        cube='trade_i_baci_a_92'
        token = 'my_token'

        oec.get_data(False, cube, drlldown, measure, cut)
        
        """
        base_url = 'https://oec.world/olap-proxy/data.jsonrecords?'

        if cut == None:
            payload = {}
        else:
            payload = cut.copy()
        
        drilldown = ','.join(drilldown)
        measure = ','.join(measure)

        payload['cube'] = cube
        payload['drilldowns'] = drilldown
        payload['measures'] = measure

        if properties != None:
            payload['properties'] = properties

        if auth:
            payload['token'] = token if token else os.environ['OEC_TOKEN']
        
        while True:    
            try: 
                r = requests.get(base_url, params = payload)
                # Print requested url
                print('Request:')
                print(r.url)
            except:
                print('Error performing request {}'.format(r.status_code))
                sleep(10)
            else:
                print('Successful data call to OEC {}'.format(r.status_code))
                break
        df = pd.DataFrame(r.json()['data'])
        
        if df.empty:
            raise Exception('Empty Dataframe')
        else:
            df.columns = df.columns.map(lambda x: x.replace(' ', '_').lower())

            return df