import os

import requests
import pandas as pd
from util import *

from typing import Optional

class OEC:
    def __init__(self) -> None:
        pass

    def get_members(self, cube:str, lvl:str, loc:str):
        url = 'https://oec.world/olap-proxy/members?cube={}&level={}&locale={}'.format(cube, lvl, loc)
        res = requests.get(url)
        df = pd.DataFrame(res.json()['data'])
        df.columns = df.columns.map(lambda x: x.replace(' ', '_').lower())
        
        return df

    def get_data(self, auth:bool, cube:str, drilldown:list, measure:list, cut:dict, token:Optional[str]):
        
        cuts = gen_cut(cut)
        drilldowns = gen_msr_dd(drilldown)
        measures = gen_msr_dd(measure)

        if auth:
            token = token if token else os.environ['OEC_TOKEN']
            url = 'https://oec.world/olap-proxy/data.jsonrecords?{}&cube={}&drilldowns={}&measures={}&token={}'.format(cuts, cube, drilldowns, measures, token)
        else:
            url = 'https://oec.world/olap-proxy/data.jsonrecords?{}&cube={}&drilldowns={}&measures={}'.format(cuts, cube, drilldowns, measures)
            print(url)
        res = requests.get(url)
        df = pd.DataFrame(res.json()['data'])
        df.columns = df.columns.map(lambda x: x.replace(' ', '_').lower())

        return df
