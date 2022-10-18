import pandas as pd

from modules.oec import OEC

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.logger import logger




class ReadStep(PipelineStep):
    def run_step(self, prev_result, params):
        df = pd.read_csv("data/gini.csv")

        df = df.set_index('geo_id')
        df.loc['scr'] = df.loc['aga']
        df = df.drop(['abu', 'aga'])
        df = df.reset_index()


        oec = OEC()

        payload = {
            'cube': 'trade_i_baci_a_92',
            'level': 'Country'
        }
        df_geo_id = oec.get_members(payload=payload)
        df_geo_id['iso_3'] = df_geo_id['id'].str.slice(start=2).str.lower()
        df_geo_id.loc[len(df_geo_id.index)] = ['asscr', 'South Caucasus', 'scr']
        id_mapper = df_geo_id.drop('label', axis = 1).set_index('iso_3').to_dict()

        df['oec_id'] = df['geo_id'].map(id_mapper['id'])
        df = df[['oec_id', 'gini']]
        df = df.dropna().reset_index(drop=True)
        print(df)
        return df

# geo_id,year,gini
# arm,2015,46.9426266666667
# aus,2015,41.6174175
# aut,2015,36.7199325

class GiniPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'oec_id': 'String',
            'gini': 'Float64'
        }

        read_step = ReadStep()

        load_step = LoadStep(
            'gini',
            db_connector,
            if_exists = 'drop',
            dtype = dtype,
            pk = ['oec_id'],
            nullable_list=[ 'gini' ]
        )        
        return [read_step, load_step]   

if __name__ == "__main__":
    pp = GiniPipeline()
    pp.run({})

# improve the log!
# Find a way to merge into oec country id