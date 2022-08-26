import pandas as pd

from modules.oec import OEC

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.logger import logger


class CountriesStep(PipelineStep):
    def run_step(self, prev_result, params):
        
        #  logger.info("Downloading Members: {} {}  from OEC...".format(payload['cube'], payload['level']))

        oec = OEC()
        payload = {
            'cube': 'trade_i_baci_a_92',
            'level': 'Country'
        }
        df = oec.get_members(payload=payload)
        
        df.columns = ['oec_id', 'comtrade_name']
        print(df.dtypes)
        return df



class CountriesPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [

        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-local', open('../conns.yaml'))

        dtype = {
            'oec_id': 'String',
            'comtrade_name': 'String',
        }

        countries_step = CountriesStep()

        load_step = LoadStep(
            'dim_countries',
            db_connector,
            if_exists = 'drop',
            dtype = dtype,
            pk = ['oec_id'],
            nullable_list=['comtrade_name']
        )        
        return [countries_step, load_step]   

if __name__ == "__main__":
    pp = CountriesPipeline()
    df = pp.run({})