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
        
        cube = 'trade_i_baci_a_92'
        drilldown = ['Exporter Country']
        measure = ['Trade Value']
        properties = ['Exporter Country ISO 3']

        df = oec.get_data(auth=True, cube=cube,drilldown=drilldown, measure=measure, properties=properties,token=None)

        return df





class CountriesPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [

        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

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