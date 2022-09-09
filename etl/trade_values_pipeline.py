
import pandas as pd

from modules.oec import OEC

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.logger import logger
from static import LIST_COUNTRY


class TradeValuesStep(PipelineStep):
    def run_step(self, prev_result, params):
        oec = OEC()
        
        year = params.get('year')
        country = params.get('country')
        
        cut = {
            'Year': str(year),
            'Exporter Country': str(country)
        }
        cube = 'trade_i_baci_a_92'
        drilldown = ['Year', 'Exporter Country', 'Importer Country', 'HS6']
        measure = ['Trade Value']
        df = oec.get_data(auth=True, cube=cube,drilldown=drilldown, measure=measure, cut=cut, token=None)
        # What if there's no data on the OEC? how to jump that exeption?
        return df



class TradeValuesPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label='year', name='year', dtype=str),
            Parameter(label='country', name='country', dtype=str)
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'year': 'Int64',
            'exporter_country_id': 'String',
            'exporter_country': 'String',
            'importer_country_id': 'String',
            'importer_country': 'String',
            'hs6_id': 'Int64',
            'hs6': 'String',
            'trade_value': 'Float64',
        }

        trade_values_step = TradeValuesStep()

        load_step = LoadStep(
            'trade_values',
            db_connector,
            if_exists = 'append',
            dtype = dtype,
            pk = ['year', 'exporter_country_id','importer_country_id','hs6_id'],
            nullable_list=[ 'exporter_country', 'importer_country', 'hs6','trade_value']
        )        
        return [trade_values_step, load_step]   

if __name__ == "__main__":
    pp = TradeValuesPipeline()

    for country in LIST_COUNTRY:
        for year in range(1995,2020 + 1):
            pp.run({
                "year": year,
                "country": country
            })


# improve the log!
