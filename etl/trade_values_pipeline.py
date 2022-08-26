
import pandas as pd

from modules.oec import OEC

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.logger import logger


class MembersStep(PipelineStep):
    def run_step(self, prev_result, params):
        oec = OEC()
        # Get members
        # Years
        years = oec.get_members(payload={'cube': 'trade_i_baci_a_92','level': 'Year'}).sort_values("id").reset_index(drop=True)['id']
        # Countries
        countries = oec.get_members(payload={'cube': 'trade_i_baci_a_92','level': 'Country'}).sort_values("id").reset_index(drop=True)['id']
        return(years, countries)

class TradeValuesStep(PipelineStep):
    def run_step(self, prev_result, params):
        # Unpack prev
        years, countries = prev_result

        oec = OEC()
        # years = ['2019','2020']
        # countries = ['afago', 'euesp']

        years = ['1995']
        countries = ['afago']


        for year in years:
            for country in countries:
                cut = {
                    'Year': str(year),
                    'Exporter Country': str(country)
                }
                cube = 'trade_i_baci_a_92'
                drilldown = ['Year', 'Exporter Country', 'Importer Country', 'HS6']
                measure = ['Trade Value']
                df = oec.get_data(auth=True, cube=cube,drilldown=drilldown, measure=measure, cut=cut, token=None)
        return df



class TradeValuesPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [

        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-local', open('../conns.yaml'))

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

        members_step = MembersStep()

        trade_values_step = TradeValuesStep()

        load_step = LoadStep(
            'trade_values',
            db_connector,
            if_exists = 'append',
            dtype = dtype,
            pk = ['year'],
            nullable_list=['year', 'exporter_country_id', 'exporter_country','importer_country_id', 'importer_country', 'hs6_id', 'hs6','trade_value']
        )        
        return [members_step, trade_values_step, load_step]   

if __name__ == "__main__":
    pp = TradeValuesPipeline()
    df = pp.run({})