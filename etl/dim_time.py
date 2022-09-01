import pandas as pd

from modules.oec import OEC

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep


class YearsStep(PipelineStep):
    def run_step(self, prev_result, params):
        oec = OEC()
        payload = {
            'cube': 'trade_i_baci_a_92',
            'level': 'Year'
        }
        df = oec.get_members(payload=payload)
        df = df.sort_values("id").reset_index(drop=True)
        df.columns = ['year']
        return df



class YearsPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [

        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'dim_year': 'Int16',
        }

        years_step = YearsStep()

        load_step = LoadStep(
            'dim_time',
            db_connector,
            if_exists = 'drop',
            dtype = dtype,
            pk = ['year']
        )        
        return [years_step, load_step]   

if __name__ == "__main__":
    pp = YearsPipeline()
    df = pp.run({})