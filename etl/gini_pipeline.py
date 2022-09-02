import pandas as pd

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.logger import logger




class ReadStep(PipelineStep):
    def run_step(self, prev_result, params):
        df = pd.read_excel("data/gini_data_v2.xlsx", engine = 'openpyxl')
        df = df.melt("Country")
        df.columns = ['country_id', 'year', 'gini']
        df['year'] = df['year'].apply(lambda x: int(x[1:]))
        return df



class GiniPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'year': 'Int64',
            'country_id': 'String',
            'gini': 'Float64'
        }

        read_step = ReadStep()

        load_step = LoadStep(
            'gini',
            db_connector,
            if_exists = 'drop',
            dtype = dtype,
            pk = ['year', 'country_id'],
            nullable_list=[ 'gini' ]
        )        
        return [read_step, load_step]   

if __name__ == "__main__":
    pp = GiniPipeline()
    pp.run({})

# improve the log!
# Find a way to merge into oec country id