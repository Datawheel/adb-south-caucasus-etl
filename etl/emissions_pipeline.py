import pandas as pd

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.logger import logger




class ReadStep(PipelineStep):
    def run_step(self, prev_result, params):
        df = pd.read_excel('data/ghg_data.xls')
        df = df.melt("Country")
        df.columns = ['country_id', 'year', 'emissions_KCO2']
        df['year'] = df['year'].apply(lambda x: int(x))
        return df



class EmissionsPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'country_id': 'String',
            'year': 'Int64',
            'emissions_KCO2': 'Float64',
        }


        read_step = ReadStep()

        load_step = LoadStep(
            'emissions',
            db_connector,
            if_exists = 'drop',
            dtype = dtype,
            pk = ['year', 'country_id'],
            nullable_list=[     
                'emissions_KCO2',
            ]
        )        
        return [read_step, load_step]   

if __name__ == "__main__":
    pp = EmissionsPipeline()
    pp.run({})

# improve the log!

