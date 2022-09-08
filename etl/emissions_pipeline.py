import pandas as pd

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.logger import logger




class ReadStep(PipelineStep):
    def run_step(self, prev_result, params):
        df = pd.read_csv('data/data_stacked_emissions_v2.csv')
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
            'country': 'String',
            'year': 'Int64',
            'population': 'Float64',
            'eci_trade': 'Float64',
            'exports': 'Float64',
            'fitness_trade': 'Float64',
            'eci_publications': 'Float64',
            'fitness_publications': 'Float64',
            'documents': 'Float64',
            'citations': 'Float64',
            'eci_patents': 'Float64',
            'fitness_patents': 'Float64',
            'patents': 'Float64',
            'eci_services': 'Float64',
            'fitness_services': 'Float64',
            'service_exports': 'Float64',
            'gdp_start': 'Float64',
            'emissions': 'Float64',
            'hum_cap': 'Float64',
            'eci_similarity_trade': 'Float64',
            'eci_similarity_publications': 'Float64',
            'eci_similarity_patents': 'Float64',
            'eci_similarity_services': 'Float64',
            'entropy_trade': 'Float64',
            'entropy_publications': 'Float64',
            'entropy_patents': 'Float64',
            'entropy_services': 'Float64',
            'hhi_trade': 'Float64',
            'hhi_publications': 'Float64',
            'hhi_patents': 'Float64',
            'hhi_services': 'Float64',
            'nat_res': 'Float64',
        }


        read_step = ReadStep()

        load_step = LoadStep(
            'emissions',
            db_connector,
            if_exists = 'drop',
            dtype = dtype,
            pk = ['year', 'country'],
            nullable_list=[     
                'population',
                'eci_trade',
                'exports',
                'fitness_trade',
                'eci_publications',
                'fitness_publications',
                'documents',
                'citations',
                'eci_patents',
                'fitness_patents',
                'patents',
                'eci_services',
                'fitness_services',
                'service_exports',
                'gdp_start',
                'emissions',
                'hum_cap',
                'eci_similarity_trade',
                'eci_similarity_publications',
                'eci_similarity_patents',
                'eci_similarity_services',
                'entropy_trade',
                'entropy_publications',
                'entropy_patents',
                'entropy_services',
                'hhi_trade',
                'hhi_publications',
                'hhi_patents',
                'hhi_services',
                'nat_res',
            ]
        )        
        return [read_step, load_step]   

if __name__ == "__main__":
    pp = EmissionsPipeline()
    pp.run({})

# improve the log!
# Find a way to merge into oec country id
