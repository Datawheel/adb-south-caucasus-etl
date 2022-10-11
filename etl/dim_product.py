import pandas as pd

from modules.oec import OEC

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.logger import logger


class ProductStep(PipelineStep):
    def run_step(self, prev_result, params):

        oec = OEC()


        # Section
        payload_section = {
            'cube': 'trade_i_baci_a_92',
            'level': 'Section'
        }
        df_section = oec.get_members(payload=payload_section)
        df_section.columns = ['section','section_name']

        # HS2
        payload_hs2 = {
            'cube': 'trade_i_baci_a_92',
            'level': 'HS2'
        }
        df_hs2 = oec.get_members(payload=payload_hs2)
        df_hs2.columns = ['hs2','hs2_name']

        # HS4
        payload_hs4 = {
            'cube': 'trade_i_baci_a_92',
            'level': 'HS4'
        }
        df_hs4 = oec.get_members(payload=payload_hs4)
        df_hs4.columns = ['hs4','hs4_name']

        # HS6
        payload_hs6 = {
            'cube': 'trade_i_baci_a_92',
            'level': 'HS6'
        }
        df_hs6 = oec.get_members(payload=payload_hs6)
        df_hs6.columns = ['hs6','hs6_name']

        # Add columns to assist merge
        df_hs6['hs2_aux'] = df_hs6['hs6'].apply(lambda id: int(str(id)[:-4]))
        df_hs6['hs4_aux'] = df_hs6['hs6'].apply(lambda id: int(str(id)[:-2]))
        # Final
        df = pd.merge(left=df_hs6,right=df_hs4, left_on="hs4_aux", right_on="hs4", how="left")
        df = pd.merge(left=df,right=df_hs2, left_on="hs2_aux", right_on="hs2", how="left")

        # Reorder cols
        df = df[["hs2", "hs2_name","hs4", "hs4_name","hs6", "hs6_name"]]
        # Rename cols
        df.columns = ['hs2_id', 'hs2', 'hs4_id', 'hs4', 'hs6_id', 'hs6']


        df['section_id'] = df['hs2_id'].apply(lambda id: int(str(id)[:-2]))
        df = pd.merge(df, df_section, left_on='section_id', right_on='section', how='inner')
        df = df.drop('section', axis=1)
        df = df.rename(columns={'section_name': 'section'})

        return df



class ProductPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [

        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            "section_id": "UInt64",
            "section": "String",
            "hs2_id": "UInt64",
            "hs2": "String",
            "hs4_id": "UInt64",
            "hs4": "String",
            "hs6_id": "UInt64",
            "hs6": "String",

        }

        product_step = ProductStep()

        load_step = LoadStep(
            'dim_product',
            db_connector,
            if_exists = 'drop',
            dtype = dtype,
            pk = ['hs6_id'],
            # nullable_list=['']
        )        
        return [product_step, load_step]   

if __name__ == "__main__":
    pp = ProductPipeline()
    df = pp.run({})