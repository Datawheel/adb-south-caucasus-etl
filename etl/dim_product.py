import pandas as pd

from modules.oec import OEC

from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.logger import logger


class ProductStep(PipelineStep):
    def run_step(self, prev_result, params):

        oec = OEC()

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
        df_hs4['hs2_aux'] = df_hs4['hs4'].apply(lambda id: int(str(id)[:-2]))
        df_hs6['hs2_aux'] = df_hs6['hs6'].apply(lambda id: int(str(id)[:-4]))

        # Final
        df = pd.merge(left=df_hs6,right=df_hs4, left_on="hs2_aux", right_on="hs2_aux", how="left")
        df = pd.merge(left=df,right=df_hs2, left_on="hs2_aux", right_on="hs2", how="left")

        df.drop(["hs2_aux"], axis = 1, inplace = True)

        # Reorder cols
        df = df[["hs2", "hs2_name","hs4", "hs4_name","hs6", "hs6_name"]]

        return df



class ProductPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [

        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-local', open('../conns.yaml'))

        dtype = {

        }

        product_step = ProductStep()

        load_step = LoadStep(
            'dim_product',
            db_connector,
            if_exists = 'drop',
            dtype = dtype,
            pk = ['hs2','hs4','hs6'],
            # nullable_list=['']
        )        
        return [product_step, load_step]   

if __name__ == "__main__":
    pp = ProductPipeline()
    df = pp.run({})