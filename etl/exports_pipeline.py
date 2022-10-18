import pandas as pd
from modules.oec import OEC
import requests as r
import numpy as np 
import economic_complexity as ec
from bamboo_lib.connectors.models import Connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep
from bamboo_lib.logger import logger
import os

oec_token = os.environ['OEC_TOKEN']

class DownloadStep(PipelineStep):
    def run_step(self, prev, params):
        # Get data directly form the OEC
        logger.info("Downloading: from OEC...")
        df_between_SCR_initial = pd.DataFrame(r.get('https://oec.world/olap-proxy/data.jsonrecords?Exporter+Country=asarm%2Casaze%2Casgeo&Importer+Country=asarm%2Casaze%2Casgeo&Year=2017%2C2018%2C2019%2C2020&cube=trade_i_baci_a_92&drilldowns=Exporter+Country%2CHS4%2CImporter+Country&measures=Trade+Value&token={}'.format(oec_token)).json()['data'])
        df_between_SCR_initial


        df_international_initial = pd.DataFrame(r.get('https://oec.world/olap-proxy/data.jsonrecords?Year=2017%2C2018%2C2019%2C2020&cube=trade_i_baci_a_92&drilldowns=Exporter+Country%2CHS4&measures=Trade+Value&token={}'.format(oec_token)).json()['data'])
        df_international_initial = df_international_initial.pivot_table(values='Trade Value', index='Country ID', columns= 'HS4 ID', aggfunc=np.sum).reset_index()


        df_indicators_initial = pd.DataFrame(r.get('https://oec.world/olap-proxy/data.jsonrecords?Indicator=NY.GDP.MKTP.PP.KD%2CNY.GDP.PCAP.PP.KD%2CSP.POP.TOTL&Year=2020&cube=indicators_i_wdi_a&drilldowns=Country%2CIndicator&measures=Measure&token={}'.format(oec_token)).json()['data'])
        df_indicators_initial = df_indicators_initial.pivot(index = 'Country ID',columns=['Indicator'], values = ['Measure'], ).reset_index()
        df_indicators_initial.columns = ['Country ID','GDP per capita, PPP (constant 2017 international $)','GDP, PPP (constant 2017 international $)', 'Population, total']
        logger.info("Processing Download...")

        # Make copies
        df_all_countries = df_international_initial.copy()
        df_between_SCR = df_between_SCR_initial.copy()
        df_indicators = df_indicators_initial.copy()

        # Fill NaNs with 0
        df_all_countries = df_all_countries.fillna(0)
        df_between_SCR = df_between_SCR.fillna(0)
        df_indicators = df_indicators.fillna(0)

        # Add SCR as country 
        df_3scr = df_all_countries[(df_all_countries['Country ID']=='asgeo') | (df_all_countries['Country ID']=='asarm') | (df_all_countries['Country ID']=='asaze')]
        df_scr= df_3scr.replace({'Country ID': {'asarm':'asscr', 'asgeo':'asscr', 'asaze':'asscr'}}).groupby('Country ID', sort=False).sum().reset_index()
        df_all_countries = df_all_countries.append(df_scr)

        # Remove the trade betweeen SCR
        df_between_SCR_per_prod = df_between_SCR.drop(columns=['Exporter Country ID', 'Exporter Country','Importer Country ID','Importer Country','HS4']).groupby('HS4 ID').sum().reset_index()
        df_all_countries = df_all_countries.set_index('Country ID')
        for prod in set(df_between_SCR_per_prod['HS4 ID']).intersection(set(df_all_countries.columns)):
            df_all_countries.loc['asscr',prod] = df_all_countries.loc['asscr',prod] - df_between_SCR_per_prod[df_between_SCR_per_prod['HS4 ID']==prod]['Trade Value'].values[0]
        df_all_countries = df_all_countries.reset_index()

        # Calculate and Add SCR to indicators 
        scr_pop_2020 = df_indicators[df_indicators['Country ID']=='asarm']['Population, total'].values[0]\
                    + df_indicators[df_indicators['Country ID']=='asgeo']['Population, total'].values[0]\
                    + df_indicators[df_indicators['Country ID']=='asaze']['Population, total'].values[0]
        scr_gdp_2020 = df_indicators[df_indicators['Country ID']=='asarm']['GDP, PPP (constant 2017 international $)'].values[0]\
                    + df_indicators[df_indicators['Country ID']=='asgeo']['GDP, PPP (constant 2017 international $)'].values[0]\
                    + df_indicators[df_indicators['Country ID']=='asaze']['GDP, PPP (constant 2017 international $)'].values[0]
        scr_gdp_per_cap = scr_gdp_2020/scr_pop_2020
        df_indicators = df_indicators.append({'Country ID':'asscr', 'GDP, PPP (constant 2017 international $)':scr_gdp_2020, 'GDP per capita, PPP (constant 2017 international $)':scr_gdp_per_cap, 'Population, total':scr_pop_2020}, ignore_index=True)


        # Drop low products
        # Sum over Trade Flows for every region 
        df_prod_trade = pd.DataFrame()
        df_prod_trade['Trade Value'] = df_all_countries.iloc[:,1:].sum()
        low_products = df_prod_trade[df_prod_trade['Trade Value'] < 500000000].index.values
        df_all_countries = df_all_countries.drop(columns=low_products)

        # Drop low countries
        # Sum over Trade Flows for every country 
        df_geo_trade = pd.DataFrame()
        df_all_countries = df_all_countries.set_index('Country ID')
        df_geo_trade['Trade Value'] = df_all_countries.iloc[:,1:].sum(axis=1)
        df_all_countries = df_all_countries.reset_index()
        low_countries = np.unique(df_geo_trade[df_geo_trade['Trade Value'] < 4000000000].index.values)
        df_all_countries = df_all_countries[~df_all_countries['Country ID'].isin(low_countries)]


        # Drop low population
        common_countries = set(df_all_countries['Country ID'].values).intersection(set(df_indicators['Country ID'].values))
        df_population_2020 = df_indicators[df_indicators['Country ID'].isin(common_countries)][['Country ID','Population, total']]
        medium_countries_pop = np.unique(df_population_2020[df_population_2020['Population, total']>1500000]['Country ID'].values)
        df_all_countries = df_all_countries[df_all_countries['Country ID'].isin(medium_countries_pop)]


        # Separate into 4 countries

        df_countries = df_all_countries[~(df_all_countries['Country ID']=='asscr')].copy()
        df_countries_no_oil = df_countries.copy()

        df_countries_scr = df_all_countries[~((df_all_countries['Country ID']=='asarm') | (df_all_countries['Country ID']=='asaze') | (df_all_countries['Country ID']=='asgeo'))].copy()
        df_countries_scr_no_oil = df_countries_scr.copy()

        df_countries_no_oil = df_countries_no_oil.set_index('Country ID')
        df_countries_scr_no_oil = df_countries_scr_no_oil.set_index('Country ID')


        # 52705	Non-Petroleum Gas
        # 52709	Crude Petroleum
        # 52710	Refined Petroleum
        # 52711	Petroleum Gas
        # 52712	Petroleum Jelly
        # 52713	Petroleum Coke
        # 73911	Petroleum Resins
        oil_prods = [
            # 52705,
            52709,
            52710,
            52711,
            52712,
            73911
        ]
        for prod in oil_prods:
            df_countries_no_oil.loc['asarm',prod] = 0
            df_countries_no_oil.loc['asgeo',prod] = 0
            df_countries_no_oil.loc['asaze',prod] = 0

            df_countries_scr_no_oil.loc['asscr',prod] = 0

        df_countries_no_oil = df_countries_no_oil.reset_index()
        df_countries_scr_no_oil = df_countries_scr_no_oil.reset_index()
        logger.info("Download Ready")

        # Export tidy

        df_countries = df_countries.melt(id_vars="Country ID", value_vars= df_countries.columns[1:], value_name='trade_value').rename(columns= {'Country ID': 'oec_id', 'HS4 ID': 'hs4_id'})
        df_countries_scr = df_countries_scr.melt(id_vars="Country ID", value_vars= df_countries_scr.columns[1:], value_name='trade_value').rename(columns= {'Country ID': 'oec_id', 'HS4 ID': 'hs4_id'})
        df_countries_no_oil = df_countries_no_oil.melt(id_vars="Country ID", value_vars= df_countries_no_oil.columns[1:], value_name='trade_value').rename(columns= {'Country ID': 'oec_id', 'HS4 ID': 'hs4_id'})
        df_countries_scr_no_oil = df_countries_scr_no_oil.melt(id_vars="Country ID", value_vars= df_countries_scr_no_oil.columns[1:], value_name='trade_value').rename(columns= {'Country ID': 'oec_id', 'HS4 ID': 'hs4_id'})

        df_countries['with_oil'] = 1
        df_countries_scr['with_oil'] = 1 
        df_countries_no_oil['with_oil'] = 0
        df_countries_scr_no_oil['with_oil'] = 0 

        df_countries['with_scr'] = 0
        df_countries_scr['with_scr'] = 1
        df_countries_no_oil['with_scr'] = 0
        df_countries_scr_no_oil['with_scr'] = 1 

        df = pd.concat([df_countries, df_countries_scr, df_countries_no_oil, df_countries_scr_no_oil])
        return df


class ExportsPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))

        dtype = {
            'oec_id': 'String',
            'hs4_id': 'UInt32',
            'trade_value': 'Float64',
            'with_scr': 'UInt16',
            'with_oil': 'UInt16',
        }

        load_step = LoadStep(
            'exports_scr',
            db_connector,
            if_exists = 'drop',
            dtype = dtype,
            pk = ['oec_id', 'hs4_id', 'with_scr', 'with_oil'],
            nullable_list=['trade_value']
        )        

        download = DownloadStep()

        return [download, load_step]

if __name__ == "__main__":
    pp = ExportsPipeline()
    pp.run({})