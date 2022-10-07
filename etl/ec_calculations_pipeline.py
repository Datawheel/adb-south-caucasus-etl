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
        df_between_AGA_initial = pd.DataFrame(r.get('https://oec.world/olap-proxy/data.jsonrecords?Exporter+Country=asarm%2Casaze%2Casgeo&Importer+Country=asarm%2Casaze%2Casgeo&Year=2017%2C2018%2C2019%2C2020&cube=trade_i_baci_a_92&drilldowns=Exporter+Country%2CHS4%2CImporter+Country&measures=Trade+Value&token={}'.format(oec_token)).json()['data'])
        df_between_AGA_initial


        df_international_initial = pd.DataFrame(r.get('https://oec.world/olap-proxy/data.jsonrecords?Year=2017%2C2018%2C2019%2C2020&cube=trade_i_baci_a_92&drilldowns=Exporter+Country%2CHS4&measures=Trade+Value&token={}'.formatoec_token)).json()['data'])
        df_international_initial = df_international_initial.pivot_table(values='Trade Value', index='Country ID', columns= 'HS4 ID', aggfunc=np.sum).reset_index()


        df_indicators_initial = pd.DataFrame(r.get('https://oec.world/olap-proxy/data.jsonrecords?Indicator=NY.GDP.MKTP.PP.KD%2CNY.GDP.PCAP.PP.KD%2CSP.POP.TOTL&Year=2020&cube=indicators_i_wdi_a&drilldowns=Country%2CIndicator&measures=Measure&token={}'.format(oec_token)).json()['data'])
        df_indicators_initial = df_indicators_initial.pivot(index = 'Country ID',columns=['Indicator'], values = ['Measure'], ).reset_index()
        df_indicators_initial.columns = ['Country ID','GDP per capita, PPP (constant 2017 international $)','GDP, PPP (constant 2017 international $)', 'Population, total']
        logger.info("Processing Download...")

        # Make copies
        df_all_countries = df_international_initial.copy()
        df_between_AGA = df_between_AGA_initial.copy()
        df_indicators = df_indicators_initial.copy()

        # Fill NaNs with 0
        df_all_countries = df_all_countries.fillna(0)
        df_between_AGA = df_between_AGA.fillna(0)
        df_indicators = df_indicators.fillna(0)

        # Add AGA as country 
        df_3aga = df_all_countries[(df_all_countries['Country ID']=='asgeo') | (df_all_countries['Country ID']=='asarm') | (df_all_countries['Country ID']=='asaze')]
        df_aga= df_3aga.replace({'Country ID': {'asarm':'AGA', 'asgeo':'AGA', 'asaze':'AGA'}}).groupby('Country ID', sort=False).sum().reset_index()
        df_all_countries = df_all_countries.append(df_aga)

        # Remove the trade betweeen AGA
        df_between_AGA_per_prod = df_between_AGA.drop(columns=['Exporter Country ID', 'Exporter Country','Importer Country ID','Importer Country','HS4']).groupby('HS4 ID').sum().reset_index()
        df_all_countries = df_all_countries.set_index('Country ID')
        for prod in set(df_between_AGA_per_prod['HS4 ID']).intersection(set(df_all_countries.columns)):
            df_all_countries.loc['AGA',prod] = df_all_countries.loc['AGA',prod] - df_between_AGA_per_prod[df_between_AGA_per_prod['HS4 ID']==prod]['Trade Value'].values[0]
        df_all_countries = df_all_countries.reset_index()

        # Calculate and Add AGA to indicators 
        aga_pop_2020 = df_indicators[df_indicators['Country ID']=='asarm']['Population, total'].values[0]\
                    + df_indicators[df_indicators['Country ID']=='asgeo']['Population, total'].values[0]\
                    + df_indicators[df_indicators['Country ID']=='asaze']['Population, total'].values[0]
        aga_gdp_2020 = df_indicators[df_indicators['Country ID']=='asarm']['GDP, PPP (constant 2017 international $)'].values[0]\
                    + df_indicators[df_indicators['Country ID']=='asgeo']['GDP, PPP (constant 2017 international $)'].values[0]\
                    + df_indicators[df_indicators['Country ID']=='asaze']['GDP, PPP (constant 2017 international $)'].values[0]
        aga_gdp_per_cap = aga_gdp_2020/aga_pop_2020
        df_indicators = df_indicators.append({'Country ID':'AGA', 'GDP, PPP (constant 2017 international $)':aga_gdp_2020, 'GDP per capita, PPP (constant 2017 international $)':aga_gdp_per_cap, 'Population, total':aga_pop_2020}, ignore_index=True)


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

        df_countries = df_all_countries[~(df_all_countries['Country ID']=='AGA')].copy()
        df_countries_no_oil = df_countries.copy()

        df_countries_aga = df_all_countries[~((df_all_countries['Country ID']=='asarm') | (df_all_countries['Country ID']=='asaze') | (df_all_countries['Country ID']=='asgeo'))].copy()
        df_countries_aga_no_oil = df_countries_aga.copy()

        df_countries_no_oil = df_countries_no_oil.set_index('Country ID')
        df_countries_aga_no_oil = df_countries_aga_no_oil.set_index('Country ID')


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

            df_countries_aga_no_oil.loc['AGA',prod] = 0

        df_countries_no_oil = df_countries_no_oil.reset_index()
        df_countries_aga_no_oil = df_countries_aga_no_oil.reset_index()
        logger.info("Download Ready")

        return df_countries, df_countries_no_oil, df_countries_aga, df_countries_aga_no_oil

class ECStep(PipelineStep):
    def run_step(self, prev_result, params):
        df_countries, df_countries_no_oil, df_countries_aga, df_countries_aga_no_oil = prev_result
        logger.info("Calculating {}...".format(params.get('calc')))
        def export_similarity_index(rca):    
            rca = np.log(rca + 1)
            numerator = rca.sub(rca.mean(axis = 1), axis = 0)
            numerator = numerator.dot(numerator.T)
            s_c_sqrd = rca.sub(rca.mean(axis = 1), axis = 0).pow(2).sum(axis=1)
            denominator = s_c_sqrd.to_frame().dot(s_c_sqrd.to_frame().T).pow(1/2)
            scc = numerator.div(denominator)
            np.fill_diagonal(scc.values, 0)
            keep = np.triu(np.ones(scc.shape)).astype('bool').reshape(scc.size)
            scc = scc.stack()[keep]
            scc = scc.to_frame().reset_index(level=1).rename(columns = {'Country ID': 'oec_id_1',0: 'similarity'}).reset_index().rename(columns = {'Country ID': 'oec_id_2'}).query("oec_id_1 != oec_id_2").sort_values(by='similarity', ascending = False).reset_index(drop =True)
            return scc
        
        df_dict = {
                # [dataframe, with_aga, with_oil] 
                'countries': [df_countries.reset_index(drop = True), 0, 0, 1],
                'countries_no_oil': [df_countries_no_oil.reset_index(drop = True), 1,0, 0],
                'countries_aga': [df_countries_aga.reset_index(drop = True), 2,1, 1],
                'countries_aga_no_oil': [df_countries_aga_no_oil.reset_index(drop = True), 3, 1, 0]
        }


        df_rca = pd.DataFrame() # |geo_id | hs4_id | method | with_oil | rca
        df_eci = pd.DataFrame() # |geo_id | method | with_oil | eci|
        df_pci = pd.DataFrame() # |hs4_id | method | with_oil | pci|
        df_proximity = pd.DataFrame() # LOGIC LAYER
        df_relatedness = pd.DataFrame() # |geo_id | hs4_id | method | with_oil | relatedness
        df_op_gain = pd.DataFrame() # |geo_id | hs4_id | method | with_oil | op_gain


        for df,metadata in df_dict.items():
            df = metadata[0].copy()
            dataset = metadata[1]
            with_aga = metadata[2]
            with_oil = metadata[3]

            
            # RCA
            rca = ec.rca(df.set_index('Country ID'))
            # ECI
            eci_value, pci_value = ec.complexity(rca)
            # PCI
            # LOGIC LAGER! ''
            proximity = ec.proximity(rcas=rca)
            similarity = export_similarity_index(rca=rca)
            # LOGIC LAGER!
            # Relatedness
            relatedness = ec.relatedness(rcas=rca, proximities=proximity)
            # OP Gain
            op_gain = ec.opportunity_gain(rcas = rca, proximities=proximity, pci = pci_value)

            rca = rca.reset_index().melt(id_vars = 'Country ID', value_vars = rca.columns, value_name = 'rca').rename(columns = {'Country ID':'geo_id', 'HS4 ID':'hs4_id', 'rca':'rca'})
            rca['dataset'] = dataset
            rca['with_aga'] = with_aga
            rca['with_oil'] = with_oil

            df_rca = pd.concat([df_rca, rca])

            eci = eci_value.to_frame(name = 'eci').sort_values(by = 'eci',ascending=False).reset_index().rename(columns = {'Country ID':'geo_id'})
            eci['dataset'] = dataset
            eci['with_aga'] = with_aga
            eci['with_oil'] = with_oil


            df_eci = pd.concat([df_eci, eci])

            pci = pci_value.to_frame(name = 'pci').sort_values(by= 'pci',ascending=False).reset_index().rename(columns = {'HS4 ID':'hs4_id'})
            pci['dataset'] = dataset
            pci['with_aga'] = with_aga
            pci['with_oil'] = with_oil


            df_pci = pd.concat([df_pci, pci])
            
            relatedness = relatedness.stack().reset_index().rename(columns={'Country ID': 'geo_id', 'HS4 ID': 'hs4_id', 0: 'relatedness'}).sort_values(by = 'relatedness',ascending=False).reset_index(drop=True)
            relatedness['dataset'] = dataset
            relatedness['with_aga'] = with_aga
            relatedness['with_oil'] = with_oil


            df_relatedness = pd.concat([df_relatedness, relatedness])

            op_gain = op_gain.stack().reset_index().rename(columns={'Country ID': 'geo_id', 'HS4 ID': 'hs4_id', 0: 'op_gain'}).sort_values(by = 'op_gain',ascending=False).reset_index(drop=True)
            op_gain['dataset'] = dataset
            op_gain['with_aga'] = with_aga
            op_gain['with_oil'] = with_oil


            df_op_gain = pd.concat([df_op_gain, op_gain])


        df_rca = df_rca.reset_index(drop = True)
        df_eci = df_eci.reset_index(drop = True)
        df_pci = df_pci.reset_index(drop = True)
        df_relatedness = df_relatedness.reset_index(drop = True)
        df_op_gain = df_op_gain.reset_index(drop = True)
        logger.info("Calculations Ready")


        if params.get('calc') == 'rca':
            return(df_rca)

        if params.get('calc') == 'eci':
            return(df_eci)

        if params.get('calc') == 'pci':
            return(df_pci)
        
        if params.get('calc') == 'relatedness':
            return(df_relatedness)
        
        if params.get('calc') == 'op_gain':
            return(df_op_gain)



class ECPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter(label='calc', name='calc', dtype=str),
        ]

    @staticmethod
    def steps(params):
        db_connector = Connector.fetch('clickhouse-database', open('../conns.yaml'))



        if params.get('calc') == 'rca':

            dtype = {
                'geo_id': 'String',
                'hs4_id': 'UInt16',
                'dataset': 'UInt16',
                'with_aga': 'UInt16',
                'with_oil': 'UInt16',
                'rca': 'Float64',
            }

            load_step = LoadStep(
                'rca',
                db_connector,
                if_exists = 'append',
                dtype = dtype,
                pk = ['geo_id', 'hs4_id', 'dataset', 'with_aga', 'with_oil'],
                nullable_list=['rca']
            )        

        if params.get('calc') == 'eci':
            dtype = {
                'geo_id': 'String',
                'eci': 'Float64',
                'dataset': 'UInt16',
                'with_aga': 'UInt16',
                'with_oil': 'UInt16',
            }

            load_step = LoadStep(
                'eci',
                db_connector,
                if_exists = 'append',
                dtype = dtype,
                pk = ['geo_id','dataset', 'with_aga', 'with_oil'],
                nullable_list=['eci']
            )        


        if params.get('calc') == 'pci':
            dtype = {
                'hs4_id': 'UInt16',
                'pci': 'Float64',
                'dataset': 'UInt16',
                'with_aga': 'UInt16',
                'with_oil': 'UInt16',
            }

            load_step = LoadStep(
                'pci',
                db_connector,
                if_exists = 'append',
                dtype = dtype,
                pk = ['hs4_id', 'dataset', 'with_aga', 'with_oil'],
                nullable_list=['pci']
            )        
        
        if params.get('calc') == 'relatedness':
            dtype = {
                'geo_id': 'String',
                'hs4_id': 'UInt16',
                'dataset': 'UInt16',
                'with_aga': 'UInt16',
                'with_oil': 'UInt16',
                'relatedness': 'Float64',
            }

            load_step = LoadStep(
                'relatedness',
                db_connector,
                if_exists = 'append',
                dtype = dtype,
                pk = ['geo_id', 'hs4_id', 'dataset', 'with_aga', 'with_oil'],
                nullable_list=['relatedness']
            )        
        
        if params.get('calc') == 'op_gain':

            dtype = {
                'geo_id': 'String',
                'hs4_id': 'UInt16',
                'dataset': 'UInt16',
                'with_aga': 'UInt16',
                'with_oil': 'UInt16',
                'op_gain': 'Float64',
            }

        if params.get('calc') == 'op_gain':

            dtype = {
                'geo_id': 'String',
                'hs4_id': 'UInt16',
                'dataset': 'UInt16',
                'with_aga': 'UInt16',
                'with_oil': 'UInt16',
                'op_gain': 'Float64',
            }

            load_step = LoadStep(
                'op_gain',
                db_connector,
                if_exists = 'append',
                dtype = dtype,
                pk = ['geo_id', 'hs4_id', 'dataset', 'with_aga', 'with_oil'],
                nullable_list=['op_gain']
            )        

        download = DownloadStep()
        ec = ECStep()




        return [download, ec, load_step]

if __name__ == "__main__":
    pp = ECPipeline()
    list_calcs = ['rca', 'eci', 'pci', 'relatedness', 'op_gain']
    for i in list_calcs:
        pp.run({'calc': i})