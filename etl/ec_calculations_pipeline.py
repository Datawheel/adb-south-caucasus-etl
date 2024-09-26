import pandas as pd
from modules.oec import OEC
from modules.peii import peii as ec_peii
from modules.pgi import pgi as ec_pgi
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

        return df_countries, df_countries_no_oil, df_countries_scr, df_countries_scr_no_oil

class ECStep(PipelineStep):
    def run_step(self, prev_result, params):
        df_countries, df_countries_no_oil, df_countries_scr, df_countries_scr_no_oil = prev_result
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
                # [dataframe, with_scr, with_oil] 
                'countries': [df_countries.reset_index(drop = True), 0, 0, 1],
                'countries_no_oil': [df_countries_no_oil.reset_index(drop = True), 1,0, 0],
                'countries_scr': [df_countries_scr.reset_index(drop = True), 2,1, 1],
                'countries_scr_no_oil': [df_countries_scr_no_oil.reset_index(drop = True), 3, 1, 0]
        }

        if params.get('calc') == 'pgi':
            # Retrieve gini
            gini_url  = 'https://adb.datawheel.us/tesseract/data.jsonrecords?cube=Gini&drilldowns=Exporter&measures=Gini+Index'
            gini_df_url = pd.DataFrame(r.get(gini_url).json()['data'])
            gini_df = gini_df_url.copy()
            gini_df.columns = gini_df.columns.map(lambda x: x.replace(' ', '_').lower())
            gini_df = gini_df.drop(columns=['exporter'])
            gini_df = gini_df.set_index('exporter_id')
        if params.get('calc') == 'peii':
            # Retrieve emissions
            emissions_url = 'https://adb.datawheel.us/tesseract/data.jsonrecords?cube=Emissions&drilldowns=Exporter&measures=Emissions'
            emissions_df_url = pd.DataFrame(r.get(emissions_url).json()['data'])
            emissions_df = emissions_df_url.copy()
            emissions_df.columns = emissions_df.columns.map(lambda x: x.replace(' ', '_').lower())
            emissions_df = emissions_df.drop(columns=['exporter'])
            emissions_df = emissions_df.set_index('exporter_id')

        df_rca = pd.DataFrame() # |geo_id | hs4_id | method | with_oil | rca
        df_eci = pd.DataFrame() # |geo_id | method | with_oil | eci|
        df_pci = pd.DataFrame() # |hs4_id | method | with_oil | pci|
        df_proximity = pd.DataFrame() # LOGIC LAYER
        df_relatedness = pd.DataFrame() # |geo_id | hs4_id | method | with_oil | relatedness
        df_op_gain = pd.DataFrame() # |geo_id | hs4_id | method | with_oil | op_gain
        df_similarity = pd.DataFrame()
        df_pgi = pd.DataFrame()
        df_peii = pd.DataFrame()

        for df,metadata in df_dict.items():
            df = metadata[0].copy()
            dataset = metadata[1]
            with_scr = metadata[2]
            with_oil = metadata[3]

            rca = ec.rca(df.set_index('Country ID'))
            eci_value, pci_value = ec.complexity(rca)
            proximity = ec.proximity(rcas=rca)
            similarity = export_similarity_index(rca=rca)
            relatedness = ec.relatedness(rcas=rca, proximities=proximity)
            op_gain = ec.opportunity_gain(rcas = rca, proximities=proximity, pci = pci_value)

            rca = rca.reset_index().melt(id_vars = 'Country ID', value_vars = rca.columns, value_name = 'rca').rename(columns = {'Country ID':'geo_id', 'HS4 ID':'hs4_id', 'rca':'rca'})


            if params.get('calc') == 'pgi':
                pgi = ec_pgi(df.set_index("Country ID"), gini_df)
                pgi = pgi.reset_index()
                pgi['with_oil'] = with_oil
                pgi['with_scr'] = with_scr
                df_pgi = pd.concat([df_pgi, pgi])

            
            if params.get('calc') == 'peii':
                peii = ec_peii(df.set_index("Country ID"), emissions_df)
                peii = peii.reset_index()
                peii['with_oil'] = with_oil
                peii['with_scr'] = with_scr
                df_peii = pd.concat([df_peii, peii])

            rca['with_scr'] = with_scr
            rca['with_oil'] = with_oil
            df_rca = pd.concat([df_rca, rca])

            eci = eci_value.to_frame(name = 'eci').sort_values(by = 'eci',ascending=False).reset_index().rename(columns = {'Country ID':'geo_id'})
            eci['with_scr'] = with_scr
            eci['with_oil'] = with_oil
            df_eci = pd.concat([df_eci, eci])

            pci = pci_value.to_frame(name = 'pci').sort_values(by= 'pci',ascending=False).reset_index().rename(columns = {'HS4 ID':'hs4_id'})
            pci['with_scr'] = with_scr
            pci['with_oil'] = with_oil
            df_pci = pd.concat([df_pci, pci])

            relatedness = relatedness.stack().reset_index().rename(columns={'Country ID': 'geo_id', 'HS4 ID': 'hs4_id', 0: 'relatedness'}).sort_values(by = 'relatedness',ascending=False).reset_index(drop=True)
            relatedness['with_scr'] = with_scr
            relatedness['with_oil'] = with_oil
            df_relatedness = pd.concat([df_relatedness, relatedness])

            proximity = proximity.stack().to_frame().reset_index(level=1).rename(columns = {'HS4 ID': 'hs4_id_2', 0:'proximity'}).reset_index().rename(columns = {'HS4 ID': 'hs4_id_1'}).sort_values(by='proximity', ascending = False).reset_index(drop =True)
            proximity['with_scr'] = with_scr
            proximity['with_oil'] = with_oil
            df_proximity = pd.concat([df_proximity, proximity])
            
            similarity = similarity
            similarity['with_scr'] = with_scr
            similarity['with_oil'] = with_oil    
            df_similarity = pd.concat([df_similarity, similarity])

            op_gain = op_gain.stack().reset_index().rename(columns={'Country ID': 'geo_id', 'HS4 ID': 'hs4_id', 0: 'op_gain'}).sort_values(by = 'op_gain',ascending=False).reset_index(drop=True)
            op_gain['with_scr'] = with_scr
            op_gain['with_oil'] = with_oil
            df_op_gain = pd.concat([df_op_gain, op_gain])


        df_rca = df_rca.reset_index(drop = True)
        df_eci = df_eci.reset_index(drop = True)
        df_pci = df_pci.reset_index(drop = True)
        df_relatedness = df_relatedness.reset_index(drop = True)
        df_proximity = df_proximity.reset_index(drop = True)
        df_similarity = df_similarity.reset_index(drop = True).rename(columns= {'oec_id_1': 'geo_id_1', 'oec_id_2': 'geo_id_2'})
        df_op_gain = df_op_gain.reset_index(drop = True)
        
        if params.get('calc') == 'pgi':
            df_pgi = df_pgi.reset_index(drop = True)
            df_pgi.columns = ['hs4_id', 'pgi', 'with_oil', 'with_scr' ]
            
        
        if params.get('calc') == 'peii':
            df_peii = df_peii.reset_index(drop = True)
            df_peii.columns = ['hs4_id', 'peii', 'with_oil', 'with_scr']
        
        logger.info("Calculations Ready")


        if params.get('calc') == 'rca':
            return(df_rca)

        if params.get('calc') == 'eci':
            return(df_eci)

        if params.get('calc') == 'pci':
            return(df_pci)
        
        if params.get('calc') == 'relatedness':
            return(df_relatedness)
        
        if params.get('calc') == 'proximity':
            return(df_proximity)

        if params.get('calc') == 'similarity':
            return(df_similarity)
        
        if params.get('calc') == 'relatedness':
            return(df_relatedness)
        
        if params.get('calc') == 'op_gain':
            return(df_op_gain)
        if params.get('calc') == 'pgi':
            return(df_pgi)
        if params.get('calc') == 'peii':
            return(df_peii)



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
                'hs4_id': 'UInt32',
                'with_scr': 'UInt16',
                'with_oil': 'UInt16',
                'rca': 'Float64',
            }

            load_step = LoadStep(
                'rca',
                db_connector,
                if_exists = 'append',
                dtype = dtype,
                pk = ['geo_id', 'hs4_id', 'with_scr', 'with_oil'],
                nullable_list=['rca']
            )        

        if params.get('calc') == 'eci':
            dtype = {
                'geo_id': 'String',
                'eci': 'Float64',
                'with_scr': 'UInt16',
                'with_oil': 'UInt16',
            }

            load_step = LoadStep(
                'eci',
                db_connector,
                if_exists = 'append',
                dtype = dtype,
                pk = ['geo_id', 'with_scr', 'with_oil'],
                nullable_list=['eci']
            )        


        if params.get('calc') == 'pci':
            dtype = {
                'hs4_id': 'UInt32',
                'pci': 'Float64',
                'with_scr': 'UInt16',
                'with_oil': 'UInt16',
            }

            load_step = LoadStep(
                'pci',
                db_connector,
                if_exists = 'append',
                dtype = dtype,
                pk = ['hs4_id', 'with_scr', 'with_oil'],
                nullable_list=['pci']
            )        
        
        if params.get('calc') == 'relatedness':
            dtype = {
                'geo_id': 'String',
                'hs4_id': 'UInt32',
                'with_scr': 'UInt16',
                'with_oil': 'UInt16',
                'relatedness': 'Float64',
            }

            load_step = LoadStep(
                'relatedness',
                db_connector,
                if_exists = 'append',
                dtype = dtype,
                pk = ['geo_id', 'hs4_id', 'with_scr', 'with_oil'],
                nullable_list=['relatedness']
            )        
        

        if params.get('calc') == 'proximity':
            dtype = {
                'hs4_id_1': 'UInt32',
                'hs4_id_2': 'UInt32',
                'proximity': 'Float64',
                'with_scr': 'UInt16',
                'with_oil': 'UInt16',
            }

            load_step = LoadStep(
                'proximity',
                db_connector,
                if_exists = 'append',
                dtype = dtype,
                pk = ['hs4_id_1','hs4_id_2','with_scr', 'with_oil'],
                nullable_list=['proximity']
            )        


        if params.get('calc') == 'similarity':
            dtype = {
                'geo_id_1': 'String',
                'geo_id_2': 'String',
                'similarity': 'Float64',
                'with_scr': 'UInt16',
                'with_oil': 'UInt16',
            }

            load_step = LoadStep(
                'similarity',
                db_connector,
                if_exists = 'append',
                dtype = dtype,
                pk = ['geo_id_1','geo_id_2','with_scr', 'with_oil'],
                nullable_list=['similarity']
            )        

        if params.get('calc') == 'op_gain':

            dtype = {
                'geo_id': 'String',
                'hs4_id': 'UInt32',
                'with_scr': 'UInt16',
                'with_oil': 'UInt16',
                'op_gain': 'Float64',
            }

        if params.get('calc') == 'op_gain':

            dtype = {
                'geo_id': 'String',
                'hs4_id': 'UInt32',
                'with_scr': 'UInt16',
                'with_oil': 'UInt16',
                'op_gain': 'Float64',
            }

            load_step = LoadStep(
                'op_gain',
                db_connector,
                if_exists = 'append',
                dtype = dtype,
                pk = ['geo_id', 'hs4_id', 'with_scr', 'with_oil'],
                nullable_list=['op_gain']
            )        

        if params.get('calc') == 'pgi':

            dtype = {
                'hs4_id': 'UInt32',
                'with_oil': 'UInt16',
                'with_scr': 'UInt16',
                'pgi': 'Float64',
            }

            load_step = LoadStep(
                'pgi',
                db_connector,
                if_exists = 'drop',
                dtype = dtype,
                pk = ['hs4_id', 'with_oil', 'with_scr'],
                nullable_list=['pgi']
            )        


        if params.get('calc') == 'peii':

            dtype = {
                'hs4_id': 'UInt32',
                'with_oil': 'UInt16',
                'with_scr': 'UInt16',
                'peii': 'Float64',
            }

            load_step = LoadStep(
                'peii',
                db_connector,
                if_exists = 'drop',
                dtype = dtype,
                pk = ['hs4_id', 'with_oil', 'with_scr'],
                nullable_list=['peii']
            )        

        download = DownloadStep()
        ec = ECStep()




        return [download, ec, load_step]

if __name__ == "__main__":
    pp = ECPipeline()
    list_calcs = [
        # 'rca', 'eci', 'pci', 'relatedness', 'op_gain','proximity', 'similarity',
        'pgi', 'peii'
        ]
    for i in list_calcs:
        pp.run({'calc': i})