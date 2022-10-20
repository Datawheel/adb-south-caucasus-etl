import pandas as pd
import numpy as np
import economic_complexity as ec
def peii(tbl: pd.DataFrame, emissions: pd.DataFrame) -> pd.DataFrame:

    # drop product with no exports and fill missing values with zeros
    tbl = tbl.dropna(how="all", axis=1).fillna(value=0)
    emissions = emissions.fillna(value=0)
    
    # get Mcp matrix
    rcas = ec.rca(tbl)
    m = rcas.copy()
    m[rcas >= 1] = 1
    m[rcas < 1] = 0
    
    # Ensures that the matrices are aligned by removing countries does not exist in both matrices
    tbl_geo = tbl.index
    emissions_geo = emissions.index
    intersection_geo = list(set(tbl_geo) & set(emissions_geo))
    tbl = tbl.filter(items=intersection_geo, axis=0)
    emissions = emissions.filter(items=intersection_geo, axis=0)
    m = m.filter(items=intersection_geo, axis=0)

    tbl = tbl.sort_index(ascending=True)
    emissions = emissions.sort_index(ascending=True)
    m = m.sort_index(ascending=True)
    
    # get Scp matrix
    col_sums = tbl.sum(axis=1)
    col_sums = col_sums.to_numpy().reshape((len(col_sums), 1))
    scp = np.divide(tbl, col_sums)
    
    # get Np array
    normp = m.multiply(scp).sum(axis=0)
    normp = pd.DataFrame(normp)
    
    # get PEII array
    num = m.multiply(scp).T.dot(emissions)
    
    peii = np.divide(num, normp)
    peii.rename(columns={peii.columns[0]: "peii"}, inplace=True)
    
    return peii