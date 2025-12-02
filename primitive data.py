import pandas as pd
import os
import numpy as np
column_map = {
    # Labels
    "label":{'KOI':"koi_disposition",'K2':"disposition",'TOI':"tfopwg_disp"},
    # Coordinates
    "ra": {"KOI": "ra", "K2": "ra", "TOI": "ra"},
    "dec": {"KOI": "dec", "K2": "dec", "TOI": "dec"},
    # Magnitudes 
    "jmag": {"KOI": "koi_jmag", "K2": "sy_jmag", "TOI": None},
    "hmag": {"KOI": "koi_hmag", "K2": "sy_hmag", "TOI": None},
    "kmag": {"KOI": "koi_kmag", "K2": "sy_kmag", "TOI": None},
    "kepmag":{"KOI":"koi_kepmag","K2":"sy_kepmag","TOI":'st_tmag'},
    "planet_radius": {"KOI": "koi_prad", "K2": "pl_rade", "TOI": "pl_rade"},
    "orbital_period": {"KOI": "koi_period", "K2": "pl_orbper", "TOI": "pl_orbper"},
    
    "stellar_teff": {"KOI": "koi_steff", "K2": "st_teff", "TOI": "st_teff"},
    "stellar_logg": {"KOI": "koi_slogg", "K2": "st_logg", "TOI": "st_logg"},
    "stellar_radius": {"KOI": "koi_srad", "K2": "st_rad", "TOI": "st_rad"},
    "stellar_mass": {"KOI": "koi_smass", "K2": "st_mass", "TOI": None},
    "lc_time0":{'KOI':"koi_time0bk", 'K2':"pl_tranmid", 'TOI':"pl_tranmid"},
    "transit_depth": {"KOI": "koi_depth", "K2": "pl_trandep", "TOI": "pl_trandep"},
    "transit_duration": {"KOI": "koi_duration", "K2": "pl_trandur", "TOI": "pl_trandurh"},
    "lc_model_snr": {"KOI": "koi_model_snr", "K2": None, "TOI": None},
    "ror_ratio": {"KOI": "koi_ror", "K2": "pl_ratror", "TOI": None},
    "semi_major_axis": {"KOI": "koi_sma", "K2": "pl_orbsmax", "TOI": None},
    
    
    
}
def map_label(value,source):
    if pd.isna(value):
        return np.nan
    value=str(value).strip().upper()
    if source=='TOI':
        if value in ['KP']:
            return 0
        elif value in ['PC','APC','CP']:
            return 1
        elif value in ['FP']:
            return 2
        elif value in ['FA']:
            return 3
    elif source in ['KOI','K2']:
        if 'CONFIRMED' in value:
            return 0
        elif 'CANDIDATE' in value:
            return 1
        elif 'FALSE POSITIVE' in value:
            return 2
        elif 'REFUTED' in value or 'REJECTED' in value:
            return 3
    return np.nan
def merge_datasets(koi,k2,toi,output):
    koi['source']='KOI'
    k2['source']='K2'
    toi['source']='TOI'
    datasets=[]
    for df,source in zip([koi,k2,toi],['KOI','K2','TOI']):
        processed_data=[]
        for target_col,source_cols in column_map.items():
            source_col=source_cols.get(source)
            if source_col and source_col in df.columns:
                if target_col=='label':
                    mapped_values=df[source_col].apply(lambda x: map_label(x,source))
                    processed_data.append(mapped_values.rename(target_col))
                else:
                    processed_data.append(df[source_col].rename(target_col))
            elif source_col is None and source =='TOI' and target_col in ['jmag','hmag','kmag','stellar_mass']:
                if target_col=='jmag':
                    processed_data.append(pd.Series([12.6615]*len(df),name='jmag'))
                elif target_col=='hmag':
                    processed_data.append(pd.Series([12.274]*len(df),name='hmag'))
                elif target_col=='kmag':
                    processed_data.append(pd.Series([12.188]*len(df),name='kmag'))
                elif target_col=='stellar_mass':
                    processed_data.append(pd.Series([0.954]*len(df),name='stellar_mass'))
            else:
                processed_data.append(pd.Series([np.nan]*len(df),name=target_col))
        processed_data.append(df['source'].rename('source'))
        combined_df=pd.concat(processed_data,axis=1)
        datasets.append(combined_df)
    final_df=pd.concat(datasets,ignore_index=True,sort=False)
    final_df.to_csv(output,index=False)

base_dir = r'D:\u盘\u盘\大学\手打代码\统计学习'
koi_path = os.path.join(base_dir, 'koi.csv')
k2_path = os.path.join(base_dir, 'k2.csv')
toi_path = os.path.join(base_dir, 'toi.csv')
output_path = os.path.join(base_dir, 'combined_exoplanet_data1.csv')
   
try:
    print('Reading data files...') 
    koi_df = pd.read_csv(koi_path)
    k2_df = pd.read_csv(k2_path)
    toi_df = pd.read_csv(toi_path)
    print('Primitive data shapes:')
    print(f'KOI data shape: {koi_df.shape}')
    print(f'K2 data shape: {k2_df.shape}')
    print(f'TOI data shape: {toi_df.shape}','\n')
    print('Aligning data columns...')
    
    merge_datasets(koi_df, k2_df, toi_df, output_path)
    print(f'Combined dataset saved to {output_path}')

except Exception as e:
    print(f'An error occurred: {e}')

   