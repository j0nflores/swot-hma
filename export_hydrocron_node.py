import json
import glob
import requests
import pandas as pd
import geopandas as gpd
from io import StringIO
import matplotlib.pyplot as plt
import os


def get_swot(feature_id):

    if not os.path.exists(f'./output/hydrocron_node/{feature_id}.csv'):
    
        try:
            
            feature='Node'
            start_time='2022-01-01T00:00:00Z'
            end_time='2025-01-30T00:00:00Z'
            output='csv'
            #fields='reach_id,time_str,wse,width,slope,geometry'
            fields = 'reach_id,node_id,time,time_tai,time_str,lat,lon,lat_u,lon_u,river_name,wse,wse_u,wse_r_u,width,width_u,area_total,area_tot_u,area_detct,area_det_u,area_wse,layovr_val,node_dist,xtrk_dist,flow_angle,node_q,node_q_b,dark_frac,ice_clim_f,ice_dyn_f,partial_f,n_good_pix,xovr_cal_q,rdr_sig0,rdr_sig0_u,rdr_pol,geoid_hght,solid_tide,load_tidef,load_tideg,pole_tide,dry_trop_c,wet_trop_c,iono_c,xovr_cal_c,p_wse,p_wse_var,p_width,p_wid_var,p_dist_out,p_length,p_dam_id,p_n_ch_max,p_n_ch_mod,cycle_id,pass_id,continent_id,range_start_time,range_end_time,crid,geometry,sword_version,collection_shortname'

     
            hydrocron_response = requests.get(
                f"https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries?feature=Reach&feature_id={feature_id}&start_time={start_time}&end_time={end_time}&output={output}&fields={fields}"
            ).json()

            print(hydrocron_response)
            csv_str = hydrocron_response['results']['csv']
            df = pd.read_csv(StringIO(csv_str))
            df = df[df.time_str!='no_data']
            # #df['time_str'] = df.time_str.apply(lambda x: pd.to_datetime(x[0:10], format='%Y-%m-%d'))
            # df = df.groupby('time_str').mean(numeric_only=True).reset_index() #
            # df.columns = ['date', 'reach_id', 'wse', 'width', 'slope']
            df.to_csv(f'./output/hydrocron/{feature_id}.csv')
            print(f'Done: {feature_id}')
        except:
            print(f'Error: {feature_id}')
    else:
        print(f'Already done: {feature_id}')
        

import pandas as pd

nodes = pd.read_csv('/nas/cee-water/cjgleason/jonathan/swot-hma/data/allocation/node_alloc.csv')

for node in nodes.ID:
    get_swot(node)