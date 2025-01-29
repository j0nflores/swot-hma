import calendar
import datetime
import os
import pandas as pd
import numpy as np
import multiprocessing as mp


def extract_cama_swot(reach):
    #run parallel by reach
    output_file = f'{output_folder}/{reach}.csv'
    
    if not os.path.exists(output_file):
        
        try:
            # Create a DataFrame with the date range as the only column
            df = pd.DataFrame(date_range, columns=["date"])

            #extract cama time series
            reach_df = alloc_df[alloc_df.ID==reach]
            df['dam_era_q'] = dam_era_data[:,reach_df.d_iy1-1,reach_df.d_ix1-1].reshape(-1).astype('float')
            df['dam_vic_q'] = dam_vic_data[:,reach_df.d_iy1-1,reach_df.d_ix1-1].reshape(-1).astype('float')
            df['nat_q'] = nat_data[:,reach_df.d_iy1-1,reach_df.d_ix1-1].reshape(-1).astype('float')

            '''#load swot wse
            swot = pd.read_csv(f'./output/hydrocron/{reach}.csv')
            swot = swot[swot.wse>0]
            swot['date'] = swot['time_str'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
            swot = swot[['date','wse']].groupby('date').mean().reset_index()
            swot['date'] = pd.to_datetime(swot['date'])
            swot.columns = ['date','swot_wse']

            #merge dataframes and export csv
            merged = df.merge(swot,how='left',on='date').set_index('date')'''
            df.to_csv(output_file)
            print('Done: ', reach)
        except:
            print('Error on: ', reach)
    else:
        print('Already done: ', reach)


def read_data(inputlist):
    yyyy = inputlist[0]
    indir = inputlist[1]
    var = inputlist[2]
    #reach = inputlist[3]

    # simulated water surface elevation
    fname=f'{indir}/{var}{yyyy}.bin'
    
    dt = get_dt(inputlist)

    #read file
    simfile=np.fromfile(fname,np.float32).reshape([dt,ny,nx])

    print ("-- reading simulation file:", fname )

    return simfile #df #list(wse)

def get_dt(inputlist):
    #get number of days
    yyyy = inputlist[0]
    indir = inputlist[1]
    var = inputlist[2]

    fname=f'{indir}/{var}{yyyy}.bin'
    file_size = os.path.getsize(fname)
    element_size = np.dtype(np.float32).itemsize
    dt = int(file_size // element_size / (1800*3600))
    
    return dt



#modified https://github.com/global-hydrodynamics/CaMa-Flood_v4/blob/master/etc/validation/src/wse_validation.py
fname="/nas/cee-water/cjgleason/jonathan/swot-hma/data/map/glb_06min/params.txt"
with open(fname,"r") as f:
    lines=f.readlines()
nx   =int  ( lines[0].split()[0] )
ny   =int  ( lines[1].split()[0] )


#load CaMa-Flood allocation files
alloc_df = pd.read_excel('./data/allocation/ToJonathan/SWOT-CaMa_alloc_1106_v0.xlsx', header=2)
alloc_df.columns = ['ID', 'd_lat', 'd_lon', 'd_area', 'd_Type', 'd_ix1', 'd_iy1', 'd_ix2', 'd_iy2',
       'd_elv_outlet', 'd_elv_gauge', 'd_elv_upst', 'd_dst_outlet', 'd_dst_upst',
       'u_lat', 'u_lon', 'u_area', 'u_Type', 'u_ix1', 'u_iy1', 'u_ix2',
       'u_iy2', 'u_elv_outlet', 'u_elv_gauge', 'u_elv_upst', 'u_dst_outlet',
       'u_dst_upst', 'tag', 'down_node_x', 'down_node_y', 'up_node_x',
       'up_node_y', 'reach_x', 'reach_y', 'reach_len', 'wse', 'width',
       'width_var', 'facc', 'slope']
alloc_df = alloc_df[alloc_df.tag<3]
reach_list = list(alloc_df.ID) 


#configs
var = 'outflw' #'sfcelv' #rivdph,outflw
year = int(os.environ['SLURM_ARRAY_TASK_ID'])

#generate number of days and date range
dt = get_dt([str(year),'./data/nat',var])
date_range = pd.date_range(start=f"{year}-01-01", periods=dt)

#load cama data
dam_era_data = read_data([str(year),'./data/dam_era',var])
dam_vic_data = read_data([str(year),'./data/dam_vic',var])
nat_data = read_data([str(year),'./data/nat',var])

#create output folder
output_folder = f'./output/cama_discharge_rev/{year}'
os.makedirs(output_folder, exist_ok=True)
    
with mp.Pool(mp.cpu_count()) as p:
    p.map(extract_cama_swot,reach_list)
    p.close()
