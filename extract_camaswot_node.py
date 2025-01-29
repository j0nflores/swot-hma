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
        
        #try:
            # Create a DataFrame with the date range as the only column
        df = pd.DataFrame(date_range, columns=["date"])

        #extract cama time series
        reach_df = alloc_df[alloc_df.ID==reach]
        df['dam_era_wse'] = dam_era_data[:,reach_df.iy1-1,reach_df.ix1-1].reshape(-1).astype('float')
        df['dam_vic_wse'] = dam_vic_data[:,reach_df.iy1-1,reach_df.ix1-1].reshape(-1).astype('float')
        df['nat_wse'] = nat_data[:,reach_df.iy1-1,reach_df.ix1-1].reshape(-1).astype('float')

        #load swot wse
        swot = pd.read_csv(f'./output/hydrocron_node/{reach}.csv')
        swot = swot[swot.wse>0]
        swot = swot[swot.node_q_b<100000]
        swot['date'] = swot['time_str'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
        swot = swot[['date','wse']].groupby('date').mean().reset_index()
        swot['date'] = pd.to_datetime(swot['date'])
        swot.columns = ['date','swot_wse']

        #merge dataframes and export csv
        merged = df.merge(swot,how='left',on='date').set_index('date')
        merged.to_csv(output_file)
        print('Done: ', reach)
        ##except:
            #print('Error on: ', reach)
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
alloc_df = pd.read_csv('/nas/cee-water/cjgleason/jonathan/swot-hma/data/allocation/node_alloc.csv')
#alloc_df = alloc_df[alloc_df.tag<3]
reach_list = list(alloc_df.ID) 


#configs
var = 'sfcelv' #rivdph,outflw
year = int(os.environ['SLURM_ARRAY_TASK_ID'])

#generate number of days and date range
dt = get_dt([str(year),'./data/nat',var])
date_range = pd.date_range(start=f"{year}-01-01", periods=dt)

#load cama data
dam_era_data = read_data([str(year),'./data/dam_era',var])
dam_vic_data = read_data([str(year),'./data/dam_vic',var])
nat_data = read_data([str(year),'./data/nat',var])

#create output folder
output_folder = f'./output/swot_cama_node_q/{year}'
os.makedirs(output_folder, exist_ok=True)
    
with mp.Pool(mp.cpu_count()) as p:
    p.map(extract_cama_swot,reach_list)
    p.close()
