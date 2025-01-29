import calendar
import datetime
import os
import pandas as pd

#modified https://github.com/global-hydrodynamics/CaMa-Flood_v4/blob/master/etc/validation/src/wse_validation.py
fname="/nas/cee-water/cjgleason/jonathan/swot-hma/data/map/glb_06min/params.txt"
with open(fname,"r") as f:
    lines=f.readlines()
nx   =int  ( lines[0].split()[0] )
ny   =int  ( lines[1].split()[0] )

def read_data(inputlist):
    yyyy = inputlist[0]
    indir = inputlist[1]
    var = inputlist[2]
    reach = inputlist[3]
    print (yyyy)

    # year, mon, day
    year=int(yyyy)

    # simulated water surface elevation
    fname=f'{indir}/{var}{yyyy}.bin'
    
    #get number of days
    file_size = os.path.getsize(fname)
    element_size = np.dtype(np.float32).itemsize
    dt = int(file_size // element_size / (1800*3600))

    #read file
    simfile=np.fromfile(fname,np.float32).reshape([dt,ny,nx])

    print ("-- reading simulation file:", fname )
        
    reach_df = alloc_df[alloc_df.ID==reach]
    wse = simfile[:,reach_df.d_iy1,reach_df.d_ix1].reshape(-1).astype('float')

    # Create a date range starting from January 1 with 162 days
    date_range = pd.date_range(start=f"{yyyy}-01-01", periods=dt)

    # Create a DataFrame with the date range as the only column
    df = pd.DataFrame(date_range, columns=["date"])
    df['wse'] = wse

    return df #list(wse)


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


#run parallel by reach
reach = reach_list[int(os.environ['SLURM_ARRAY_TASK_ID'])]
var = 'sfcelv' #rivdph,outflw

output_file = f'./output/swot_cama/{reach}.csv'

if not os.path.exists(output_file):
    
    #extract cama from dam era
    dfs = []
    for year in range(2022,2025):
        inlist = [str(year),'./data/dam_era',var, reach]
        data = read_data(inlist)
        dfs.append(data)
    alldf = pd.concat(dfs)

    #extract cama naturalized
    dfs = []
    for year in range(2022,2025):
        inlist = [str(year),'./data/nat',var, reach]
        data = read_data(inlist)
        dfs.append(data)
    natdf = pd.concat(dfs)
    natdf.columns = ['date','nat_wse']

    #extract cama from dam vic
    dfs = []
    for year in range(2022,2025):
        inlist = [str(year),'./data/dam_vic',var, reach]
        data = read_data(inlist)
        dfs.append(data)
    vicdf = pd.concat(dfs)
    vicdf.columns = ['date','dam_vic_wse']

    #load swot wse
    swot = pd.read_csv(f'./output/hydrocron/{reach}.csv')
    swot = swot[swot.wse>0]
    swot['date'] = swot['time_str'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
    swot = swot[['date','wse']].groupby('date').mean().reset_index()
    swot['date'] = pd.to_datetime(swot['date'])
    swot.columns = ['date','swot_wse']

    #merge dataframes to export
    #merged = swot.merge(alldf,on='date')#.set_index('date') #.head()
    merged2 = alldf.merge(vicdf,on='date')#.set_index('date')
    merged3 = merged2.merge(natdf,on='date')#.set_index('date')
    merged3 = merged3.merge(swot,on='date').set_index('date')
    merged3.to_csv(output_file, index='date')
    
else:
    print('Already done: ', reach)