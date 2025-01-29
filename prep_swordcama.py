import geopandas as gpd
import pandas as pd
import glob


#get sword node-level dataframe
files = glob.glob('/nas/cee-water/cjgleason/data/SWORD/SWORDv16/shp/AS/as_sword_nodes*.shp')
gdfs = []
for i,shp in enumerate(files):
    gdf = gpd.read_file(shp)
    gdfs.append(gdf)

# Concatenate all GeoDataFrames
merged_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))


hma = gpd.read_file('/nas/cee-water/cjgleason/jonathan/swot-hma/data/sword/sword_hma.shp')
#hma.head()


#prep dataframe for cama allocation

outdfs = []
for reach in hma.reach_id:
    
    outlet_reach = hma[hma.reach_id==reach].copy()
    #outlet_reach = outlet_reach[['x','y','reach_id']]
    outlet_reach = outlet_reach.rename(columns={'x':'reach_x','y':'reach_y'})
    
    outlet_nodes = merged_gdf[merged_gdf.reach_id==reach].copy()
    down = outlet_nodes[outlet_nodes.node_id==outlet_nodes.node_id.min()][['x','y','node_id','reach_id']]
    up = outlet_nodes[outlet_nodes.node_id==outlet_nodes.node_id.max()][['x','y','node_id','reach_id']]

    down.columns = ['down_node_x','down_node_y','down_node_id','reach_id']
    up.columns = ['up_node_x','up_node_y','up_node_id','reach_id']

    outfile = up.merge(outlet_reach,on='reach_id')
    outfile = down.merge(outfile,on='reach_id')#.set_index('reach_id')
    outdfs.append(outfile)
    

out_main = gpd.GeoDataFrame(pd.concat(outdfs,ignore_index=True))

out_main.to_csv('./data/cama/all_reach_test.csv')