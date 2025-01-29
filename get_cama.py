import requests

params = ['outflw','rivdph','sfcelv']

for year in range(2022,2025):
    print(year)
    
    for param in params:

        https://hydro.iis.u-tokyo.ac.jp/~dung/DAM_ERA5/outflw****.bin

        #url = f'https://hydro.iis.u-tokyo.ac.jp/~dung/NAT/{param}{year}.bin'
        
        url = f'https://hydro.iis.u-tokyo.ac.jp/~dung/DAM_ERA5/{param}{year}.bin'
        
        #user, password = 'hydrography', 'rivernetwork'
        resp = requests.get(url, allow_redirects=True) #, auth=(user, password))

        out_fold = '/nas/cee-water/cjgleason/jonathan/swot-hma/data/dam_era'

        open(f'{out_fold}/{param}{year}.bin', 'wb').write(resp.content)
