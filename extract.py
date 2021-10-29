#!/usr/bin/env python
import pandas as pd
import rasterio
import numpy as np
import pyproj
import datetime
import csv
import glob
import os
import multiprocessing as mp
from functools import partial

def get_rowcol_from_point(x, y, transform):
    col, row = ~transform * (x, y)
    col, row = int(col), int(row)
    return row, col

def extract_data(fn, communities):
    fn_prefix = fn.split('.')[0]
    fn_parts = fn_prefix.split('_')
    month = fn_parts[6].lstrip('0')
    year = fn_parts[7]
    with rasterio.open(fn) as rst:
        arr = rst.read(1)
        data = []
        for i in range(0, len(communities)):
            name = communities.index[i]
            x = communities[i]['rowcol']['row']
            y = communities[i]['rowcol']['col']
            data.append({
                'community': name,
                'month': month,
                'year': year,
                'temperature': arr[x][y]
            })
        return data

def run_extraction(files, communities):
    f = partial(extract_data, communities=communities)
    pool = mp.Pool(8)
    extracted = pool.map(f, files)
    pool.close()
    pool.join()
    pool = None

    combined = []
    for result in extracted:
        combined += result

    months = range(1, 13)
    years = set(map(lambda x: x['year'], combined))

    month_temps = {}
    results = []
    for community in communities.index:
        month_temps[community] = {}
        for month in months:
            month_temps[community][str(month)] = []

    for result in combined:
        community = result['community']
        month = str(result['month'])
        temperature = result['temperature']
        month_temps[community][month].append(temperature)

    for i in range(0, len(communities)):
        row = {
            'community': communities.index[i],
            'country': 'US',
            'resolution': '10min',
            'scenario': 'cru32',
            'daterange': 'Historical',
            'unit': 'C',
            'latitude': communities[i]['orig']['lat'],
            'longitude': communities[i]['orig']['lon'],
            'type': 'Temperature'
            
        }
        for month in months:
            month_abbr = datetime.datetime.strptime(str(month), "%m").strftime("%b").lower()
            month_label_min = month_abbr + 'Min'
            month_label_max = month_abbr + 'Max'
            month_label_mean = month_abbr + 'Mean'
            month_label_sd = month_abbr + 'Sd'
            temps = month_temps[community][str(month)]
            row[month_label_min] = min(temps)
            row[month_label_max] = max(temps)
            row[month_label_mean] = round(sum(temps) / len(temps), 2)
            row[month_label_sd] = np.std(temps)
            results.append(row)

    return results

def project(x):
    point = pyproj.Proj('EPSG:4326')(x.longitude, x.latitude)
    return {
        'orig': {
            'lat': x.latitude,
            'lon': x.longitude
        },
        'proj': {
            'lat': point[1],
            'lon': point[0]
        }
    }

def transform(x):
    lat = x['proj']['lat']
    lon = x['proj']['lon']
    row, col = get_rowcol_from_point(lon, lat, transform=meta['transform'])
    x['rowcol'] = {
        'row': row,
        'col': col
    }
    return x

if __name__ == '__main__':
    community_files = glob.glob(os.path.join('../geospatial-vector-veracity/vector_data/point/', '*.csv'))
    community_dfs = map(lambda x: pd.read_csv(x), community_files)
    communities_df = pd.concat(community_dfs)
    communities_df.rename(columns={'name': 'community'}, inplace=True)
    communities_df.index = communities_df['community'] 

    geotiff_files = glob.glob(os.path.join('tas/', '*.tif'))

    with rasterio.open(geotiff_files[0]) as tmp:
    	meta = tmp.meta

    projected_pts = communities_df.apply(project, axis=1)
    communities = projected_pts.apply(transform)

    results = run_extraction(geotiff_files, communities)

    keys = results[0].keys()
    with open('historical_temperatures.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(results)
