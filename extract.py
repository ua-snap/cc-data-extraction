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

def extract_data(fn, communities, daterange):
    fn_prefix = fn.split('.')[0]
    fn_parts = fn_prefix.split('_')
    month = fn_parts[6].lstrip('0')
    year = int(fn_parts[7])

    if year < daterange[0] or year > daterange[1]:
        return []

    with rasterio.open(fn) as rst:
        arr = rst.read(1)
        data = []
        for index, community in communities.iterrows():
            value = get_closest_value(arr, community)
            data.append({
                'id': community['id'],
                'month': month,
                'year': str(year),
                'value': value
            })

    return data

def run_extraction(files, communities, scenario, resolution, type, daterange):
    f = partial(extract_data, communities=communities, daterange=daterange)
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

    month_values = {}
    results = []
    for index, community in communities.iterrows():
        month_values[community['id']] = {}
        for month in months:
            month_values[community['id']][str(month)] = []

    for result in combined:
        community_id = result['id']
        month = str(result['month'])
        value = result['value']
        month_values[community_id][month].append(value)

    for index, community in communities.iterrows():
        row = {
            'community': community['name'],
            'region': community['region'],
            'country': community['country'],
            'latitude': community.loc['orig']['lat'],
            'longitude': community.loc['orig']['lon'],
            'type': type,
            'scenario': scenario,
            'resolution': resolution,
            'unit': 'C'
        }

        if daterange == [1960, 1989]:
            row['daterange'] = 'Historical'
        else:
            row['daterange'] = '{0}-{1}'.format(daterange[0], daterange[1])

        for month in months:
            month_abbr = datetime.datetime.strptime(str(month), "%m").strftime("%b").lower()
            month_label_min = month_abbr + 'Min'
            month_label_max = month_abbr + 'Max'
            month_label_mean = month_abbr + 'Mean'
            month_label_sd = month_abbr + 'Sd'
            values = np.array(month_values[community['id']][str(month)])

            if None in values:
                row[month_label_min] = 'null'
                row[month_label_mean] = 'null'
                row[month_label_max] = 'null'
                row[month_label_sd] = 'null'
            elif len(values) > 0:
                row[month_label_min] = values.min()
                row[month_label_mean] = values.mean().round(1)
                row[month_label_max] = values.max()
                row[month_label_sd] = values.std().round(1)

        results.append(row)
    return results

def project(x, projection):
    point = pyproj.Proj(projection)(x.longitude, x.latitude)
    x['orig'] = {
        'lat': x.latitude,
        'lon': x.longitude
    }
    x['proj'] = {
        'lat': point[1],
        'lon': point[0]
    }
    return x

def transform(x, meta):
    lat = x['proj']['lat']
    lon = x['proj']['lon']
    row, col = get_rowcol_from_point(lon, lat, transform=meta['transform'])
    x['rowcol'] = {
        'row': row,
        'col': col
    }
    return x

def get_closest_value(arr, community):
    rowcol = community.loc['rowcol']
    row = rowcol['row']
    col = rowcol['col']
    value = arr[row][col]
    distance = 0

    checked = {}
    checked[row] = {}
    checked[row][col] = True

    # Check points around perimeter of previously checked points.
    # TODO: Ignore the innermost set of checked points to optimize considerably.
    while np.isclose(value, -3.40E+38):
        distance += 1
        if distance > 8:
            return None

        checked_points = []
        for row in checked.keys():
            for col in checked[row].keys():
                if checked[row][col] == True:
                    checked_points.append([row, col])

        for point in checked_points:
            for direction in range(4):
                offset_row = point[0]
                offset_col = point[1]

                if direction == 0:
                    offset_row += 1
                elif direction == 1:
                    offset_row -= 1
                elif direction == 2:
                    offset_col += 1
                elif direction == 3:
                    offset_col -= 1

                if offset_row not in checked:
                    checked[offset_row] = {}
                checked[offset_row][offset_col] = True

                value = arr[offset_row][offset_col]
                if not np.isclose(value, -3.40E+38):
                    return value

    return value

def process_dataset(communities, geotiffs, scenario, resolution, type, daterange, projection):
    with rasterio.open(geotiffs[0]) as tmp:
    	meta = tmp.meta
    communities = communities.apply(project, projection=projection, axis=1)
    communities = communities.apply(transform, meta=meta, axis=1)
    return run_extraction(geotiffs, communities, scenario, resolution, type, daterange)

if __name__ == '__main__':
    alaska = pd.read_csv('../geospatial-vector-veracity/vector_data/point/alaska_point_locations.csv')
    alberta = pd.read_csv('../geospatial-vector-veracity/vector_data/point/alberta_point_locations.csv')
    british_columbia = pd.read_csv('../geospatial-vector-veracity/vector_data/point/british_columbia_point_locations.csv')
    manitoba = pd.read_csv('../geospatial-vector-veracity/vector_data/point/manitoba_point_locations.csv')
    nwt = pd.read_csv('../geospatial-vector-veracity/vector_data/point/northwest_territories_point_locations.csv')
    saskatchewan = pd.read_csv('../geospatial-vector-veracity/vector_data/point/saskatchewan_point_locations.csv')
    yukon = pd.read_csv('../geospatial-vector-veracity/vector_data/point/yukon_point_locations.csv')

    cru_communities = pd.concat([alaska, alberta, british_columbia, manitoba, nwt, saskatchewan, yukon])
    cru_temp_geotiffs = glob.glob(os.path.join('tas/', '*.tif'))
    cru_precip_geotiffs = glob.glob(os.path.join('pr/', '*.tif'))
    cru_temp_results = process_dataset(cru_communities, cru_temp_geotiffs, 'cru32', '10min', 'Temperature', [1960, 1989], 'EPSG:4326')
    cru_precip_results = process_dataset(cru_communities, cru_precip_geotiffs, 'cru32', '10min', 'Precipitation', [1960, 1989], 'EPSG:4326')

    rcp45_communities = alaska
    rcp45_geotiffs = glob.glob(os.path.join('rcp45/', '*.tif'))
    rcp45_temp_results = process_dataset(rcp45_communities, rcp45_geotiffs, 'rcp45', '2km', 'Temperature', [2040, 2049], 'EPSG:3338')

    combined_results = cru_temp_results + cru_precip_results + rcp45_temp_results

    keys = combined_results[0].keys()
    with open('data.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(combined_results)
