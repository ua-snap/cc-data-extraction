#!/usr/bin/env python
import pandas as pd
import rasterio
import numpy as np
import pyproj
import datetime
import csv
import json
import glob
import re
import os
import os.path
import logging
import multiprocessing as mp
from functools import partial
import luts

logging.basicConfig(level=logging.INFO)


def get_rowcol_from_point(x, y, transform):
    col, row = ~transform * (x, y)
    col, row = int(col), int(row)
    return row, col


def extract_data(filepath, communities, scenario, daterange):
    filename = filepath.split("/")[-1]
    filename_prefix = filename.split(".")[0]
    filename_parts = filename_prefix.split("_")

    if scenario == "prism":
        month = filename_parts[5].lstrip("0")
    else:
        month = filename_parts[6].lstrip("0")
        year = int(filename_parts[7])
        if year < daterange[0] or year > daterange[1]:
            return []

    with rasterio.open(filepath) as rst:
        arr = rst.read(1)
        data = []
        for index, community in communities.iterrows():
            value = get_closest_value(arr, community)
            data.append({"id": community["id"], "month": month, "value": value})

    return data


def run_extraction(files, communities, scenario, resolution, type, daterange):
    f = partial(
        extract_data, communities=communities, scenario=scenario, daterange=daterange
    )
    pool = mp.Pool(8)
    extracted = pool.map(f, files)
    pool.close()
    pool.join()
    pool = None

    combined = []
    for result in extracted:
        combined += result

    months = range(1, 13)

    month_values = {}
    results = []
    for index, community in communities.iterrows():
        month_values[community["id"]] = {}
        for month in months:
            month_values[community["id"]][str(month)] = []

    for result in combined:
        community_id = result["id"]
        month = str(result["month"])
        value = result["value"]
        month_values[community_id][month].append(value)

    for index, community in communities.iterrows():
        row = {
            "id": community["id"],
            "community": community["name"],
            "region": community["region"],
            "country": community["country"],
            "latitude": community.loc["orig"]["lat"],
            "longitude": community.loc["orig"]["lon"],
            "type": type,
            "scenario": scenario,
            "resolution": resolution,
            "unit": "C",
        }

        if scenario in ["cru32", "prism"]:
            row["daterange"] = "Historical"
        else:
            row["daterange"] = "{0}-{1}".format(daterange[0], daterange[1])

        data_exists = True
        for month in months:
            month_abbr = (
                datetime.datetime.strptime(str(month), "%m").strftime("%b").lower()
            )
            month_label_mean = month_abbr + "Mean"
            mean_values = np.array(month_values[community["id"]][str(month)])

            if None in mean_values:
                data_exists = False
                continue
            elif len(mean_values) > 0:
                row[month_label_mean] = mean_values.mean().round(1)

        if data_exists:
            results.append(row)

    return results


def project(x, projection):
    point = pyproj.Proj(projection)(x.longitude, x.latitude)
    x["orig"] = {"lat": x.latitude, "lon": x.longitude}
    x["proj"] = {"lat": point[1], "lon": point[0]}
    return x


def transform(x, meta):
    lat = x["proj"]["lat"]
    lon = x["proj"]["lon"]
    row, col = get_rowcol_from_point(lon, lat, transform=meta["transform"])

    x["rowcol"] = {"row": row, "col": col}

    return x


def get_closest_value(arr, community):
    rowcol = community.loc["rowcol"]
    row = rowcol["row"]
    col = rowcol["col"]
    value = arr[row][col]
    distance = 0

    checked = {}
    checked[row] = {}
    checked[row][col] = True

    # Check points around perimeter of previously checked points.
    # TODO: Ignore the innermost set of checked points to optimize considerably.
    while np.isclose(value, -3.40e38) or np.isclose(value, -9999.0):
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
                if not np.isclose(value, -3.40e38) and not np.isclose(value, -9999.0):
                    return value

    return value


def process_dataset(
    scenario, resolution, type_label, daterange, geotiffs, communities, projection
):
    with rasterio.open(geotiffs[0]) as tmp:
        meta = tmp.meta
    communities = communities.apply(project, projection=projection, axis=1)

    communities = communities.apply(
        transform, meta=meta, axis=1
    )
    return run_extraction(
        geotiffs, communities, scenario, resolution, type_label, daterange
    )


def process_scenarios(scenarios):
    for scenario in luts.scenarios_lu:
        process_resolutions(scenario, luts.resolutions_lu[scenario].keys())


def process_resolutions(scenario, resolutions):
    for resolution in resolutions:
        process_types(scenario, resolution, luts.types_lu)


def process_types(scenario, resolution, types):
    for type in types:
        path = "input/{0}/{1}/{2}/".format(scenario, resolution, type)
        geotiffs = glob.glob(os.path.join(path, "*.tif"))
        projection = luts.projections_lu[scenario]
        type_label = luts.types_lu[type]
        process_dateranges(
            scenario,
            resolution,
            type,
            type_label,
            luts.dateranges_lu[scenario],
            geotiffs,
        )


def create_csv(filename, keys):
    with open(filename, "a", newline="") as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        output_file.close()


def process_dateranges(scenario, resolution, type, type_label, dateranges, geotiffs):
    communities = luts.resolutions_lu[scenario][resolution]
    projection = luts.projections_lu[scenario]
    for daterange in dateranges:
        results = process_dataset(
            scenario,
            resolution,
            type_label,
            daterange,
            geotiffs,
            communities,
            projection,
        )
        keys = results[0].keys()

        for index, community in communities.iterrows():
            filename = "output/" + community["id"] + ".csv"
            if not os.path.exists(filename):
                create_csv(filename, keys)

            data = []
            for result in results:
                if result["id"] == community["id"]:
                    data.append(result)

            with open(filename, "a", newline="") as output_file:
                dict_writer = csv.DictWriter(output_file, keys)
                dict_writer.writerows(data)
                output_file.close()

        log_vars = [scenario, resolution, type, daterange[0], daterange[1]]

        logging.info("Complete: {0}/{1}/{2}/{3}-{4}".format(*log_vars))


if __name__ == "__main__":
    locations = luts.all_locations
    communities = {}
    for index, location in locations.iterrows():
        communities[location["id"]] = location["name"] + ", " + location["region"]

    # Output the file used to populate the web app community selector dropdown.
    community_file = open("CommunityNames.json", "w")
    json.dump(communities, community_file)
    community_file.close()

    # Process each scenario with its resolution, type, and daterang permutations
    # specified in the luts.py file.
    process_scenarios(luts.scenarios_lu)
