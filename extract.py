#!/usr/bin/env python
""" Generate community temperature and precipitation CSVs from raster data"""
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
import rasterio
import numpy as np
import pyproj
import luts

logging.basicConfig(level=logging.INFO)
PROCESSES = 8
OUTPUT_DIR = "output"
CSV_OUTPUT_DIR = OUTPUT_DIR + "/csv"
COMMUNITY_NAME_FILE = "CommunityNames.json"
MAX_GRID_DISTANCE = 1
MONTHS = range(1, 13)

CSV_METADATA = """# Location: {location}
# RCP scenario values represent different projected global emission scenarios based on possible human behavior. CRU 3.22 and PRISM are historical datasets.
# Mean monthly values are the average for a given month across all five models and all ten years in the decade.
# Min values represent the coolest year in the decade for a given month for the average of the five models.
# Max values represent the warmest year in the decade for a given month for the average of the five models.
"""


# Transform coordinates into grid position using affine read from GeoTIFF.
def get_rowcol_from_point(x, y, transform):
    """Transform projected lat/lon coordinate using GeoTIFF's affine"""
    col, row = ~transform * (x, y)
    col, row = int(col), int(row)
    return row, col


def extract_data(filepath, communities, scenario, daterange):
    """Combine extracted point data with community and month information"""
    filename = filepath.split("/")[-1]
    filename_prefix = filename.split(".")[0]
    filename_parts = filename_prefix.split("_")
    matches = re.search(r"(min|max)", filename_parts[0])

    if matches is None:
        stat = "mean"
    else:
        stat = matches.group(1)

    # PRISM GeoTIFFs are an average from 1961-1990, not separated by year.
    # For other GeoTIFFs, make sure we are processing the correct year range.
    if scenario == "prism":
        month = filename_parts[5].lstrip("0")
    else:
        month = filename_parts[6].lstrip("0")
        year = int(filename_parts[7])
        if year < daterange[0] or year > daterange[1]:
            return []

    # Append extracted values to data array. These values will be given more
    # structure later, after multiprocessing extraction is complete.
    with rasterio.open(filepath) as rst:
        arr = rst.read(1)
        data = []
        for _, community in communities.iterrows():
            value = get_closest_value(arr, community, scenario)
            data.append(
                {"id": community["id"], "month": month, "value": value, "stat": stat}
            )

    return data


def run_extraction(files, communities, scenario, resolution, daterange, variable):
    """Run parallel GeoTIFF extractions and combine results"""
    variable_label = luts.variables_lu[variable]

    # Set up and run multiprocessing step.
    partial_function = partial(
        extract_data,
        communities=communities,
        scenario=scenario,
        daterange=daterange,
    )
    with mp.Pool(PROCESSES) as pool:
        extracted = pool.map(partial_function, files)
        pool.close()
        pool.join()
        pool = None

    # Squish results into a single array, not an array of arrays.
    combined = []
    for result in extracted:
        combined += result

    return compile_results(
        combined, communities, scenario, resolution, variable_label, daterange
    )


def compile_results(
    combined, communities, scenario, resolution, variable_label, daterange
):
    """Compile extraction results into structure suitable for CSV output"""
    month_values = {}
    results = []

    # Create an array for each community/month combination.
    for _, community in communities.iterrows():
        month_values[community["id"]] = {}
        for month in MONTHS:
            month_values[community["id"]][str(month)] = {
                "mean": [],
                "min": [],
                "max": [],
            }

    # Populate each community/month combo array with values. These values
    # are either the annual temperature or precipitation values for this month.
    for result in combined:
        community_id = result["id"]
        month = str(result["month"])
        value = result["value"]
        if result["stat"] == "max":
            month_values[community_id][month]["max"].append(value)
        elif result["stat"] == "min":
            month_values[community_id][month]["min"].append(value)
        else:
            month_values[community_id][month]["mean"].append(value)

    for _, community in communities.iterrows():
        # Each field of the "row" dict represents a CSV file column.
        row = {
            "id": community["id"],
            "community": community["name"],
            "region": community["region"],
            "country": community["country"],
            "latitude": community.loc["orig"]["lat"],
            "longitude": community.loc["orig"]["lon"],
            "type": variable_label,
            "scenario": scenario,
            "resolution": resolution,
        }

        row["unit"] = get_unit_label(variable_label)
        row["daterange"] = get_daterange_label(scenario, daterange)

        row = populate_data(community, month_values, row, variable_label)

        # If the value for any month is missing for a particular combination of
        # community/scenario/variable/resolution/daterange, omit the entire row
        # from the CSV output.
        if row:
            results.append(row)

    return results


def populate_data(community, month_values, row, variable):
    """Populate row of CSV data and round temp/precip values as appropriate"""
    data_exists = True
    for month in MONTHS:
        month_abbr = datetime.datetime.strptime(str(month), "%m").strftime("%b").lower()
        month_label_mean = month_abbr + "Mean"
        month_label_min = month_abbr + "Min"
        month_label_max = month_abbr + "Max"

        mean_values = np.array(month_values[community["id"]][str(month)]["mean"])
        min_values = np.array(month_values[community["id"]][str(month)]["min"])
        max_values = np.array(month_values[community["id"]][str(month)]["max"])

        if None in mean_values:
            data_exists = False
            continue

        if len(mean_values) > 0:
            month_mean = mean_values.mean()
            if variable == "Temperature":
                if len(min_values) > 0:
                    row[month_label_min] = min_values.min().round(1)
                else:
                    row[month_label_min] = ""

                row[month_label_mean] = month_mean.round(1)

                if len(max_values) > 0:
                    row[month_label_max] = max_values.min().round(1)
                else:
                    row[month_label_max] = ""

            elif variable == "Precipitation":
                row[month_label_min] = ""
                row[month_label_mean] = int(month_mean.round())
                row[month_label_max] = ""

    if data_exists:
        return row

    return None


def get_unit_label(variable):
    """Output correct unit based on climate variable"""
    if variable == "Temperature":
        return "C"
    if variable == "Precipitation":
        return "mm"
    return None


def get_daterange_label(scenario, daterange):
    """Output label for historical baseline vs. projected decade"""
    if scenario in ["cru322", "prism"]:
        return "Historical"
    return "{0}-{1}".format(daterange[0], daterange[1])


def project(x, projection):
    """Project lat/lon coordinates based on GeoTIFF's EPSG code"""
    point = pyproj.Proj(projection)(x.longitude, x.latitude)

    # Keep the original lat/lon coordinates. This is useful for debugging.
    x["orig"] = {"lat": x.latitude, "lon": x.longitude}

    # Projected lat/lon coordinates are transformed into a grid position later.
    x["proj"] = {"lat": point[1], "lon": point[0]}

    return x


def transform(x, meta):
    """Transform projected coordinates into row/col values"""
    lat = x["proj"]["lat"]
    lon = x["proj"]["lon"]
    row, col = get_rowcol_from_point(lon, lat, transform=meta["transform"])
    x["rowcol"] = {"row": row, "col": col}
    return x


# If there is no data at the grid position provided by the lat/lon
# transformation, look around neighboring grid cells for data. If no data is
# found within MAX_GRID_DISTANCE, None is returned.
def get_closest_value(arr, community, scenario):
    """Return value at array position, or the closest value around it"""
    rowcol = community.loc["rowcol"]
    row = rowcol["row"]
    col = rowcol["col"]
    value = arr[row][col]
    distance = 0

    # Keep track of which grid cells have been checked.
    checked = {}
    checked[row] = {}
    checked[row][col] = True

    # Check points around perimeter of previously checked points.
    # TODO: Ignore the innermost set of checked points to optimize considerably.
    while np.isclose(value, -3.40e38) or np.isclose(value, -9999.0):
        distance += 1
        if distance > MAX_GRID_DISTANCE:
            log_vars = [community, scenario]
            logging.error("No data available: %s/%s", *log_vars)
            return None

        value = check_neighbors(arr, checked)
        if value is not None:
            return value

    # Return the value if it was found at the expected grid location without
    # looking at neighboring cells.
    return value


def check_neighbors(arr, checked):
    """Check neighboring grid cells and return first value that is found"""
    # Create an array of points checked during the previous loop iteration.
    checked_points = []
    for row in checked.keys():
        for col in checked[row].keys():
            if checked[row][col] is True:
                checked_points.append([row, col])

    # Look at neighboring cells of each previous checked point.
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

            # Return found value if it is not a nodata value.
            if not np.isclose(value, -3.40e38) and not np.isclose(value, -9999.0):
                return value

    return None


def process_dataset(scenario, resolution, daterange, geotiffs, variable):
    """Process dataset for scenario, daterange, and climate variable combo"""
    communities = luts.resolutions_lu[scenario][resolution]
    projection = luts.projections_lu[scenario]

    # Open one of the GeoTIFFs belonging to this dataset to grab the metadata
    # that is common across all of the dataset's GeoTIFFs, including the affine
    # used for transformation into a grid location.
    with rasterio.open(geotiffs[0]) as tmp:
        meta = tmp.meta

    # Project each community's lat/lon coordinated using dataset's EPSG code.
    communities = communities.apply(project, projection=projection, axis=1)

    # Transform projected lat/lon points into row/col grid locations.
    communities = communities.apply(transform, meta=meta, axis=1)

    return run_extraction(
        geotiffs, communities, scenario, resolution, daterange, variable
    )


def process_scenarios():
    """Process all scenarios according to settings in luts.py"""
    for scenario in luts.scenarios_lu:
        process_resolutions(scenario, luts.resolutions_lu[scenario].keys())


def process_resolutions(scenario, resolutions):
    """Process resolutions according to settings in luts.py"""
    for resolution in resolutions:
        process_variables(scenario, resolution)


def process_variables(scenario, resolution):
    """Process each climate variable (temperature, precipitation)"""
    variables = luts.variables_lu
    for variable in variables:
        # Expected GeoTIFF paths are explained in README.
        path = "input/{0}/{1}/{2}/".format(scenario, resolution, variable)
        geotiffs = glob.glob(os.path.join(path, "*.tif"))

        if variable == "tas":
            path = "input/{0}/{1}/tasmin/".format(scenario, resolution)
            geotiffs += glob.glob(os.path.join(path, "*.tif"))

            path = "input/{0}/{1}/tasmax/".format(scenario, resolution)
            geotiffs += glob.glob(os.path.join(path, "*.tif"))

        process_dateranges(
            scenario,
            resolution,
            variable,
            geotiffs,
        )


# Results are appended to CSV files in chunks to free up memory. Create each CSV
# file with its header row before appending data to it.
def create_csv(filename, community, keys):
    """Create CSV files with metadata and header rows, append results later"""
    location = community["name"]
    if community["alt_name"]:
        location += " (" + community["alt_name"] + ")"
    metadata = CSV_METADATA.format(location=location)
    with open(filename, "a", newline="") as output_file:
        output_file.write(metadata)
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        output_file.close()


def process_dateranges(scenario, resolution, variable, geotiffs):
    """Process each daterange for a dataset according to settings in luts.py"""
    dateranges = luts.dateranges_lu[scenario]

    for daterange in dateranges:
        results = process_dataset(
            scenario,
            resolution,
            daterange,
            geotiffs,
            variable,
        )

        populate_csvs(results, luts.resolutions_lu[scenario][resolution])

        log_vars = [scenario, resolution, variable, daterange[0], daterange[1]]
        logging.info("Complete: %s/%s/%s/%d-%d", *log_vars)


def populate_csvs(results, communities):
    """Append data extract results to community CSVs"""
    keys = results[0].keys()

    for _, community in communities.iterrows():
        filename = CSV_OUTPUT_DIR + "/" + community["id"] + ".csv"
        if not os.path.exists(filename):
            create_csv(filename, community, keys)

        data = []
        for result in results:
            if result["id"] == community["id"]:
                data.append(result)

        with open(filename, "a", newline="") as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writerows(data)
            output_file.close()


if __name__ == "__main__":
    # Create output directories if they do not exist.
    for directory in [OUTPUT_DIR, CSV_OUTPUT_DIR]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    # Output the file used to populate the web app community selector dropdown.
    locations = luts.all_locations
    communities = {}
    for index, location in locations.iterrows():
        community = {"name": location["name"], "region": location["region"]}
        if location["alt_name"] != "":
            community["alt_name"] = location["alt_name"]
        communities[location["id"]] = community
    sorted_communities = dict(sorted(communities.items(), key=lambda x: x[1]["name"]))
    with open(OUTPUT_DIR + "/" + COMMUNITY_NAME_FILE, "w") as community_file:
        json.dump(sorted_communities, community_file, indent=2)
        community_file.close()

    # Process each scenario with its resolution, variable, and daterang permutations
    # specified in the luts.py file.
    process_scenarios()
