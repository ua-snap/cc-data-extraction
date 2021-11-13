import pandas as pd

alaska = pd.read_csv(
    "../geospatial-vector-veracity/vector_data/point/alaska_point_locations.csv"
)
alberta = pd.read_csv(
    "../geospatial-vector-veracity/vector_data/point/alberta_point_locations.csv"
)
british_columbia = pd.read_csv(
    "../geospatial-vector-veracity/vector_data/point/british_columbia_point_locations.csv"
)
manitoba = pd.read_csv(
    "../geospatial-vector-veracity/vector_data/point/manitoba_point_locations.csv"
)
nwt = pd.read_csv(
    "../geospatial-vector-veracity/vector_data/point/northwest_territories_point_locations.csv"
)
saskatchewan = pd.read_csv(
    "../geospatial-vector-veracity/vector_data/point/saskatchewan_point_locations.csv"
)
yukon = pd.read_csv(
    "../geospatial-vector-veracity/vector_data/point/yukon_point_locations.csv"
)

all_locations = pd.concat(
    [alaska, alberta, british_columbia, manitoba, nwt, saskatchewan, yukon]
)

non_nwt_locations = pd.concat(
    [alaska, alberta, british_columbia, manitoba, saskatchewan, yukon]
)

scenarios_lu = ["cru32", "prism", "rcp45", "rcp60", "rcp85"]

communities_lu = {
    "cru32": all_locations,
    "prism": non_nwt_locations,
    "rcp45": all_locations,
    "rcp60": all_locations,
    "rcp85": all_locations,
}

types_lu = {"tas": "Temperature", "pr": "Precipitation"}

resolutions_lu = {
    "cru32": ["10min"],
    "prism": ["2km"],
    "rcp45": ["2km", "10min"],
    "rcp60": ["2km", "10min"],
    "rcp85": ["2km", "10min"],
}

dateranges_lu = {
    "cru32": [[1960, 1989]],
    "prism": [[1961, 1990]],
    "rcp45": [[2040, 2049], [2060, 2069], [2090, 2099]],
    "rcp60": [[2040, 2049], [2060, 2069], [2090, 2099]],
    "rcp85": [[2040, 2049], [2060, 2069], [2090, 2099]],
}

projections_lu = {
    "cru32": "EPSG:4326",
    "prism": "EPSG:3338",
    "rcp45": "EPSG:3338",
    "rcp60": "EPSG:3338",
    "rcp85": "EPSG:3338",
}
