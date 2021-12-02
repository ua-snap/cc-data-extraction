import pandas as pd

alaska = pd.read_csv(
    "../geospatial-vector-veracity/vector_data/point/alaska_point_locations.csv",
    keep_default_na=False,
)
alberta = pd.read_csv(
    "../geospatial-vector-veracity/vector_data/point/alberta_point_locations.csv",
    keep_default_na=False,
)
british_columbia = pd.read_csv(
    "../geospatial-vector-veracity/vector_data/point/british_columbia_point_locations.csv",
    keep_default_na=False,
)
manitoba = pd.read_csv(
    "../geospatial-vector-veracity/vector_data/point/manitoba_point_locations.csv",
    keep_default_na=False,
)
nwt = pd.read_csv(
    "../geospatial-vector-veracity/vector_data/point/northwest_territories_point_locations.csv",
    keep_default_na=False,
)
saskatchewan = pd.read_csv(
    "../geospatial-vector-veracity/vector_data/point/saskatchewan_point_locations.csv",
    keep_default_na=False,
)
yukon = pd.read_csv(
    "../geospatial-vector-veracity/vector_data/point/yukon_point_locations.csv",
    keep_default_na=False,
)
non_nwt = pd.concat([alaska, alberta, british_columbia, manitoba, saskatchewan, yukon])
all_locations = pd.concat(
    [alaska, alberta, british_columbia, manitoba, nwt, saskatchewan, yukon]
)

scenarios_lu = ["cru322", "prism", "rcp45", "rcp60", "rcp85"]

resolutions_lu = {
    "cru322": {
        "10min": nwt,
    },
    "prism": {
        "2km": non_nwt,
    },
    "rcp45": {
        "10min": nwt,
        "2km": non_nwt,
    },
    "rcp60": {
        "10min": nwt,
        "2km": non_nwt,
    },
    "rcp85": {"10min": nwt, "2km": non_nwt},
}

types_lu = {"tas": "Temperature", "pr": "Precipitation"}

dateranges_lu = {
    "cru322": [[1961, 1990]],
    "prism": [[1961, 1990]],
    "rcp45": [[2030, 2039], [2060, 2069], [2090, 2099]],
    "rcp60": [[2030, 2039], [2060, 2069], [2090, 2099]],
    "rcp85": [[2030, 2039], [2060, 2069], [2090, 2099]],
}

projections_lu = {
    "cru322": "EPSG:4326",
    "prism": "EPSG:3338",
    "rcp45": "EPSG:3338",
    "rcp60": "EPSG:3338",
    "rcp85": "EPSG:3338",
}
