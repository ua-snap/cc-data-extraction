# cc-data-extraction

This script extracts temperature and precipitation values from historical and projected raster data for use with the [Community Climate Charts](https://snap.uaf.edu/tools/community-charts) web application.

## Setup

### Repositories

Clone the [geospatial-vector-veracity](https://github.com/ua-snap/geospatial-vector-veracity) repository:

```
cd ~/repos
git clone git@github.com:ua-snap/geospatial-vector-veracity.git
```

Clone this repository next to it:

```
cd ~/repos
git clone git@github.com:ua-snap/cc-data-extraction.git
cd cc-data-extraction
brew install proj
pipenv install
```

### Input Data

The extract.py script depends on the following datasets:

- [Historical Monthly Temperature Products - 10 min CRU TS 3.22](http://ckan.snap.uaf.edu/dataset/historical-monthly-temperature-products-10-min-cru-ts-3-22)
- [Historical Monthly Precipitation Products - 10 min CRU TS 3.22](http://ckan.snap.uaf.edu/dataset/historical-monthly-precipitation-products-10-min-cru-ts-3-22)
- [PRISM 1961-1990 Climatologies](http://ckan.snap.uaf.edu/dataset/prism-1961-1990-climatologies)
- [Projected Monthly and Derived Temperature Products - 2km CMIP5/AR5](http://ckan.snap.uaf.edu/dataset/projected-monthly-and-derived-temperature-products-2km-cmip5-ar5)
- [Projected Monthly and Derived Precipitation Products - 2km CMIP5/AR5](http://ckan.snap.uaf.edu/dataset/projected-monthly-and-derived-precipitation-products-2km-cmip5-ar5)
- [Projected Monthly Temperature Products - 10 min CMIP5/AR5](http://ckan.snap.uaf.edu/dataset/projected-monthly-temperature-products-10-min-cmip5-ar5)
- [Projected Monthly Precipitation Products - 10 min CMIP5/AR5](http://ckan.snap.uaf.edu/dataset/projected-monthly-precipitation-products-10-min-cmip5-ar5)

For the projected datasets, this script uses only the 5-model average. There is no need to download projected data for individual models.

These datasets need to be extracted into an `input` subdirectory with the following structure:

```
input
├── cru32
│   └── 10min
│       ├── pr
│       └── tas
├── prism
│   └── 2km
│       ├── pr
│       └── tas
├── rcp45
│   ├── 10min
│   │   ├── pr
│   │   └── tas
│   └── 2km
│       ├── pr
│       ├── tas
│       ├── tasmax
│       └── tasmin
├── rcp60
│   ├── 10min
│   │   ├── pr
│   │   └── tas
│   └── 2km
│       ├── pr
│       ├── tas
│       ├── tasmax
│       └── tasmin
└── rcp85
    ├── 10min
    │   ├── pr
    │   └── tas
    └── 2km
        ├── pr
        ├── tas
        ├── tasmax
        └── tasmin
```

## Run

```
pipenv run ./extract.py
```

The script will output the following files for use with the [Community Climate Charts web application](https://github.com/ua-snap/dash-cc):

* `CommunityNames.json`: Used to populate the community selector dropdown menu.
* `csv/*.csv`: CSV files containing historical and projected temperature and precipitation values for each community.