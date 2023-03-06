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

- [Historical Monthly Temperature Products - 10 min CRU TS 3.22](https://catalog.snap.uaf.edu/geonetwork/srv/eng/catalog.search#/metadata/38154af5-c99a-42f9-a240-6ece4f8484a2)
- [Historical Monthly Precipitation Products - 10 min CRU TS 3.22](https://catalog.snap.uaf.edu/geonetwork/srv/eng/catalog.search#/metadata/be387bf7-9018-4376-a3d2-97ba6441a30b)
- [PRISM 1961-1990 Climatologies](https://catalog.snap.uaf.edu/geonetwork/srv/eng/catalog.search#/metadata/0e8e42f7-6774-4d35-a7b3-4a82f8b48e00)
- [Projected Monthly and Derived Temperature Products - 2km CMIP5/AR5](https://catalog.snap.uaf.edu/geonetwork/srv/eng/catalog.search#/metadata/ba834996-ad15-4785-9b43-ef2af86a5ad9)
- [Projected Monthly and Derived Precipitation Products - 2km CMIP5/AR5](https://catalog.snap.uaf.edu/geonetwork/srv/eng/catalog.search#/metadata/f44595c8-5384-4c02-9ab4-f7a9c43e92eb)
- [Projected Monthly Temperature Products - 10 min CMIP5/AR5](https://catalog.snap.uaf.edu/geonetwork/srv/eng/catalog.search#/metadata/815c6708-b6cf-4a46-b5c8-344851063117)
- [Projected Monthly Precipitation Products - 10 min CMIP5/AR5](https://catalog.snap.uaf.edu/geonetwork/srv/eng/catalog.search#/metadata/0de55611-6d88-4c21-9894-95c22b404433)

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