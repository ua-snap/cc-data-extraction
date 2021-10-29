# cc-data-extraction

This is a work in progress. This script currently only extracts historical temperature data for each community.

## Setup

Clone this repository:

```
git clone git@github.com:ua-snap/cc-data-extraction.git
```

Clone the [https://github.com/ua-snap/geospatial-vector-veracity](geospatial-vector-veracity) repository next to it:

```
git clone git@github.com:ua-snap/geospatial-vector-veracity.git
```

Download the [http://ckan.snap.uaf.edu/dataset/historical-monthly-temperature-products-10-min-cru-ts-3-22](Historical Monthly Temperature Products - 10 min CRU TS 3.22) data set, and copy or link the extracted `tas` directory into the `cc-data-extraction` directory. For example:

```
cp -r ~/Downloads/tas cc-data-extraction/
```

## Run

```
python3 ./extract.py
```
