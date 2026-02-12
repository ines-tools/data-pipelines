# Power plants and storage component tool

## Overview
+ installation and use
+ Data sources
+ Configuration files
+ Approach

## installation and use
The power and storage component tool is meant to be used directly in the datapipelines workflow. It is not really meant to be used outside of this datapipeline as the output of this tool is an intermediate data format that relies on the ines builder to reach the ines format.

It is assumed that you already have Spine Toolbox and ines tools installed.

To add the pipeline, create a folder with the following files:
+ the data sources as explained below,
+ the assumptions file as explained below,
+ the configuration files as explained below,
+ the Power_Sector_template_DB,
+ the power_DB script.

In Spine Toolbox, create a workflow with:
+ data connections for all the data sources and configuration files,
+ a spine database for the output,
+ a tool for the power_DB script
    + make the power_DB script the main file for this tool
    + connect all the data and the database to this tool (about 10 files)
    + make the data available as tool arguments (the order does not matter)

## Data sources
[PyPSA power plant matching](https://github.com/PyPSA/powerplantmatching/blob/master/powerplants.csv) for data on existing power plants.

[PyPSA technology data](https://github.com/PyPSA/technology-data/tree/master/outputs) for more general data for future technologies or to replace missing data.

[ECB inflation data](https://github.com/ines-tools/data-pipelines/blob/main/EU_historical_inflation_ECB.csv) for discounting.

## Assumptions
There is some missing data in the data from PyPSA. Assumptions are used for that missing data. These assumptions are based on various sources. The assumptions and their sources are collected in a single assumptions file that makes a distinction between conversion technologies and storage. The distinction between existing and new units is only considered in the name with the suffix '-existing'.

## Configuration files
The script operates according to the configuration specified in "config.json".

The script makes a distinction between 4 types of years for the costs:
- euroyear: the year in which the data is formatted (it is assumed that all the input data is formatted for this year), e.g. 2020
- referenceyear: the year in which the data needs to be formatted, e.g. 2025
- baseyear: the year of the data for existing units, e.g. 2020
- milestoneyears: the years of the data for the new units, e.g. 2030, 2040 and 2050

The euroyear and the referenceyear are part of the configuration file, but the baseyear and the milestoneyears are currently hardcoded at the start of the script. In a future version of the tool, the baseyear and milestoneyears should be more flexible.

The script aggregates the positional data to areas specified by the geolevel in the general configuration file. Any geolevel that is present in the provided geojson file is valid. For the European case study that implies the levels PECD1, PECD2, NUTS2 and NUTS3. For the Industrial case study that implies the level IC1.

## Approach
At the start, the script gathers all input data and uses fuzzy search to determine in what order the input files need to be loaded. This approach allows for some errors in the filename and some errors in ordering the tool arguments.

The script makes a distinction between existing units and new units.

For the existing units, we first load the existing power plants from the power plant matching file and load the (general) technology data associated to the reference year. Mapping functions are used to match the names expected by the power db template example with the names from the technology data catalogue (originally using fuzzy search but eventually hardcoded for more precise data assumptions). The power plants are aggregated and missing data is adjusted with the general data wherever possible. The capacity decays according to the lifetime information of the existing power plant and the provided milestoneyears. If such data is missing, a random milestone year is chosen to decommission the unit.

For the new units, the general technology data is loaded for the milestone years (instead of the reference year).

If there are missing values at the end of the process, these are then replaced by values from the assumptions file.