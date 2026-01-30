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
+ data connections for the data sources and configuration files,
+ a spine database for the output; preload this database with the template,
+ a tool for the power_DB script; connect all the data and databases to this tool (and make them available as tool arguments); the order does not matter.

## Data sources
[PyPSA power plant matching](https://github.com/PyPSA/powerplantmatching/blob/master/powerplants.csv) for data on existing power plants.

[PyPSA technology data](https://github.com/PyPSA/technology-data/tree/master/outputs) for more general data for future technologies or to replace missing data.

[ECB inflation data](https://github.com/ines-tools/data-pipelines/blob/main/EU_historical_inflation_ECB.csv) for discounting.

## Assumptions
There is some missing data in the data from PyPSA. Assumptions are used for that missing data. These assumptions are based on various sources. The assumptions and their sources are collected in a single assumptions file that makes a distinction between conversion technologies and storage. The distinction between existing and new units is only considered in the name with the suffix '-existing'.

## Configuration files
The scripts aggregates the positional data to areas specified by a geojson file. Any geolevel within that geojson file can be used, but the geolevel needs to be specified in the main function.

Currently you have to enter the geolevel directy in the script but in the future this is likely to be done through a separate configuration file instead. The same holds for some other parameters like the reference year and the names for the existing/new units as well as the template (which currently has to be loaded manually in the output database).

## Approach
At the start, the script gathers all input data and uses fuzzy search to determine in what order the input files need to be loaded. This approach allows for some errors in the filename and some errors in ordering the tool arguments.

The script makes a distinction between existing units and new units.

For the existing units, we first load the existing power plants from the power plant matching file and load the (general) technology data associated to the reference year. Mapping functions are used to match the names expected by the power db template example with the names from the technology data catalogue (originally using fuzzy search but eventually hardcoded for more precise data assumptions). The power plants are aggregated and missing data is adjusted with the general data wherever possible. The capacity decays according to the lifetime information of the existing power plant and the provided milestoneyears. If such data is missing, a random milestone year is chosen to decommission the unit.

For the new units, the general technology data is loaded for the milestone years (instead of the reference year).