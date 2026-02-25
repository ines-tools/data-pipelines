"""
# Power DB

Convert Power Plant Matching (ppm) and Technology Data Repository (tdr) to the Juha driven Alvaro's Intermediate data Format (jaif) for the data pipelines (or component tools), intended to be used by the European and Industrial case study of the mopo project.

## To Do:
- [x] merge files from Alvaro
- [x] see what data there is for CC and H2 in our csv files and add to the code accordingly
- [x] similar for storage parameters
- [x] check consistency of datavalues for all data (e.g. kW vs MW)
- [x] if there is data/technologies missing, put them in a list for Fortum
    - Missing technologies:
        - OCGT-H2 not listed in tdr
        - H2 not specified as a fuel type in ppm
        - No CC found in ppm
    - Missing data:
        - Battery efficiency
        - Battery capacity - energy
        - Operational cost
            ○ CCGT+CC
            ○ CCGT-H2
            ○ OCGT+CC
            ○ SCPC+CC
            ○ bioST
            ○ bioST+CC
            ○ Fuelcell
            ○ Geothermal
- [x] add assumption parameters from an excel file to account for missing data from tdr/ppm
- [x] add final validation check to warn if None values are being added to jaif
- [x] check whether the script is compatible with the geojson file of the industrial study (only need to replace the geojson file and replace PECD1 to IC1 in the configuration file)
- [x] reject existing power plants if they are not within areas specified by the geojson file
- [x] remove y2025 from parameter maps (part is assumptions file, part is reference year)
- [x] check purging (and that there is no "unit" in the maps)
- [x] the assumptions have constant values over the years, instead have an assumption of the growth/decline over the milestoneyears? No the data is in 2025 values, the inflation will automatically change the values over the years.
- [x] difference in cost between existing tech and new tech
    - For existing tech
        - 2020 cost in 2025 EUR
    - For future tech
        - y2030 - 2030 cost in 2025 EUR
        - y2040 - 2040 cost in 2025 EUR
        - y2050 - 2050 cost in 2025 EUR
- [x] storage
    - [x] storage is the energy, storage-connection is power, both need data on
        - [x] lifetime (currently only storage has one, not the connection, we should set the default the same?)
        - [x] cost (apparently we only provide costs for the connection, i.e. power, at the moment)
    - [x] "In the DEA catalogue, you can find different cost for energy and power regarding investment cost (both storage and storage-connection). I think fom cost only for energy (storage) and operational cost only for power (storage-connection)." (Alvaro has another example in his mail)

Optional:
- [ ] Currently, for some parameters that only require 1 value in jaif, new units use the first milestoneyear for its value  while in some instances it probably should use the average over the years.
- [ ] aggregate all units by type and use them as another data (arche)type (probably requires moving loading of files from existin/new units to main function)
- [ ] allow for different values for different years in the assumption file.
- [ ] for storage, if there is no cost data for either energy or power, the energy_power_ratio needs to be specified ("ideally, investment and fixed costs in the storage and storage connection. If no investment costs for the storage connection then energy_power_ratio and operational costs only for the storage-connection")
"""

import sys
import csv
import json
import random
from pprint import pprint
from copy import deepcopy
from math import sqrt
from pprint import pprint
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from fuzzywuzzy.process import extractOne
import spinedb_api as api
from spinedb_api import purge, DatabaseMapping
import warnings
import yaml 

##########
# MAIN
##########

def add_scenario(db_map : DatabaseMapping,name_scenario : str) -> None:
    _, error = db_map.add_scenario_item(name=name_scenario)
    if error is not None:
        raise RuntimeError(error)
    
def main(
    geo,
    inf,
    bsy,
    msy,
    ppm,
    ass,
    cnf,
    tmp,
    spd,
):
    # initialise jaif structure
    jaif = {  # dictionary for intermediate data format
        "entities": [
            ["commodity", "elec", None],
            ["commodity", "CO2", None],
        ],
        "parameter_values": [],
    }

    # load configuration
    with open(cnf, "r") as f:
        config = json.load(f)
        geolevel = config["geolevel"]
        euroyear = config["euroyear"]
        referenceyear = config["referenceyear"]
        units_existing = config["units_existing"]
        units_new = config["units_new"]
        commodities = config["commodities"]

    # load geo data
    geomap = gpd.read_file(geo)
    geomap = geomap[geomap["level"] == geolevel]  # used by units existing
    regions = geomap["id"].to_list()  # used by units new
    # print("Regions")
    # pprint(regions)#debugline

    # load and calculate inflation data
    yearly_inflation = {}
    with open(inf, "r", encoding="utf-8") as file:
        csvreader = csv.reader(file)
        next(csvreader)  # skip header
        for line in csvreader:
            yearly_inflation[int(line[1])] = float(line[2]) / 100
    # print(yearly_inflation)#debugline
    inflation = 1.0
    for y in range(euroyear, referenceyear):
        inflation *= 1 - yearly_inflation[y]
    # print(inflation)#debugline

    # load and format assumptions
    ass_tech = (
        pd.read_excel(ass, sheet_name="technology")
        .replace({float("nan"): None})
        .set_index("technology")
        .to_dict("index")
    )
    ass_store = (
        pd.read_excel(ass, sheet_name="storage")
        .replace({float("nan"): None})
        .set_index("storage")
        .to_dict("index")
    )
    assumptions = ass_tech | ass_store
    assumptions = map_ass_jaif(assumptions, inflation)
    # pprint(assumptions)  # debugline

    # format data
    existing_units(
        jaif,
        assumptions,
        ppm,
        bsy,
        list(msy.keys()),
        inflation,
        geomap,
        units_existing,
        commodities,
    )
    new_units(
        jaif,
        assumptions,
        msy,
        inflation,
        regions,
        units_new,
        commodities,
    )

    # warn for none values before saving
    warn_for_none(jaif)

    # save to spine database
    with api.DatabaseMapping(spd) as target_db:
        # empty database
        # target_db.purge_items("entity")
        # target_db.purge_items("parameter_value")
        # target_db.purge_items("alternative")
        # target_db.purge_items("scenario")
        target_db.refresh_session()
        purge.purge(target_db, purge_settings=None)
        # target_db.commit_session("Purged entities and parameter values")

        versionconfig = yaml.safe_load(open(sys.argv[-1], "rb"))
        add_scenario(target_db,f"v_{versionconfig["energy_conversion"]["version"]}")

        # load template
        with open(tmp, "r") as f:
            db_template = json.load(f)
        api.import_data(
            target_db,
            entity_classes=db_template["entity_classes"],
            parameter_definitions=db_template["parameter_definitions"],
            alternatives=[["Base", None]],
        )

        # load data
        importlog = api.import_data(target_db, **jaif)
        try:
            target_db.commit_session("Added pypsa data")
        except api.exception.NothingToCommit:
            print("Warning: No new data was added to commit. This might indicate:")
            print("- No matching data found in the input files")
            print("- All data was filtered out during processing")
            print("- Data format issues preventing import")
            print("Import will continue without committing.")
    return importlog


##########
# EXISTING
##########


def existing_units(
    jaif,
    assumptions,
    ppm,
    bsy,
    milestoneyears,
    inflation,
    geomap,
    units_existing,
    commodities,
):
    """
    Adds existing units to jaif

    with parameters:
        conversion rate of 2025
        operational cost of 2025
        capacity of 2025
        technology__region : units_existing = expected capacity for y2030, y2040, y2050 based on decommissions
        technonology__to_commodity: capacity = 1.0
    """
    # load baseyear data and convert to dictionary
    unit_types = {}
    # could be done differently as unit_types[line[0]][line[1]][year][line[2]]
    for year, path in bsy.items():  # only one entry
        baseyear = year
        with open(path, "r", encoding="utf-8") as file:
            unit_types[year] = {}
            for line in csv.reader(file):
                line = map_tdr_jaif(line)
                if (
                    line[0] in units_existing
                    or line[0] in commodities
                    or line[0] == "CC"
                ) and line[2] != "unknown":
                    if (
                        line[0] not in unit_types[year]
                    ):  # to avoid stray entries, use fuzzy search of unit_types keys
                        unit_types[year][line[0]] = {}
                    unit_types[year][line[0]][line[1]] = line[2]
                    # unit_types[year][line[0]][line[1]+'_description']=line[3]+' '+line[4]+' '+line[5]
    # print("Unit Types")
    # pprint(unit_types)  # debugline

    with open(ppm, mode="r") as file:
        unit_instances = list(csv.DictReader(file))
    # print(unit_instances)#debugline
    # print("Total units before aggregation:", len(unit_instances))  # debugline

    # aggregate and clean units
    unit_instances = aggregate_units(
        unit_instances,
        assumptions,
        unit_types,
        units_existing,
        baseyear,
        milestoneyears,
        geomap,
    )
    # pprint(unit_instances)#debugline
    # pprint(unit_types)

    regionlist = []
    commoditylist = []
    technologylist = []
    # unit_type_key_list = [] # debugline

    for unit in unit_instances:
        # print([unit["region"], unit["commodity"], unit["technology"]])  # debugline
        if unit["region"] not in regionlist:
            regionlist.append(unit["region"])
            jaif["entities"].extend(
                [
                    ["region", unit["region"], None],
                ]
            )
        if unit["commodity"]:
            # commodity
            if unit["commodity"] not in commoditylist:
                commoditylist.append(unit["commodity"])
                jaif["entities"].append(["commodity", unit["commodity"], None])

        # power plant
        if unit["entityclass"] == "PP":
            if unit["technology"] not in technologylist:
                technologylist.append(unit["technology"])
                has_carbon_capture = "+CC" in unit["technology"]
                jaif["entities"].extend(
                    [
                        ["technology", unit["technology"] + "-existing", None],
                        [
                            "technology__to_commodity",
                            [unit["technology"] + "-existing", "elec"],
                            None,
                        ],
                    ]
                )
                if has_carbon_capture:
                    jaif["entities"].append(
                        [
                            "technology__to_commodity",
                            [unit["technology"] + "-existing", "CO2"],
                            None,
                        ]
                    )
                # map technology to commodity
                if unit["commodity"]:
                    jaif["entities"].extend(
                        [
                            [
                                "commodity__to_technology",
                                [unit["commodity"], unit["technology"] + "-existing"],
                                None,
                            ],
                            [
                                "commodity__to_technology__to_commodity",
                                [
                                    unit["commodity"],
                                    unit["technology"] + "-existing",
                                    "elec",
                                ],
                                None,
                            ],
                        ]
                    )
                    if has_carbon_capture:
                        jaif["entities"].append(
                            [
                                "commodity__to_technology__to_commodity",
                                [
                                    unit["commodity"],
                                    unit["technology"] + "-existing",
                                    "CO2",
                                ],
                                None,
                            ]
                        )

                _, fixed_cost_abs = calculate_investment_and_fixed_costs(
                    unit,
                    assumptions,
                    unit_types,
                    [baseyear],
                    invest_modifier=1000.0 * inflation,
                    fixed_modifier=1.0,
                )
                # print(fixed_cost_abs)  # debugline

                operational_cost_val = search_data(
                    unit,
                    assumptions,
                    unit_types,
                    unit["technology"],
                    [baseyear],
                    "operational_cost",
                    modifier=inflation,
                )
                # print(operational_cost_val)  # debugline

                jaif["parameter_values"].extend(
                    [
                        [
                            "technology__to_commodity",
                            [unit["technology"] + "-existing", "elec"],
                            "capacity",
                            1.0,
                            "Base",
                        ],
                        [
                            "technology__to_commodity",
                            [unit["technology"] + "-existing", "elec"],
                            "fixed_cost",
                            fixed_cost_abs,
                            "Base",
                        ],
                        [
                            "technology__to_commodity",
                            [unit["technology"] + "-existing", "elec"],
                            "operational_cost",
                            operational_cost_val,
                            "Base",
                        ],
                    ]
                )

                if unit["commodity"]:
                    conversion_rate = search_data(
                        unit,
                        assumptions,
                        unit_types,
                        unit["technology"],
                        [baseyear],
                        "conversion_rate",
                        prioritise_assumption=True,
                    )
                    if conversion_rate is not None:
                        jaif["parameter_values"].append(
                            [
                                "commodity__to_technology__to_commodity",
                                [
                                    unit["commodity"],
                                    unit["technology"] + "-existing",
                                    "elec",
                                ],
                                "conversion_rate",
                                conversion_rate,
                                "Base",
                            ]
                        )

                    co2_captured = search_data(
                        unit,
                        assumptions,
                        unit_types,
                        unit["technology"],
                        [baseyear],
                        "CO2_captured",
                        prioritise_assumption=True,
                    )
                    if has_carbon_capture and co2_captured is not None:
                        jaif["parameter_values"].append(
                            [
                                "commodity__to_technology__to_commodity",
                                [
                                    unit["commodity"],
                                    unit["technology"] + "-existing",
                                    "CO2",
                                ],
                                "CO2_captured",
                                co2_captured,
                                "Base",
                            ]
                        )

            jaif["entities"].extend(
                [
                    [
                        "technology__region",
                        [unit["technology"] + "-existing", unit["region"]],
                        None,
                    ],
                ]
            )

            units_existing_val = search_data(
                unit,
                assumptions,
                unit_types,
                unit["technology"],
                milestoneyears,
                "capacity",
                data=[[year, unit["capacity"][year]] for year in milestoneyears],
            )
            jaif["parameter_values"].extend(
                [
                    [
                        "technology__region",
                        [unit["technology"] + "-existing", unit["region"]],
                        "units_existing",
                        units_existing_val,
                        "Base",
                    ],
                ]
            )
            # pprint(year_data(unit, unit_types,unit_types_key, "efficiency"))
        # if unit["entityclass"]=="CHP": # skip

        # storage
        if unit["entityclass"] == "Store":
            # map_tdr needs to be updated with storage bicharging and storage and so does this part
            if unit["technology"] not in technologylist:
                technologylist.append(unit["technology"])
                jaif["entities"].extend(
                    [
                        ["storage", unit["technology"] + "-existing", None],
                        [
                            "storage_connection",
                            [unit["technology"] + "-existing", "elec"],
                            None,
                        ],
                    ]
                )
                efficiency_in_val = search_data(
                    unit,
                    assumptions,
                    unit_types,
                    unit["technology"],
                    [baseyear],
                    "efficiency",
                    modifier=1 / sqrt(2),
                )
                efficiency_out_val = search_data(
                    unit,
                    assumptions,
                    unit_types,
                    unit["technology"],
                    [baseyear],
                    "efficiency",
                    modifier=1 / sqrt(2),
                )
                jaif["parameter_values"].extend(
                    [
                        [
                            "storage_connection",
                            [unit["technology"] + "-existing", "elec"],
                            "efficiency_in",
                            efficiency_in_val,
                            "Base",
                        ],
                        [
                            "storage_connection",
                            [unit["technology"] + "-existing", "elec"],
                            "efficiency_out",
                            efficiency_out_val,
                            "Base",
                        ],
                    ]
                )

                # Calculate storage energy and power costs separately for existing units
                # Energy costs (for storage entity)
                storage_energy_invest_cost, storage_energy_fixed_cost = (
                    calculate_investment_and_fixed_costs(
                        unit,
                        assumptions,
                        unit_types,
                        [baseyear],
                        invest_modifier=1000.0 * inflation,
                        fixed_modifier=1.0,
                        invest_param="investment_cost_energy",
                        fixed_param="fixed_cost_energy",
                    )
                )

                # Power costs (for storage_connection entity)
                storage_power_invest_cost, storage_power_fixed_cost = (
                    calculate_investment_and_fixed_costs(
                        unit,
                        assumptions,
                        unit_types,
                        [baseyear],
                        invest_modifier=1000.0 * inflation,
                        fixed_modifier=1.0,
                        invest_param="investment_cost_power",
                        fixed_param="fixed_cost_power",
                    )
                )

                storage_operational_cost_existing = search_data(
                    unit,
                    assumptions,
                    unit_types,
                    unit["technology"],
                    [baseyear],
                    "operational_cost",
                    modifier=inflation,
                )

                jaif["parameter_values"].extend(
                    [
                        [
                            "storage",
                            unit["technology"] + "-existing",
                            "investment_cost",
                            storage_energy_invest_cost,
                            "Base",
                        ],
                        [
                            "storage",
                            unit["technology"] + "-existing",
                            "fixed_cost",
                            storage_energy_fixed_cost,
                            "Base",
                        ],
                        [
                            "storage_connection",
                            [unit["technology"] + "-existing", "elec"],
                            "investment_cost",
                            storage_power_invest_cost,
                            "Base",
                        ],
                        [
                            "storage_connection",
                            [unit["technology"] + "-existing", "elec"],
                            "fixed_cost",
                            storage_power_fixed_cost,
                            "Base",
                        ],
                        [
                            "storage_connection",
                            [unit["technology"] + "-existing", "elec"],
                            "operational_cost",
                            storage_operational_cost_existing,
                            "Base",
                        ],
                    ]
                )

            # Calculate storages_existing value
            storages_existing_val = search_data(
                unit,
                assumptions,
                unit_types,
                unit["technology"],
                [baseyear],
                "capacity",
                data=[[k, v] for k, v in unit["capacity"].items()],
            )

            # For battery storage, only create storage__region if there are existing storages
            should_create_storage_region = True
            if unit["technology"] in ["battery-storage", "battery-storage-iron-air"]:
                # Check if storages_existing_val has any non-None, non-zero values
                should_create_storage_region = False
                if storages_existing_val is not None:
                    if (
                        isinstance(storages_existing_val, dict)
                        and "data" in storages_existing_val
                    ):
                        # Check if any year has a non-zero value
                        for year_data in storages_existing_val["data"]:
                            if (
                                len(year_data) >= 2
                                and year_data[1] is not None
                                and year_data[1] != 0
                            ):
                                should_create_storage_region = True
                                break
                    elif storages_existing_val != 0:
                        should_create_storage_region = True

            if should_create_storage_region:
                jaif["entities"].extend(
                    [
                        [
                            "storage__region",
                            [unit["technology"] + "-existing", unit["region"]],
                            None,
                        ],
                    ]
                )
                jaif["parameter_values"].extend(
                    [
                        [
                            "storage__region",
                            [unit["technology"] + "-existing", unit["region"]],
                            "storages_existing",
                            storages_existing_val,
                            "Base",
                        ],
                    ]
                )

    return jaif


##########
# NEW
##########


def new_units(
    jaif,
    assumptions,
    msy,
    inflation,
    regions,
    units_new,
    commodities,
):
    """
    Adds new units to jaif

    with parameters:
        lifetime
        investment_cost map for y2030, y2040, y2050
        fixed_cost map for y2030, y2040, y2050
        operational_cost map for y2030, y2040, y2050
        average CO2_captured
        average conversion_rate
        capacity = 1 from asset to main commodity
    """
    unit_types = {}
    # could be done differently as unit_types[line[0]][line[1]][year][line[2]]
    for year, path in msy.items():  # only one entry
        with open(path, "r", encoding="utf-8") as file:
            unit_types[year] = {}
            for line in csv.reader(file):
                line = map_tdr_jaif(line)
                if (
                    line[0] in units_new or line[0] in commodities or line[0] == "CC"
                ) and line[2] != "unknown":
                    if (
                        line[0] not in unit_types[year]
                    ):  # to avoid stray entries, use fuzzy search of unit_types keys
                        unit_types[year][line[0]] = {}
                    unit_types[year][line[0]][line[1]] = line[2]
                    # unit_types[year][line[0]][line[1]+'_description']=line[3]+' '+line[4]+' '+line[5]
    # print("Unit Types")
    # pprint(unit_types)  # debugline

    unit_instances = generate_unit_instances(regions, units_new)

    regionlist = []
    commoditylist = []
    technologylist = []
    # unit_type_key_list = [] # for debugging
    years = list(msy.keys())
    for unit in unit_instances:
        # print([unit["region"],unit["commodity"],unit["technology"]])#debugline
        if unit["region"] not in regionlist:
            regionlist.append(unit["region"])
            jaif["entities"].extend(
                [
                    ["region", unit["region"], None],
                ]
            )
        if unit["commodity"]:
            # commodity
            if unit["commodity"] not in commoditylist:
                commoditylist.append(unit["commodity"])
                jaif["entities"].append(["commodity", unit["commodity"], None])
        # power plant
        if unit["entityclass"] == "PP":
            if unit["technology"] not in technologylist:
                technologylist.append(unit["technology"])
                has_carbon_capture = "+CC" in unit["technology"]

                jaif["entities"].extend(
                    [
                        ["technology", unit["technology"], None],
                        [
                            "technology__to_commodity",
                            [unit["technology"], "elec"],
                            None,
                        ],
                    ]
                )
                if has_carbon_capture:
                    jaif["entities"].append(
                        [
                            "technology__to_commodity",
                            [unit["technology"], "CO2"],
                            None,
                        ]
                    )

                if unit["commodity"]:
                    jaif["entities"].extend(
                        [
                            [
                                "commodity__to_technology",
                                [unit["commodity"], unit["technology"]],
                                None,
                            ],
                            [
                                "commodity__to_technology__to_commodity",
                                [unit["commodity"], unit["technology"], "elec"],
                                None,
                            ],
                        ]
                    )
                    if has_carbon_capture:
                        jaif["entities"].append(
                            [
                                "commodity__to_technology__to_commodity",
                                [unit["commodity"], unit["technology"], "CO2"],
                                None,
                            ]
                        )

                # Calculate investment and fixed costs
                invest_cost, fixed_cost = calculate_investment_and_fixed_costs(
                    unit,
                    assumptions,
                    unit_types,
                    years,
                    invest_modifier=1000.0 * inflation,
                    fixed_modifier=1.0,
                )

                lifetime_val = search_data(
                    unit,
                    assumptions,
                    unit_types,
                    unit["technology"],
                    [years[0]],
                    "lifetime",
                )
                operational_cost_new = search_data(
                    unit,
                    assumptions,
                    unit_types,
                    unit["technology"],
                    years,
                    "operational_cost",
                    modifier=inflation,
                )

                jaif["parameter_values"].extend(
                    [
                        [
                            "technology",
                            unit["technology"],
                            "lifetime",
                            lifetime_val,
                            "Base",
                        ],
                        [
                            "technology__to_commodity",
                            [unit["technology"], "elec"],
                            "capacity",
                            1.0,
                            "Base",
                        ],
                        [
                            "technology__to_commodity",
                            [unit["technology"], "elec"],
                            "investment_cost",
                            invest_cost,
                            "Base",
                        ],
                        [
                            "technology__to_commodity",
                            [unit["technology"], "elec"],
                            "fixed_cost",
                            fixed_cost,
                            "Base",
                        ],
                        [
                            "technology__to_commodity",
                            [unit["technology"], "elec"],
                            "operational_cost",
                            operational_cost_new,
                            "Base",
                        ],
                    ]
                )

                if unit["commodity"]:
                    conversion_rate = search_data(
                        unit,
                        assumptions,
                        unit_types,
                        unit["technology"],
                        [years[0]],
                        "conversion_rate",
                        prioritise_assumption=True,
                    )
                    if conversion_rate is not None:
                        jaif["parameter_values"].append(
                            [
                                "commodity__to_technology__to_commodity",
                                [unit["commodity"], unit["technology"], "elec"],
                                "conversion_rate",
                                conversion_rate,
                                "Base",
                            ]
                        )

                    co2_captured = search_data(
                        unit,
                        assumptions,
                        unit_types,
                        unit["technology"],
                        [years[0]],
                        "CO2_captured",
                        prioritise_assumption=True,
                    )
                    if has_carbon_capture and co2_captured is not None:
                        jaif["parameter_values"].append(
                            [
                                "commodity__to_technology__to_commodity",
                                [unit["commodity"], unit["technology"], "CO2"],
                                "CO2_captured",
                                co2_captured,
                                "Base",
                            ]
                        )

            jaif["entities"].extend(
                [
                    ["technology__region", [unit["technology"], unit["region"]], None],
                ]
            )
            # pprint(year_data(unit, unit_types,unit_types_key, "efficiency"))
        # if unit["entityclass"]=="CHP": # skip

        # storage
        if unit["entityclass"] == "Store":
            # map_tdr needs to be updated with storage bicharging and storage and so does this part
            if unit["technology"] not in technologylist:
                technologylist.append(unit["technology"])
                jaif["entities"].extend(
                    [
                        ["storage", unit["technology"], None],
                        ["storage_connection", [unit["technology"], "elec"], None],
                    ]
                )

                storage_efficiency_in = search_data(
                    unit,
                    assumptions,
                    unit_types,
                    unit["technology"],
                    [years[0]],
                    "efficiency",
                    modifier=1 / sqrt(2),
                )
                storage_efficiency_out = search_data(
                    unit,
                    assumptions,
                    unit_types,
                    unit["technology"],
                    [years[0]],
                    "efficiency",
                    modifier=1 / sqrt(2),
                )

                jaif["parameter_values"].extend(
                    [
                        [
                            "storage_connection",
                            [unit["technology"], "elec"],
                            "efficiency_in",
                            storage_efficiency_in,
                            "Base",
                        ],
                        [
                            "storage_connection",
                            [unit["technology"], "elec"],
                            "efficiency_out",
                            storage_efficiency_out,
                            "Base",
                        ],
                    ]
                )

                # Calculate storage energy and power costs separately
                # Energy costs (for storage entity)
                storage_energy_invest_cost, storage_energy_fixed_cost = (
                    calculate_investment_and_fixed_costs(
                        unit,
                        assumptions,
                        unit_types,
                        years,
                        invest_modifier=1000.0 * inflation,
                        fixed_modifier=1.0,
                        invest_param="investment_cost_energy",
                        fixed_param="fixed_cost_energy",
                    )
                )

                # Power costs (for storage_connection entity)
                storage_power_invest_cost, storage_power_fixed_cost = (
                    calculate_investment_and_fixed_costs(
                        unit,
                        assumptions,
                        unit_types,
                        years,
                        invest_modifier=1000.0 * inflation,
                        fixed_modifier=1.0,
                        invest_param="investment_cost_power",
                        fixed_param="fixed_cost_power",
                    )
                )

                storage_lifetime = search_data(
                    unit, assumptions, unit_types, unit["technology"], years, "lifetime"
                )

                storage_operational_cost = search_data(
                    unit,
                    assumptions,
                    unit_types,
                    unit["technology"],
                    years,
                    "operational_cost",
                    modifier=inflation,
                )

                jaif["parameter_values"].extend(
                    [
                        [
                            "storage",
                            unit["technology"],
                            "lifetime",
                            storage_lifetime,
                            "Base",
                        ],
                        [
                            "storage",
                            unit["technology"],
                            "investment_cost",
                            storage_energy_invest_cost,
                            "Base",
                        ],
                        [
                            "storage",
                            unit["technology"],
                            "fixed_cost",
                            storage_energy_fixed_cost,
                            "Base",
                        ],
                        [
                            "storage_connection",
                            [unit["technology"], "elec"],
                            "lifetime",
                            storage_lifetime,
                            "Base",
                        ],
                        [
                            "storage_connection",
                            [unit["technology"], "elec"],
                            "investment_cost",
                            storage_power_invest_cost,
                            "Base",
                        ],
                        [
                            "storage_connection",
                            [unit["technology"], "elec"],
                            "fixed_cost",
                            storage_power_fixed_cost,
                            "Base",
                        ],
                        [
                            "storage_connection",
                            [unit["technology"], "elec"],
                            "operational_cost",
                            storage_operational_cost,
                            "Base",
                        ],
                    ]
                )

            # Don't create storage__region entities for battery storage technologies
            if unit["technology"] not in [
                "battery-storage",
                "battery-storage-iron-air",
            ]:
                jaif["entities"].extend(
                    [
                        ["storage__region", [unit["technology"], unit["region"]], None],
                    ]
                )

    return jaif


#########
# EXTRA
#########


def generate_unit_instances(regions, units):
    map_jaif = {
        "bioST": {
            "commodity": "bio",
            "technology": "bioST",
            "entityclass": "PP",
        },
        # "bioST+CC": {
        #     "commodity": "bio",
        #     "technology": "bioST+CC",
        #     "entityclass": "PP",
        # },
        "CCGT": {
            "commodity": "CH4",
            "technology": "CCGT",
            "entityclass": "PP",
        },
        "CCGT+CC": {
            "commodity": "CH4",
            "technology": "CCGT+CC",
            "entityclass": "PP",
        },
        "CCGT-H2": {
            "commodity": "H2",
            "technology": "CCGT-H2",
            "entityclass": "PP",
        },
        "fuelcell": {
            "commodity": "H2",
            "technology": "fuelcell",
            "entityclass": "PP",
        },
        "geothermal": {
            "commodity": None,
            "technology": "geothermal",
            "entityclass": "PP",
        },
        "nuclear-3": {
            "commodity": "U-92",
            "technology": "nuclear-3",
            "entityclass": "PP",
        },
        "OCGT": {
            "commodity": "CH4",
            "technology": "OCGT",
            "entityclass": "PP",
        },
        # "OCGT+CC": {
        #     "commodity": "CH4",
        #     "technology": "OCGT+CC",
        #     "entityclass": "PP",
        # },
        "OCGT-H2": {
            "commodity": "H2",
            "technology": "OCGT-H2",
            "entityclass": "PP",
        },
        "oil-eng": {
            "commodity": "HC",
            "technology": "oil-eng",
            "entityclass": "PP",
        },
        "SCPC": {
            "commodity": "coal",
            "technology": "SCPC",
            "entityclass": "PP",
        },
        "SCPC+CC": {
            "commodity": "coal",
            "technology": "SCPC+CC",
            "entityclass": "PP",
        },
        "battery-storage": {
            "commodity": "elec",
            "technology": "battery-storage",
            "entityclass": "Store",
        },
        "battery-storage-iron-air": {
            "commodity": "elec",
            "technology": "battery-storage-iron-air",
            "entityclass": "Store",
        },
    }
    unit_instances = []
    for region in regions:
        for unit in units:
            if unit in map_jaif:
                unit_jaif = map_jaif[
                    unit
                ].copy()  # Create a copy instead of referencing the original
                unit_jaif["region"] = region
                unit_instances.append(unit_jaif)
    return unit_instances


def aggregate_units(
    unit_instances,
    assumptions,
    unit_types,
    units,
    baseyear,
    milestoneyears,
    geomap,
    average_parameters=["conversion_rate"],
    sum_parameters=[],
    cumulative_parameters=["capacity"],
):
    """
    Aggregate and clean units
    """
    aggregated_units = {}
    for unit in unit_instances:
        # original_unit = unit.copy()  # debugline
        # print(unit["Country"])  # debugline
        unit = map_ppm_jaif(unit)
        unit["region"] = get_region(unit, geomap)
        # print(unit["region"])  # debugline

        """
        #region debugblock: Check if mapping worked for storage - ONLY for 'Other' fuel type
        if original_unit.get('Set') == 'Store' and original_unit.get('Fueltype') == 'Other':
            fuel = original_unit.get('Fueltype')
            tech = original_unit.get('Technology')
            set_val = original_unit.get('Set')
            print(f"OTHER STORAGE MAPPING: Key=({fuel}, {tech}, {set_val})")
            print(f"  -> Mapped=({unit.get('commodity')}, {unit.get('technology')}, {unit.get('entityclass')})")
            if unit.get('commodity') == 'unknown':
                print(f"  -> MAPPING FAILED! No mapping found for key ({fuel}, {tech}, {set_val})")
        if unit["entityclass"] == "Store":
            print(f"STORAGE UNIT FOUND: {unit['commodity']}, {unit['technology']}, {unit['entityclass']}, Region: {unit.get('region', 'NOT_SET')}")
        #endregion debugblock
        """

        if (unit["technology"] in units) and unit["region"]:
            # tuple for aggregating
            unit_tuple = tuple(
                [unit[key] for key in ["commodity", "technology", "region"]]
            )
            if unit_tuple not in aggregated_units.keys():
                # print(unit_tuple)# debugline
                # initialise
                aggregated_units[unit_tuple] = deepcopy(unit)
                aggregated_unit = aggregated_units[unit_tuple]
                for parameter in average_parameters:
                    unit[parameter] = search_data(
                        unit,
                        assumptions,
                        unit_types,
                        unit["technology"],
                        [baseyear],
                        parameter,
                    )
                    if unit[parameter]:
                        aggregated_unit[parameter] = float(unit[parameter])
                    else:
                        aggregated_unit[parameter] = None
                for parameter in sum_parameters:
                    unit[parameter] = search_data(
                        unit,
                        assumptions,
                        unit_types,
                        unit["technology"],
                        [baseyear],
                        parameter,
                    )
                    if unit[parameter]:
                        aggregated_unit[parameter] = float(unit[parameter])
                    else:
                        aggregated_unit[parameter] = None
                for parameter in cumulative_parameters:
                    if parameter == "capacity":
                        lifetime = search_data(
                            unit,
                            assumptions,
                            unit_types,
                            unit["technology"],
                            [baseyear],
                            "lifetime",
                        )
                        unit["capacity"] = decay_capacity(
                            unit, lifetime, milestoneyears
                        )
                    if unit[parameter]:
                        aggregated_unit[parameter] = deepcopy(unit[parameter])
                    else:
                        aggregated_unit[parameter] = None
            else:
                # aggregate
                aggregated_unit = aggregated_units[unit_tuple]
                for parameter in average_parameters:
                    unit[parameter] = search_data(
                        unit,
                        assumptions,
                        unit_types,
                        unit["technology"],
                        [baseyear],
                        parameter,
                    )
                    if aggregated_unit[parameter] and unit[parameter]:
                        aggregated_unit[parameter] = (
                            float(aggregated_unit[parameter]) + float(unit[parameter])
                        ) / 2
                    elif unit[parameter]:
                        aggregated_unit[parameter] = float(unit[parameter])
                for parameter in sum_parameters:
                    unit[parameter] = search_data(
                        unit,
                        assumptions,
                        unit_types,
                        unit["technology"],
                        [baseyear],
                        parameter,
                    )
                    if aggregated_unit[parameter] and unit[parameter]:
                        aggregated_unit[parameter] = float(
                            aggregated_unit[parameter]
                        ) + float(unit[parameter])
                    elif unit[parameter]:
                        aggregated_unit[parameter] = float(unit[parameter])
                for parameter in cumulative_parameters:
                    if parameter == "capacity":
                        lifetime = search_data(
                            unit,
                            assumptions,
                            unit_types,
                            unit["technology"],
                            [baseyear],
                            "lifetime",
                        )
                        unit["capacity"] = decay_capacity(
                            unit, lifetime, milestoneyears
                        )
                    # else assume data is already in correct format
                    if aggregated_unit[parameter] and unit[parameter]:
                        for year in aggregated_unit[parameter].keys():
                            aggregated_unit[parameter][year] += unit[parameter][year]
                    elif unit[parameter]:
                        aggregated_unit[parameter] = deepcopy(unit[parameter])
    return aggregated_units.values()


def get_region(unit, geomap):
    """
    Get region associated to the lat/lon coordinates of the unit.
    Return None if there is no polygon around a point.
    Warn for overlapping polygons.
    """
    lat = float(unit["lat"])
    lon = float(unit["lon"])
    point = Point(lon, lat)
    pip = geomap.contains(point)
    region = None
    for index, value in enumerate(pip):
        if value:
            if region:
                overlap_region = geomap.iloc[index]["id"]
                print(
                    "Warning: Overlapping polygons, choosing "
                    + region
                    + " over "
                    + overlap_region
                )
            else:
                region = geomap.iloc[index]["id"]
    return region


def decay_capacity(unit, lifetime, milestoneyears):  # baseyear
    capacity = {}  # {baseyear: float(unit["capacity"])}
    # try to use dateout
    if unit["date_out"]:
        for milestoneyear in milestoneyears:
            if int(milestoneyear[1:]) < int(float(unit["date_out"])):
                capacity[milestoneyear] = float(unit["capacity"])
            else:
                capacity[milestoneyear] = 0.0
    elif unit["date_in"] and lifetime:
        for milestoneyear in milestoneyears:
            if int(milestoneyear[1:]) < int(float(unit["DateIn"])) + int(
                float(lifetime)
            ):
                capacity[milestoneyear] = float(unit["capacity"])
            else:
                capacity[milestoneyear] = 0.0
    else:
        randomyear = random.choice(milestoneyears)
        for milestoneyear in milestoneyears:
            if int(milestoneyear[1:]) < int(randomyear[1:]):
                capacity[milestoneyear] = float(unit["capacity"])
            else:
                capacity[milestoneyear] = 0.0
    return capacity


def map_ass_jaif(
    ass,
    inflation,
    costparameters=[
        "investment_cost",
        "fixed_cost",
        "operational_cost",
        "investment_cost_energy",
        "investment_cost_power",
        "fixed_cost_energy",
        "fixed_cost_power",
    ],
):
    assumptions = {}
    for tech, properties in ass.items():
        assumptions[tech] = {}
        for k, v in properties.items():
            # Strip newlines and extra whitespace, then get first part before space
            key = k.replace("\n", " ").replace("\r", " ").strip().split()[0] if k else k
            value = v
            if value and (key in costparameters):
                value *= inflation
            assumptions[tech][key] = value
    return assumptions


def map_ppm_jaif(unit_ppm):
    map_ppm = {  # print and copy all possible tuples in ppm (from debug script) and then make a manual map to jaif
        # (fuel,tech,set)
        ("Hard Coal", "Steam Turbine", "PP"): ("coal", "SCPC", "PP"),
        ("Nuclear", "Steam Turbine", "PP"): ("U-92", "nuclear-3", "PP"),
        # ('Hard Coal', 'Steam Turbine', 'CHP'):("","",""),
        # ('Hydro', 'Reservoir', 'Store'):("","",""),
        # ('Hydro', 'Run-Of-River', 'Store'):("","",""),
        # ('Hydro', 'Pumped Storage', 'Store'):("","",""),
        # ('Hydro', 'Run-Of-River', 'PP'):("","",""),
        # ('Hard Coal', 'CCGT', 'CHP'):("","",""),
        ("Hard Coal", "CCGT", "PP"): ("coal", "SCPC", "PP"),  # "CCGT"
        ("Lignite", "Steam Turbine", "PP"): ("coal", "SCPC", "PP"),
        # ('Natural Gas', 'CCGT', 'CHP'):("","",""),
        ("Natural Gas", "CCGT", "PP"): ("CH4", "CCGT", "PP"),
        ("Solid Biomass", "Steam Turbine", "PP"): ("bio", "bioST", "PP"),
        # ('Lignite', 'Steam Turbine', 'CHP'):("","",""),
        # ('Oil', 'Steam Turbine', 'CHP'):("","",""),
        # ('Hydro', 'Reservoir', 'PP'):("","",""),
        ("Oil", "Steam Turbine", "PP"): ("HC", "oil-eng", "PP"),
        # ('Oil', 'CCGT', 'CHP'):("","",""),
        # ('Lignite', 'CCGT', 'CHP'):("","",""),
        ("Natural Gas", "Steam Turbine", "PP"): ("CH4", "CCGT", "PP"),
        ("Hard Coal", None, "PP"): ("coal", "SCPC", "PP"),
        (None, "Steam Turbine", "PP"): ("CH4", "CCGT", "PP"),
        # ('Natural Gas', 'Steam Turbine', 'CHP'):("","",""),
        # (None, 'Steam Turbine', 'CHP'):("","",""),
        # ('Hydro', None, 'PP'):("","",""),
        # ('Solar', 'Pv', 'CHP'):("","",""),
        # ('Hydro', None, 'Store'):("","",""),
        # ('Wind', 'Onshore', 'PP'):("","",""),
        # (None, 'Marine', 'Store'):("","",""),
        # ('Wind', 'Offshore', 'PP'):("","",""),
        ("Lignite", None, "PP"): ("coal", "SCPC", "PP"),
        ("Geothermal", "Steam Turbine", "PP"): (None, "geothermal", "PP"),
        # ('Hydro', 'Pumped Storage', 'PP'):("","",""),
        # ('Wind', 'Onshore', 'Store'):("","",""),
        # ('Solar', 'Pv', 'PP'):("","",""),
        # ('Solid Biomass', 'CCGT', 'CHP'):("","",""),
        (None, "CCGT", "PP"): ("CH4", "CCGT", "PP"),
        # ('Solid Biomass', 'Steam Turbine', 'CHP'):("","",""),
        ("Oil", None, "PP"): ("HC", "oil-eng", "PP"),
        # ('Hard Coal', None, 'CHP'):("","",""),
        # ('Hydro', 'Run-Of-River', 'CHP'):("","",""),
        ("Waste", None, "PP"): ("waste", "wasteST", "PP"),
        ("Waste", "Steam Turbine", "PP"): ("waste", "wasteST", "PP"),
        ("Oil", "CCGT", "PP"): ("HC", "oil-eng", "PP"),
        ("Biogas", None, "PP"): ("bio", "bioST", "PP"),
        ("Biogas", "CCGT", "PP"): ("bio", "bioST", "PP"),
        (None, None, "PP"): ("CH4", "CCGT", "PP"),
        ("Natural Gas", None, "PP"): ("CH4", "CCGT", "PP"),
        # ('Natural Gas', None, 'CHP'):("","",""),
        ("Solid Biomass", None, "PP"): ("bio", "bioST", "PP"),
        # ('Waste', 'Steam Turbine', 'CHP'):("","",""),
        # ('Solar', 'Pv', 'Store'):("","",""),
        ("Waste", "CCGT", "PP"): ("waste", "wasteST", "PP"),
        # ('Wind', None, 'PP'):("","",""),
        ("Solid Biomass", "Pv", "PP"): ("bio", "bioST", "PP"),  # assumption
        ("Geothermal", None, "PP"): (None, "geothermal", "PP"),
        # ('Biogas', 'Steam Turbine', 'CHP'):("","",""),
        # (None, None, 'Store'):("","",""),
        # ('Natural Gas', 'Combustion Engine', 'CHP'):("","",""),
        ("Biogas", "Steam Turbine", "PP"): ("bio", "bioST", "PP"),
        # (None, None, 'CHP'):("","",""),
        # ('Oil', None, 'CHP'):",
        ("Natural Gas", "Combustion Engine", "PP"): ("CH4", "CCGT", "PP"),  # assumption
        ("Biogas", "Combustion Engine", "PP"): ("bio", "bioST", "PP"),  # assumption
        # ('Waste', None, 'CHP'):("","",""),
        # ('Solar', 'PV', 'PP'):("","",""),
        # ('Solar', 'CSP', 'PP'):("","",""),
        ("Other", None, "Store"): ("elec", "battery-storage", "Store"),  # assumption
        ("Other", "", "Store"): (
            "elec",
            "battery-storage",
            "Store",
        ),  # if technology is empty string
    }

    unknown = [" ", "", "unknown", "Unknown", "not found", "Not Found"]
    if unit_ppm["Technology"] in unknown:
        tech = None
    else:
        tech = unit_ppm["Technology"]
    if unit_ppm["Fueltype"] in unknown:
        fuel = None
    else:
        fuel = unit_ppm["Fueltype"]
    (fuel_ppm, tech_ppm, set_ppm) = map_ppm.get(
        (fuel, tech, unit_ppm["Set"]), ("unknown", "unknown", "unknown")
    )

    try:
        eta_ppm = float(unit_ppm["Efficiency"])
    except:
        eta_ppm = None
    try:
        cap_ppm = float(unit_ppm["Capacity"])
    except:
        cap_ppm = None
    try:
        datein_ppm = float("DateIn")
    except:
        datein_ppm = None
    try:
        dateout_ppm = float("DateOut")
    except:
        dateout_ppm = None

    unit_jaif = {
        "commodity": fuel_ppm,
        "technology": tech_ppm,
        "entityclass": set_ppm,
        "conversion_rate": eta_ppm,
        "capacity": cap_ppm,
        "date_in": datein_ppm,
        "date_out": dateout_ppm,
        "lat": unit_ppm["lat"],
        "lon": unit_ppm["lon"],
    }
    return unit_jaif


def map_tdr_jaif(line_tdr):
    # dictionary with name of technology in datafile to name in intermediate format

    # technology mapping
    map_tdr0 = {
        "battery storage": "battery-storage",
        "biogas": "bioST",
        # "biogas CC": "bioST+CC",
        # "biogas plus hydrogen":"bioST-H2"
        "CCGT": "CCGT",
        # "CCGT+CC",
        # "CCGT-H2",
        "fuel cell": "fuelcell",
        "geothermal": "geothermal",
        "pumped-Storage-Hydro-bicharger": "hydro-turbine",
        "nuclear": "nuclear-3",
        "OCGT": "OCGT",
        # "OCGT+CC",
        # "OCGT-H2",
        "oil": "oil-eng",
        "coal": "SCPC",
        # "SCPC+CC",
        "biomass": "wasteST",  # assumption
        # "biogas":"bio",
        # "gas":"CH4",
        # "CO2",
        # "coal":"coal",
        # "elec",
        # "hydrogen":"H2",
        # "oil":"HC",
        # "uranium":"U-92",
        # "waste",
        "direct air capture": "CC",
    }

    # parameter mapping
    map_tdr1 = {
        "FOM": "fixed_cost",
        "investment": "investment_cost",
        "lifetime": "lifetime",
        "VOM": "operational_cost",
        "efficiency": "conversion_rate",
        "C stored": "CO2_captured",
        "CO2 stored": "CO2_captured",
        # "capture rate":"CO2_capture_rate",
        # "capture_rate":"CO2_capture_rate",
        "capacity": "capacity",
        # "fuel":"operational_cost",
    }

    # value mapping
    try:
        map_tdr2 = float(line_tdr[2])
    except:
        map_tdr2 = None

    line_jaif = [
        map_tdr0.get(line_tdr[0], "unknown"),
        map_tdr1.get(line_tdr[1], "unknown"),
        map_tdr2,
    ]
    # print(f"replacing {line_tdr} for {line_jaif}")#debugline
    return line_jaif


def calculate_investment_and_fixed_costs(
    unit,
    assumptions,
    unit_types,
    years,
    invest_modifier=1000.0,
    fixed_modifier=1.0,
    invest_param="investment_cost",
    fixed_param="fixed_cost",
):
    """
    Calculate investment and fixed costs for any unit (PP or storage)
    Returns tuple: (investment_cost, fixed_cost)
    - Investment cost: converted from kWh to MWh
    - Fixed cost: converted from percentage to absolute currency units (EUR/MWh)
    - invest_param: parameter name for investment cost (e.g., 'investment_cost', 'investment_cost_energy', 'investment_cost_power')
    - fixed_param: parameter name for fixed cost (e.g., 'fixed_cost', 'fixed_cost_energy', 'fixed_cost_power')
    """
    invest_cost = search_data(
        unit,
        assumptions,
        unit_types,
        unit["technology"],
        years,
        invest_param,
        modifier=invest_modifier,
        prioritise_assumption=True,
    )
    fixed_cost_pct = search_data(
        unit,
        assumptions,
        unit_types,
        unit["technology"],
        years,
        fixed_param,
        modifier=fixed_modifier,
    )

    # Calculate fixed cost: percentage * investment cost / 100
    if isinstance(invest_cost, dict) and isinstance(fixed_cost_pct, dict):
        # Both are multi-year data (maps)
        fixed_cost_data = []
        for year_data in invest_cost["data"]:
            year = year_data[0]
            invest_val = year_data[1]
            # Find corresponding fixed cost percentage for this year
            fixed_pct_val = None
            for fc_data in fixed_cost_pct["data"]:
                if fc_data[0] == year:
                    fixed_pct_val = fc_data[1]
                    break
            if invest_val is not None and fixed_pct_val is not None:
                fixed_cost_data.append([year, invest_val * fixed_pct_val / 100])
            else:
                fixed_cost_data.append([year, None])

        fixed_cost = {
            "index_type": "str",
            "rank": 1,
            "index_name": "year",
            "type": "map",
            "data": fixed_cost_data,
        }
    elif isinstance(invest_cost, dict):
        # Investment cost is multi-year, fixed cost is single value
        if fixed_cost_pct is not None:
            fixed_cost_data = [
                [
                    year_data[0],
                    (
                        year_data[1] * fixed_cost_pct / 100
                        if year_data[1] is not None
                        else None
                    ),
                ]
                for year_data in invest_cost["data"]
            ]
            fixed_cost = {
                "index_type": "str",
                "rank": 1,
                "index_name": "year",
                "type": "map",
                "data": fixed_cost_data,
            }
        else:
            fixed_cost = None
    elif isinstance(fixed_cost_pct, dict):
        # Fixed cost is multi-year, investment cost is single value
        if invest_cost is not None:
            fixed_cost_data = [
                [
                    year_data[0],
                    (
                        invest_cost * year_data[1] / 100
                        if year_data[1] is not None
                        else None
                    ),
                ]
                for year_data in fixed_cost_pct["data"]
            ]
            fixed_cost = {
                "index_type": "str",
                "rank": 1,
                "index_name": "year",
                "type": "map",
                "data": fixed_cost_data,
            }
        else:
            fixed_cost = None
    else:
        # Both are single values
        if invest_cost is not None and fixed_cost_pct is not None:
            fixed_cost = invest_cost * fixed_cost_pct / 100
        else:
            fixed_cost = None

    return invest_cost, fixed_cost


def search_data(
    unit,
    assumptions,
    unit_types,
    unit_type_key,
    years,
    parameter,
    data=None,
    modifier=1.0,
    prioritise_assumption=False,
):
    # if parameter in ["fixed_cost"]:
    #     pprint(f"unit: {unit}, unit_types: {unit_types}, tech: {unit_type_key}, year:{years}, inflation: {modifier}") #debugline
    if not data:
        data = []
        for year in years:
            if unit_type_key in unit_types[year]:
                unit_type = unit_types[year][unit_type_key]
                # print(f"Unit type found for year {year}: {unit_type}") #debugline
            else:
                unit_type = {}
            datavalue = None
            if parameter in unit:
                if unit[parameter]:
                    datavalue = unit[parameter]
            if not datavalue and parameter in unit_type:
                if unit_type[parameter]:
                    datavalue = unit_type[parameter]
            if datavalue:
                datavalue *= modifier
            data.append([year, datavalue])

            # print(f"Year: {year}, Data Value: {datavalue}, Modifier: {modifier}") #debugline
    if len(data) == 0:
        parameter_value = None
        print(f"Cannot find parameter {parameter} for {unit["technology"]}")
    elif len(data) > 1:
        parameter_value = {
            "index_type": "str",
            "rank": 1,
            "index_name": "year",
            "type": "map",
            "data": data,
        }
    else:
        parameter_value = data[0][1]
    parameter_value = propose_assumption(
        unit["technology"],
        parameter,
        parameter_value,
        assumptions,
        years,
        prioritise_assumption=prioritise_assumption,
    )
    return parameter_value


def propose_assumption(
    unit_type,
    parameter,
    proposed_value,
    assumptions,
    years,
    prioritise_assumption=False,
):
    """
    Replace proposed value with assumption if possible.

    This function assumes that the proposed value is either None or already in the proper format. In other words, this function does not check whether proposed_value and years are compatible. The function does check whether there are None values in the proposed_value and replaces it fully if so.

    This function assumes that the data for all assumption years are the same. If in the future that format changes, this code need to change as well.

    For some parameters, the assumption values should always override existing values from jaif. To that end, use the prioritise assumption toggle.
    """
    # print(unit_type)  # debugline
    # print(parameter)  # debugline
    # print(proposed_value)  # debugline

    # check whether a multi year proposed value has a None value
    if len(years) > 1:
        replace = False
        for v in proposed_value[
            "data"
        ]:  # proposed_value["data"] = [['y2030', None], ['y2040', None], ['y2050', None]]
            if v[1] is None:
                replace = True
        if replace:
            proposed_value = None
    # proposed value should now either be a single None value, a regular single value or a map without None values

    returnvalue = proposed_value
    if prioritise_assumption or (returnvalue is None):
        if unit_type in assumptions:
            if parameter in assumptions[unit_type]:
                assumed_value = assumptions[unit_type][parameter]
                if assumed_value is not None:
                    if len(years) > 1:
                        data = [[year, assumed_value] for year in years]
                        returnvalue = {
                            "index_type": "str",
                            "rank": 1,
                            "index_name": "year",
                            "type": "map",
                            "data": data,
                        }
                    else:
                        returnvalue = assumed_value
    # print(returnvalue)  # debugline
    return returnvalue


def warn_for_none(jaif):
    """
    Validate the final parameter_values list for None values before saving to database.
    Issues warnings for any None values found.

    Args:
        jaif: The jaif dictionary containing parameter_values to validate
    """
    warnings_issued = []

    for param_value in jaif.get("parameter_values", []):
        if len(param_value) < 4:
            continue

        entity_type = param_value[0]
        entity_name = param_value[1]
        parameter = param_value[2]
        value = param_value[3]

        # Determine unit type from entity_name
        unit_type = ""
        if isinstance(entity_name, list) and len(entity_name) > 0:
            unit_type = str(entity_name[0])
        else:
            unit_type = str(entity_name)

        # Check for None value
        if value is None:
            warning_msg = f"None value for {entity_type} '{entity_name}', parameter '{parameter}' (unit type: {unit_type})"
            if warning_msg not in warnings_issued:
                warnings.warn(warning_msg)
                warnings_issued.append(warning_msg)
            continue

        # Check if value is a map/dict with None values in data
        if isinstance(value, dict) and value.get("type") == "map" and "data" in value:
            none_years = []
            for year_data in value["data"]:
                if len(year_data) >= 2 and year_data[1] is None:
                    none_years.append(year_data[0])

            if none_years:
                warning_msg = f"Warning: None values in map for {entity_type} '{entity_name}', parameter '{parameter}', years: {', '.join(map(str, none_years))} (unit type: {unit_type})"
                if warning_msg not in warnings_issued:
                    warnings.warn(warning_msg)
                    warnings_issued.append(warning_msg)
    return warnings_issued


if __name__ == "__main__":
    # flexibility in input
    # geo = sys.argv[1]
    # inf = sys.argv[2]
    # ppm = sys.argv[3] # pypsa power plant matching
    # tdr = {str(2020+(i-2)*10):sys.argv[i] for i in range(4,len(sys.argv)-1)} # pypsa technology data repository
    # spd = sys.argv[-1] # spine database preformatted with an intermediate format for the mopo project
    # flexibility in order (with limited flexibility of input)
    inputfiles = {
        "geo": "geo",  # "onshore.geojson",
        "inf": "inflation",  # "EU_historical_inflation_ECB.csv",
        # "tdr":{"y2020":"costs_2020","y2030":"costs_2030","y2040":"costs_2040","y2050":"costs_2050",},
        "bsy": {
            "y2020": "costs_2020",
        },
        "msy": {
            "y2030": "costs_2030",
            "y2040": "costs_2040",
            "y2050": "costs_2050",
        },
        "ppm": "powerplants",  # "powerplants.csv",
        "ass": "assumptions",  # "assumptions.xlsx",
        "cnf": "config",  # config.json
        "tmp": "template",  # power_template_DB.json
        "spd": "http",  # spine db
    }
    for key, value in inputfiles.items():
        if type(value) == dict:
            for k, v in value.items():
                if extractOne(v, sys.argv[1:]):
                    inputfiles[key][k] = extractOne(v, sys.argv[1:])[0]
                else:
                    inputfiles[key][k] = None
                print(f"Using {inputfiles[key][k]} as {v}")
        else:
            if extractOne(value, sys.argv[1:]):
                inputfiles[key] = extractOne(value, sys.argv[1:])[0]
            else:
                inputfiles[key] = None
            print(f"Using {inputfiles[key]} as {value}")

    importlog = main(**inputfiles)
    pprint(importlog)  # debug and information line
