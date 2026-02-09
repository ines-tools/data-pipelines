# Convert Power Plant Matching (ppm) and Technology Data Repository (tdr) to the Juha drive Alvaro's Intermediate data Format (jaif) for use in the data pipelines of the energy modelling workbench
# ppm: Capacity, Efficiency and lifetime (DateOut-DateIn or DateOut-2020)
# tdr: 2020-2050, investment, FOM, efficiency

"""
To Do:
- [x] merge files from Alvaro OR replace files of Alvaro with our files
- [X] see what data there is for CC and H2 in our csv files and add to the code accordingly
- [X] similar for storage parameters
- [X] check consistency of datavalues for all data (e.g. kW vs MW)
- [X] if there is data/technologies missing, put them in a list for Fortum
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
- [X] add assumption parameters from an excel file to account for missing data from tdr/ppm
- [X] add final validation check to warn if None values are being added to jaif
- [ ] check whether the script is compatible with the geojson file of the industrial study
- [x] reject existing power plants if they are not within areas specified by the geojson file
- [ ] remove y2025 from parameter maps (part is assumptions file, part is reference year)
- [x] check purging (and that there is no "unit" in the maps)
- [x] the assumptions have constant values over the years, instead have an assumption of the growth/decline over the milestoneyears? No the data is in 2025 values, the inflation will automatically change the values over the years.
- [ ] difference between existing tech and new tech
    - existing tech has all float values (including the costs) and has 2020 costs in 2025 values
    - new tech has all map values and has 2030, 2040 and 2050 costs in 2025 values
- [ ] storage
    - [ ] storage is the energy, storage-connection is power, both need data on
        - [ ] lifetime (currently only storage has one, not the connection, we should set the default the same?)
        - [ ] cost (apparently we only provide costs for the connection, i.e. power, at the moment)
    - [ ] if there is no cost data for either energy or power, the energy_power_ratio needs to be specified ("ideally, investment and fixed costs in the storage and storage connection. If no investment costs for the storage connection then energy_power_ratio and operational costs only for the storage-connection")
    - [ ] "In the DEA catalogue, you can find different cost for energy and power regarding investment cost (both storage and storage-connection). I think fom cost only for energy (storage) and operational cost only for power (storage-connection)." (Alvaro has another example in his mail)

Optional:
- [ ] Currently, for some parameters that only require 1 value in jaif, new units use the first milestoneyear for its value  while in some instances it probably should use the average over the years.
- [ ] aggregate all units by type and use them as another data (arche)type (probably requires moving loading of files from existin/new units to main function)
- [ ] when loading from the assumptions file, check the milestoneyears (and/or reference year) instead of loading all the years that are in the assumptions columns
- [ ] When the milestone years are different from the years in the files (whether it is costs.csv or assumptions.xlsx), perform an inflation calculation (similar to reference year but with an assumption on the discount rate)
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
from spinedb_api import purge
import warnings


def main(
    geo,
    inf,
    rfy,
    msy,
    ppm,
    ass,
    cnf,
    tmp,
    spd,
    # geolevel="PECD1",  # "PECD1",# "PECD2",# "NUTS2",# "NUTS3",#
    # referenceyear="y2025",
    # units_existing=["bioST","CCGT","nuclear-3","oil-eng","SCPC","wasteST","geothermal","battery-storage"],
    # units_new=["bioST","bioST+CC","CCGT","CCGT+CC","CCGT-H2","fuelcell","geothermal","nuclear-3","OCGT","OCGT+CC","OCGT-H2","oil-eng","SCPC","SCPC+CC","wasteST","battery-storage"],# "hydro-turbine"
    # units_CC=[],
    # units_H2=[],
    # commodities=[],
    # parameters_existing=["conversion_rate","operational_cost","capacity"],
    # parameters_new=["lifetime","investment_cost","fixed_cost","operational_cost","CO2_captured","conversion_rate"],
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
        referenceyear = config["referenceyear"]
        units_existing = config["units_existing"]
        units_new = config["units_new"]
        commodities = config["commodities"]

    # load data
    geomap = gpd.read_file(geo)
    geomap = geomap[geomap["level"] == geolevel]  # used by units existing
    regions = geomap["id"].to_list()  # used by units new
    # print("Regions")
    # pprint(regions)#debugline

    # format data
    existing_units(
        jaif,
        inf,
        rfy,
        ppm,
        ass,
        geomap,
        referenceyear,
        list(msy.keys()),
        units_existing,
        commodities,
    )
    new_units(jaif, msy, regions, units_new, commodities)
    apply_assumption_parameters(jaif, ass, regions)

    # validate final parameter values before saving
    validate_final_parameter_values(jaif)

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


#######################################################################################################################################################
#######################################################################################################################################################


def existing_units(
    jaif,
    inf,
    rfy,
    ppm,
    ass,
    geomap,
    referenceyear,
    milestoneyears,
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
    # load data
    yearly_inflation = {}
    with open(inf, "r", encoding="utf-8") as file:
        csvreader = csv.reader(file)
        next(csvreader)  # skip header
        for line in csvreader:
            yearly_inflation[int(line[1])] = float(line[2]) / 100
    # print(yearly_inflation)#debugline

    # load reference year data and convert to dictionary
    unit_types = {}
    # could be done differently as unit_types[line[0]][line[1]][year][line[2]]
    for year, path in rfy.items():  # only one entry
        datayear = year
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
        unit_types,
        units_existing,
        referenceyear,
        datayear,
        milestoneyears,
        geomap,
    )
    # pprint(unit_instances)#debugline
    # pprint(unit_types)

    regionlist = []
    commoditylist = []
    technologylist = []
    # unit_type_key_list = [] # for debugging
    years = [referenceyear].extend(milestoneyears)

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
                            [
                                "commodity__to_technology__to_commodity",
                                [
                                    unit["commodity"],
                                    unit["technology"] + "-existing",
                                    "CO2",
                                ],
                                None,
                            ],
                        ]
                    )

                inflation = inflationfactor(yearly_inflation, datayear, referenceyear)
                _, fixed_cost_abs = calculate_investment_and_fixed_costs(
                    unit,
                    unit_types,
                    [datayear],
                    search_fn=search_data_existing,
                    invest_modifier=1000.0 * inflation,
                    fixed_modifier=1.0,
                )

                operational_cost_val = search_data(
                    unit,
                    unit_types,
                    unit["technology"],
                    [datayear],
                    "operational_cost",
                    modifier=inflationfactor(yearly_inflation, datayear, referenceyear),
                )

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
                        unit_types,
                        unit["technology"],
                        [datayear],
                        "conversion_rate",
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
                        unit, unit_types, unit["technology"], [datayear], "CO2_captured"
                    )
                    if co2_captured is not None:
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
                unit_types,
                unit["technology"],
                years,
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
                    unit_types,
                    unit["technology"],
                    [datayear],
                    "efficiency",
                    modifier=1 / sqrt(2),
                )
                efficiency_out_val = search_data(
                    unit,
                    unit_types,
                    unit["technology"],
                    [datayear],
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
                """
                jaif["parameter_values"].extend([
                    [
                        "storage_connection",
                        [
                            unit["technology"]+"-existing",
                            "elec"
                        ],
                        "investment_cost",
                        search_data(unit, unit_types, unit["Technolgy"], years, "investment_cost"),
                        "Base"
                    ],
                    [
                        "storage_connection",
                        [
                            unit["technology"]+"-existing",
                            "elec"
                        ],
                        "fixed_cost",
                        search_data(unit, unit_types, unit["Technolgy"], years, "fixed_cost"),
                        "Base"
                    ],
                ])
                """
            jaif["entities"].extend(
                [
                    [
                        "storage__region",
                        [unit["technology"] + "-existing", unit["region"]],
                        None,
                    ],
                ]
            )
            storages_existing_val = search_data(
                unit,
                unit_types,
                unit["technology"],
                [referenceyear],
                "capacity",
                data=[[k, v] for k, v in unit["capacity"].items()],
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


def new_units(jaif, msy, regions, units_new, commodities):
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
                            [
                                "commodity__to_technology__to_commodity",
                                [unit["commodity"], unit["technology"], "CO2"],
                                None,
                            ],
                        ]
                    )

                # Calculate investment and fixed costs
                invest_cost, fixed_cost = calculate_investment_and_fixed_costs(
                    unit, unit_types, years
                )

                lifetime_val = search_data(
                    unit,
                    unit_types,
                    unit["technology"],
                    [years[0]],
                    "lifetime",
                )
                operational_cost_new = search_data(
                    unit,
                    unit_types,
                    unit["technology"],
                    years,
                    "operational_cost",
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
                        unit_types,
                        unit["technology"],
                        [years[0]],
                        "conversion_rate",
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
                        unit, unit_types, unit["technology"], [years[0]], "CO2_captured"
                    )
                    if co2_captured is not None:
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
                    unit_types,
                    unit["technology"],
                    [years[0]],
                    "efficiency",
                    modifier=1 / sqrt(2),
                )
                storage_efficiency_out = search_data(
                    unit,
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

                # Calculate storage investment and fixed costs
                storage_invest_cost, storage_fixed_cost = (
                    calculate_investment_and_fixed_costs(unit, unit_types, years)
                )

                storage_lifetime = search_data(
                    unit, unit_types, unit["technology"], years, "lifetime"
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
                            "storage_connection",
                            [unit["technology"], "elec"],
                            "investment_cost",
                            storage_invest_cost,
                            "Base",
                        ],
                        [
                            "storage_connection",
                            [unit["technology"], "elec"],
                            "fixed_cost",
                            storage_fixed_cost,
                            "Base",
                        ],
                    ]
                )

            jaif["entities"].extend(
                [
                    ["storage__region", [unit["technology"], unit["region"]], None],
                ]
            )

    return jaif


def apply_assumption_parameters(jaif, assumptions_path, regions=None):
    """
    Load technology parameters from an assumptions Excel file and merge them into jaif.

    Reads from two sheets:
    - 'Generation': For power plant parameters
    - 'Storage': For storage parameters

    Expected columns (case-insensitive, spaces/underscores ignored):
    technology/tech, efficiency_in, efficiency_out, storage_capacity, capacity,
    operational_cost[_YEAR], investment_cost[_YEAR], fixed_cost[_YEAR], lifetime.
    Year-suffixed cost columns are combined into jaif maps keyed by year (e.g. y2025).
    Only non-empty values are applied. Technologies or storages not present in jaif
    are created if they don't exist (especially useful for new storage types).

    Args:
        jaif: The jaif dictionary to update
        assumptions_path: Path to the Excel assumptions file
        regions: List of region IDs for creating storage__region entities for new storages
    """

    if not assumptions_path:
        print("No assumptions file provided; skipping assumptions merge.")
        return jaif

    try:
        xls = pd.ExcelFile(assumptions_path)
    except FileNotFoundError:
        print(f"Assumptions file not found: {assumptions_path}")
        return jaif

    # Helper lookups for existing jaif entities
    technologies = {
        entity[1] for entity in jaif["entities"] if entity[0] == "technology"
    }
    storages = {entity[1] for entity in jaif["entities"] if entity[0] == "storage"}

    def resolve_entity_name(name, existing_names):
        if name in existing_names:
            return name
        suffix_name = f"{name}-existing"
        if suffix_name in existing_names:
            return suffix_name
        return None

    def find_connection_commodity(entity_type, tech_name):
        for entity in jaif["entities"]:
            if entity[0] == entity_type and isinstance(entity[1], list):
                if entity[1][0] == tech_name:
                    return entity[1][1]
        return "elec"

    def find_input_commodity(tech_name):
        """Find the input commodity for a technology from commodity__to_technology relationship."""
        for entity in jaif["entities"]:
            if entity[0] == "commodity__to_technology" and isinstance(entity[1], list):
                if entity[1][1] == tech_name:  # tech_name is second element
                    return entity[1][0]  # return commodity (first element)
        return None  # No input commodity found

    def upsert_parameter(entity_type, entity_name, parameter, value):
        # Skip empties but allow dict/map values
        if value is None:
            return
        if not isinstance(value, dict) and pd.isna(value):
            return
        entry = [entity_type, entity_name, parameter, value, "Base"]
        for idx, existing in enumerate(jaif["parameter_values"]):
            if (
                existing[0] == entity_type
                and existing[1] == entity_name
                and existing[2] == parameter
            ):
                jaif["parameter_values"][idx] = entry
                return
        jaif["parameter_values"].append(entry)

    def build_year_map(row, base_name):
        """Return map (year->value) if year-suffixed cols exist, else scalar value."""
        entries = []
        for col, val in row.items():
            if not isinstance(col, str):
                continue
            if not col.startswith(base_name + "_"):
                continue
            suffix = col[len(base_name) + 1 :]
            if suffix == "":
                continue
            if pd.isna(val):
                continue
            year_label = str(suffix)
            if not year_label.startswith("y") and year_label.isdigit():
                year_label = f"y{year_label}"
            entries.append([year_label, val])

        if entries:
            entries.sort(
                key=lambda pair: (
                    (0, int(str(pair[0]).lstrip("y")))
                    if str(pair[0]).lstrip("y").isdigit()
                    else (1, str(pair[0]))
                )
            )
            return {
                "index_type": "str",
                "rank": 1,
                "index_name": "year",
                "type": "map",
                "data": entries,
            }

        fallback = row.get(base_name)
        if pd.isna(fallback):
            return None
        return fallback

    # Process Generation sheet for technologies
    if "Generation" in xls.sheet_names:
        df_gen = pd.read_excel(assumptions_path, sheet_name="Generation")
        # Normalise column names
        df_gen.columns = [
            str(col).strip().lower().replace(" ", "_") for col in df_gen.columns
        ]
        if "technology" not in df_gen.columns and "tech" in df_gen.columns:
            df_gen = df_gen.rename(columns={"tech": "technology"})
        if "technology" not in df_gen.columns:
            print(
                "Generation sheet is missing a 'technology' or 'tech' column; skipping."
            )
        else:
            for _, row in df_gen.iterrows():
                tech_raw = row.get("technology")
                if pd.isna(tech_raw):
                    continue
                tech = str(tech_raw).strip()
                if not tech:
                    continue

                tech_name = resolve_entity_name(tech, technologies)

                # Apply to technology-based units (power plants)
                if tech_name:
                    commodity = find_connection_commodity(
                        "technology__to_commodity", tech_name
                    )
                    connection = [tech_name, commodity]

                    # For commodity__to_technology__to_commodity, need input commodity too
                    input_commodity = find_input_commodity(tech_name)
                    if input_commodity:
                        conversion_connection = [input_commodity, tech_name, commodity]
                    else:
                        conversion_connection = None

                    invest_val = build_year_map(row, "investment_cost")
                    op_cost_val = build_year_map(row, "operational_cost")
                    fixed_cost_val = build_year_map(row, "fixed_cost")
                    upsert_parameter(
                        "technology", tech_name, "lifetime", row.get("lifetime")
                    )
                    upsert_parameter(
                        "technology__to_commodity",
                        connection,
                        "capacity",
                        row.get("capacity"),
                    )
                    upsert_parameter(
                        "technology__to_commodity",
                        connection,
                        "operational_cost",
                        op_cost_val,
                    )
                    upsert_parameter(
                        "technology__to_commodity",
                        connection,
                        "investment_cost",
                        invest_val,
                    )
                    upsert_parameter(
                        "technology__to_commodity",
                        connection,
                        "fixed_cost",
                        fixed_cost_val,
                    )
                    if conversion_connection:
                        upsert_parameter(
                            "commodity__to_technology__to_commodity",
                            conversion_connection,
                            "conversion_rate",
                            row.get("conversion_rate"),
                        )

                if not tech_name:
                    print(f"Assumptions tech '{tech}' not found in jaif; skipping.")

    # Process Storage sheet for storage parameters
    if "Storage" in xls.sheet_names:
        df_stor = pd.read_excel(assumptions_path, sheet_name="Storage")
        # Normalise column names
        df_stor.columns = [
            str(col).strip().lower().replace(" ", "_") for col in df_stor.columns
        ]
        if "technology" not in df_stor.columns and "tech" in df_stor.columns:
            df_stor = df_stor.rename(columns={"tech": "technology"})
        if "technology" not in df_stor.columns:
            print("Storage sheet is missing a 'technology' or 'tech' column; skipping.")
        else:
            for _, row in df_stor.iterrows():
                storage_raw = row.get("technology")
                if pd.isna(storage_raw):
                    continue
                storage = str(storage_raw).strip()
                if not storage:
                    continue

                storage_name = resolve_entity_name(storage, storages)

                # If storage doesn't exist, create it
                if not storage_name:
                    storage_name = storage
                    commodity = "elec"  # Default commodity for storage

                    # Create storage entity
                    jaif["entities"].extend(
                        [
                            ["storage", storage_name, None],
                            ["storage_connection", [storage_name, commodity], None],
                        ]
                    )

                    # Create storage__region entities for all regions if regions provided
                    if regions:
                        for region in regions:
                            jaif["entities"].append(
                                [
                                    "storage__region",
                                    [storage_name, region],
                                    None,
                                ]
                            )

                    # Update storages set for future lookups
                    storages.add(storage_name)
                    print(
                        f"Created new storage entity '{storage_name}' from assumptions file."
                    )

                # Apply to storage units
                if storage_name:
                    invest_val = build_year_map(row, "investment_cost")
                    op_cost_val = build_year_map(row, "operational_cost")
                    fixed_cost_val = build_year_map(row, "fixed_cost")
                    commodity = find_connection_commodity(
                        "storage_connection", storage_name
                    )
                    connection = [storage_name, commodity]
                    upsert_parameter(
                        "storage", storage_name, "lifetime", row.get("lifetime")
                    )
                    upsert_parameter(
                        "storage_connection",
                        connection,
                        "efficiency_in",
                        row.get("efficiency_in"),
                    )
                    upsert_parameter(
                        "storage_connection",
                        connection,
                        "efficiency_out",
                        row.get("efficiency_out"),
                    )
                    upsert_parameter(
                        "storage_connection",
                        connection,
                        "investment_cost",
                        invest_val,
                    )
                    upsert_parameter(
                        "storage_connection",
                        connection,
                        "operational_cost",
                        op_cost_val,
                    )
                    upsert_parameter(
                        "storage_connection",
                        connection,
                        "fixed_cost",
                        fixed_cost_val,
                    )
                    upsert_parameter(
                        "storage_connection",
                        connection,
                        "capacity",
                        row.get("capacity"),
                    )
                    upsert_parameter(
                        "storage",
                        storage_name,
                        "storage_capacity",
                        row.get("storage_capacity"),
                    )

    return jaif


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
        "OCGT+CC": {
            "commodity": "CH4",
            "technology": "OCGT+CC",
            "entityclass": "PP",
        },
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
    unit_types,
    units,
    referenceyear,
    datayear,
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
        original_unit = unit.copy()  # Keep original for debugging
        # print(unit["Country"])  # debugline
        unit = map_ppm_jaif(unit)
        unit["region"] = get_region(unit, geomap)
        # print(unit["region"])  # debugline

        # Debug: Check if mapping worked for storage - ONLY for 'Other' fuel type
        # if original_unit.get('Set') == 'Store' and original_unit.get('Fueltype') == 'Other':
        #     fuel = original_unit.get('Fueltype')
        #     tech = original_unit.get('Technology')
        #     set_val = original_unit.get('Set')
        #     print(f"OTHER STORAGE MAPPING: Key=({fuel}, {tech}, {set_val})")
        #     print(f"  -> Mapped=({unit.get('commodity')}, {unit.get('technology')}, {unit.get('entityclass')})")
        #     if unit.get('commodity') == 'unknown':
        #         print(f"  -> MAPPING FAILED! No mapping found for key ({fuel}, {tech}, {set_val})")

        # if unit["entityclass"] == "Store":
        #     print(f"STORAGE UNIT FOUND: {unit['commodity']}, {unit['technology']}, {unit['entityclass']}, Region: {unit.get('region', 'NOT_SET')}") #debugline

        if (unit["technology"] in units) and unit["region"]:
            # tuple for aggregating
            unit_tuple = tuple(
                [unit[key] for key in ["commodity", "technology", "region"]]
            )
            if unit_tuple not in aggregated_units.keys():
                # print(unit_tuple)# debug and information line
                # initialise
                aggregated_units[unit_tuple] = deepcopy(unit)
                aggregated_unit = aggregated_units[unit_tuple]
                for parameter in average_parameters:
                    unit[parameter] = search_data(
                        unit, unit_types, unit["technology"], [datayear], parameter
                    )
                    if unit[parameter]:
                        aggregated_unit[parameter] = float(unit[parameter])
                    else:
                        aggregated_unit[parameter] = None
                for parameter in sum_parameters:
                    unit[parameter] = search_data(
                        unit, unit_types, unit["technology"], [datayear], parameter
                    )
                    if unit[parameter]:
                        aggregated_unit[parameter] = float(unit[parameter])
                    else:
                        aggregated_unit[parameter] = None
                for parameter in cumulative_parameters:
                    if parameter == "capacity":
                        lifetime = search_data(
                            unit, unit_types, unit["technology"], [datayear], "lifetime"
                        )
                        unit["capacity"] = decay_capacity(
                            unit, lifetime, referenceyear, milestoneyears
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
                        unit, unit_types, unit["technology"], [datayear], parameter
                    )
                    if aggregated_unit[parameter] and unit[parameter]:
                        aggregated_unit[parameter] = (
                            float(aggregated_unit[parameter]) + float(unit[parameter])
                        ) / 2
                    elif unit[parameter]:
                        aggregated_unit[parameter] = float(unit[parameter])
                for parameter in sum_parameters:
                    unit[parameter] = search_data(
                        unit, unit_types, unit["technology"], [datayear], parameter
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
                            unit, unit_types, unit["technology"], [datayear], "lifetime"
                        )
                        unit["capacity"] = decay_capacity(
                            unit, lifetime, referenceyear, milestoneyears
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
    """
    lat = float(unit["lat"])
    lon = float(unit["lon"])
    point = Point(lon, lat)
    pip = geomap.contains(point)
    region = None
    for index, value in enumerate(pip):
        if value:
            if region:
                print("Warning: Overlapping polygons, using region " + region)
            else:
                region = geomap.iloc[index]["id"]
    return region


def decay_capacity(unit, lifetime, referenceyear, milestoneyears):
    capacity = {referenceyear: float(unit["capacity"])}
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
    unit, unit_types, years, search_fn=None, invest_modifier=1000.0, fixed_modifier=1.0
):
    """
    Calculate investment and fixed costs for any unit (PP or storage)
    Returns tuple: (investment_cost, fixed_cost)
    - Investment cost: converted from kWh to MWh
    - Fixed cost: converted from percentage to absolute currency units (EUR/MWh)
    """
    if search_fn is None:
        search_fn = search_data

    invest_cost = search_fn(
        unit,
        unit_types,
        unit["technology"],
        years,
        "investment_cost",
        modifier=invest_modifier,
    )
    fixed_cost_pct = search_fn(
        unit,
        unit_types,
        unit["technology"],
        years,
        "fixed_cost",
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
    unit, unit_types, unit_type_key, years, parameter, data=None, modifier=1.0
):
    if not data:
        data = []
        for year in years:
            if unit_type_key in unit_types[year]:
                unit_type = unit_types[year][unit_type_key]
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
    return parameter_value


def search_data_existing(
    unit, unit_types, unit_type_key, years, parameter, data=None, modifier=1.0
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
    return parameter_value


def inflationfactor(yearly_inflation, year, referenceyear):
    if "y" in year:
        year = int(year[1:])
    if "y" in referenceyear:
        referenceyear = int(referenceyear[1:])
    inflation = 1.0
    for y in range(year, referenceyear):
        inflation *= 1 - yearly_inflation[y]
    return inflation


def validate_final_parameter_values(jaif):
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
            warning_msg = f"Warning: None value for {entity_type} '{entity_name}', parameter '{parameter}' (unit type: {unit_type})"
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
        "rfy": {
            "y2030": "costs_2030",  # "y2020": "costs_2020",
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
