import spinedb_api as api
from spinedb_api import DatabaseMapping
from spinedb_api.dataframes import to_dataframe
from spinedb_api.parameter_value import convert_map_to_table, IndexedValue
from sqlalchemy.exc import DBAPIError
import datetime
import pandas as pd
import sys
import numpy as np
import json
import yaml 
import time as time_lib
import os

if len(sys.argv) > 1:
    url_result = sys.argv[1]
else:
    exit("Please provide spineopt database url as argument. They should be of the form ""sqlite:///path/db_file.sqlite""")

def nested_index_names(value, names = None, depth = 0):
    if names is None:
        names = []
    if depth == len(names):
        names.append(value.index_name)
    elif value.index_name != names[-1]:
        raise RuntimeError(f"Index names at depth {depth} do no match: {value.index_name} vs. {names[-1]}")
    for y in value.values:
        if isinstance(y, IndexedValue):
            nested_index_names(y, names, depth + 1)
    return names

def get_latest_alternatives(config):

    predefined_scenarios = [scenario_i for scenario_i in config["scenarios"]]
    with DatabaseMapping(url_result) as result_db:
        alternatives = [i["name"] for i in result_db.get_alternative_items()]
        latest_alternatives = {}
        for alternative in alternatives:
            if "@" in alternative:
                name, timestamp = alternative.split('@')
                timestamp = pd.Timestamp(timestamp)
            
                if (name not in latest_alternatives or timestamp > latest_alternatives[name]) and any(i in name for i in predefined_scenarios):
                    latest_alternatives[name] = timestamp

    return latest_alternatives

def find_the_extreme_period(latest_alternatives):

    with DatabaseMapping(url_result) as result_db:

        dict_extremes = {}
        for parameter_name in ["objective_objective_penalties"]:
            for parameter_map in result_db.get_parameter_value_items(parameter_definition_name = parameter_name):
                entity_name = parameter_map["entity_byname"][1]
                scenario_name, timestamp = parameter_map["alternative_name"].split("@")
                timestamp = pd.Timestamp(timestamp)
                if scenario_name in latest_alternatives:
                    if timestamp == latest_alternatives[scenario_name]:
                        map_table = convert_map_to_table(parameter_map["parsed_value"])
                        index_names = nested_index_names(parameter_map["parsed_value"])
                        data = pd.DataFrame(map_table, columns=index_names + [entity_name]).set_index(index_names[0])[entity_name]

                        shed = data[data > 0.0]
                        for index_ in shed.index:
                            day =  pd.Timestamp(index_).dayofyear
                            if day not in dict_extremes:
                                dict_extremes[day] = ["winter" if pd.Timestamp(index_).month in [10,11,12,1,2,3] else "summer", pd.Timestamp(index_).year, shed.at[index_]]
                            else:
                                if shed.at[index_] > dict_extremes[day][2]:
                                    dict_extremes[day] = ["winter" if pd.Timestamp(index_).month in [10,11,12,1,2,3] else "summer", pd.Timestamp(index_).year, shed.at[index_]]
    extremes_df = pd.DataFrame.from_dict(dict_extremes,orient="index",columns=["season","year","impact"]).reset_index().rename(columns={"index":"day"})
    return extremes_df

def build_initial_representatives(extremes_df):

    profiles = {}
    profiles_folder = "profiles/"
    results_folder = "results/"
    representative_periods_df = pd.read_csv(os.path.join(results_folder,"representative_periods.csv"))
    new_representative_periods_df = pd.DataFrame(columns=representative_periods_df.columns)
    periods_map = dict(zip(range(1,8761),np.repeat(range(1,366),24)))
    for profiles_path in os.listdir(profiles_folder):
        wyear = profiles_path.split("_")[1].split(".")[0]
        profiles[wyear] = pd.read_csv(os.path.join(profiles_folder,profiles_path))

        profiles[wyear]["period"] = profiles[wyear]["timestep"]
        profiles[wyear]["period"] = profiles[wyear]["period"].map(periods_map)
        profiles[wyear]["timestep"] = profiles[wyear]["timestep"].apply(lambda x: ((x - 1) % 24)+1)
        
        if wyear in representative_periods_df.columns:
            representative_periods = representative_periods_df[wyear].tolist()

            for event_type in ["summer","winter"]:
                original_length = len(representative_periods)
                for event_ in extremes_df[extremes_df["season"] == event_type].index:
                    if extremes_df.at[event_,"day"] not in representative_periods:
                        representative_periods.append(extremes_df.at[event_,"day"])
                        print(f"{event_type} event added {extremes_df.at[event_,"day"]} year {extremes_df.at[event_,"year"]} with impact: {extremes_df.at[event_,"impact"]}")
                        break
                if original_length == len(representative_periods):
                    print(f"WARNING: No {event_type} event added")

            extreme_periods = profiles[wyear][profiles[wyear]["period"].isin(representative_periods)].sort_values(by=["period","timestep","profile_name"])[["period","timestep","profile_name","value"]]
            map_rp = {extreme_periods["period"].unique().tolist()[i-1]:i for i in range(1,len(extreme_periods["period"].unique())+1)}
            new_representative_periods_df[wyear] = sorted(extreme_periods["period"].unique().tolist())
            extreme_periods["period"] = extreme_periods["period"].map(map_rp)
            
            extreme_periods.index = range(1,extreme_periods.shape[0]+1)
            extreme_periods.to_csv(os.path.join(results_folder,f"initial_representative_periods_{wyear}.csv"),index=False)

    print(new_representative_periods_df)
    new_representative_periods_df.to_csv(os.path.join(results_folder,"representative_periods.csv"),index=False)    

def main():
    
    with open(sys.argv[2], 'r') as file:
        config = yaml.safe_load(file)

    latest_alternatives = get_latest_alternatives(config)
    extremes_df = find_the_extreme_period(latest_alternatives)
    build_initial_representatives(extremes_df)

if __name__ == "__main__":
    main()