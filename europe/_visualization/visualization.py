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
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.colors as pc
import copy
import re 
from math import cos, sin, radians, sqrt, pi
import matplotlib.pyplot as plt
from matplotlib.patches import Wedge
import geopandas as gpd
import sys
from streamlit.web import cli as stcli
import dill

url_results = sys.argv[1]
result_db = DatabaseMapping(url_results)

if len(sys.argv) > 2:
    sopt_results = sys.argv[2]
    sopt_db = DatabaseMapping(sopt_results)
else:
    print("There is no associated SpineOpt DB, you will get an error if you call it")

with open("node_mapping.yml","r") as file:
    node_map = yaml.safe_load(file)
with open("node_mapping_sankey.yml","r") as file:
    node_map_sankey = yaml.safe_load(file)
with open("unit_mapping.yml","r") as file:
    unit_map = yaml.safe_load(file)
with open("scenario_mapping.yml","r") as file:
    scenario_map = yaml.safe_load(file)
        

def extract_polygon(unit_name: str):
    if not isinstance(unit_name, str):
        return None
    part = unit_name.rsplit('_', 1)[-1][-2:]
    return part or None

def apply_unit_name(unit_name: str):
    new_name = None
    if not isinstance(unit_name, str):
        return None
    for key_i in unit_map:
        if key_i in unit_name:
            new_name = unit_map[key_i]
    return new_name or unit_name.rsplit('_', 1)[0]

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

def from_DB_to_df(map_years):

    alternatives = [i["name"] for i in result_db.get_alternative_items()]
    latest_alternatives = {}
    for alternative in alternatives:
        if "@" in alternative:
            name, timestamp = alternative.split('@')
            timestamp = pd.Timestamp(timestamp)
        
            if name not in latest_alternatives or timestamp > latest_alternatives[name]:
                latest_alternatives[name] = timestamp

    years = list(map_years.values())
    start_date = {2030:"2030-01-01 00:00:00",2041:"2040-12-31 23:00:00",2050:"2049-12-31 23:00:00"}
    years_index = [pd.Timestamp(i) for year in map_years for i in pd.date_range(start=start_date[year],end=str(year)+"-12-31 23:00:00",freq="1h")]

    rps = get_representative_periods()
    analyzed_nodes = ["elec","HC","H2","CH4","heat","cool","MeOH"]#,"steel-primary","steel-secondary","MeOH","glass-float","glass-container","glass-fibre","fertiliser-ammonia-NH3","chemical-PE","chemical-PEA","chemical-olefins","cement"]
    unit_to_node_map = {}
    energy_map    = {name:pd.DataFrame(columns=["source","target"]+years) for name in latest_alternatives}
    flows_map     = {name:pd.DataFrame(columns=["source","target","commodity"]+years) for name in latest_alternatives}
    flows_sto     = {name:pd.DataFrame(columns=["source","target"]+years) for name in latest_alternatives}
    emission_map  = {name:pd.DataFrame(columns=["source","target"]+years) for name in latest_alternatives}
    unit_to_flows = {name:pd.DataFrame(columns=["unit_name","node"]+years) for name in latest_alternatives}
    units_cap     = {name:pd.DataFrame(columns=["unit_name"]+years) for name in latest_alternatives}
    units_inv     = {name:pd.DataFrame(columns=["unit_name"]+years) for name in latest_alternatives}
    units_dec     = {name:pd.DataFrame(columns=["unit_name"]+years) for name in latest_alternatives}
    node_state    = {name:pd.DataFrame() for name in latest_alternatives}

    for param_map in result_db.get_parameter_value_items(parameter_definition_name = "unit_flow"):
        scenario_name, timestamp = param_map["alternative_name"].split("@")
        timestamp = pd.Timestamp(timestamp)
        if scenario_name in latest_alternatives:
            if timestamp == latest_alternatives[scenario_name]:
                alte_name = scenario_name
                if "to_node" in param_map["entity_byname"]:

                    unit_name = param_map["entity_byname"][1]
                    node_name = param_map["entity_byname"][2]

                    map_table = convert_map_to_table(param_map["parsed_value"])
                    index_names = nested_index_names(param_map["parsed_value"])
                    data = pd.DataFrame(map_table, columns=index_names + [unit_name]).set_index(index_names[0])[unit_name]
                    data.index = [pd.Timestamp(i) for i in data.index.astype("string")]

                    if rps:
                        data = data.loc[rps[scenario_name.split("__")[0]].index]*rps[scenario_name.split("__")[0]].loc[:,"weight"]

                    unit_to_flows[alte_name].loc[unit_to_flows[alte_name].shape[0],:] = [unit_name,node_name.split("_")[0]] + [data[data.index.year==year_i].sum() for year_i in map_years]

                    if node_name != "atmosphere":
                        if not (any(i in unit_name for i in ["+CC","MEA","DEA","-CC","Oxy","CaL"]) and node_name.split("_")[0] == "CO2"):
                            unit_to_node_map[unit_name] = node_name.split("_")[0] 
                        if "wind" in unit_name:
                            energy_map[alte_name].loc[energy_map[alte_name].shape[0],:] = ["wind",unit_name] + [data[data.index.year==year_i].sum() for year_i in map_years]
                        elif "solar" in unit_name:
                            energy_map[alte_name].loc[energy_map[alte_name].shape[0],:] = ["solar",unit_name] + [data[data.index.year==year_i].sum() for year_i in map_years]
                        elif "RoR" in unit_name:
                            energy_map[alte_name].loc[energy_map[alte_name].shape[0],:] = ["hydro",unit_name] + [data[data.index.year==year_i].sum() for year_i in map_years]
                    else:
                        emission_map[alte_name].loc[energy_map[alte_name].shape[0],:] = [unit_name,"atmosphere"] + [data[data.index.year==year_i].sum() for year_i in map_years]

                elif "from_node" in param_map["entity_byname"]:
                    unit_name = param_map["entity_byname"][1]
                    node_name = param_map["entity_byname"][2]

                    map_table = convert_map_to_table(param_map["parsed_value"])
                    index_names = nested_index_names(param_map["parsed_value"])
                    data = pd.DataFrame(map_table, columns=index_names + [unit_name]).set_index(index_names[0])[unit_name]
                    data.index = [pd.Timestamp(i) for i in data.index.astype("string")]
                    if rps:
                        data = data.loc[rps[scenario_name.split("__")[0]].index]*rps[scenario_name.split("__")[0]].loc[:,"weight"]
                    if node_name != "atmosphere":
                        energy_map[alte_name].loc[energy_map[alte_name].shape[0],:] = [node_name.split("_")[0],unit_name] + [data[data.index.year==year_i].sum() for year_i in map_years]
                    else:
                        emission_map[alte_name].loc[energy_map[alte_name].shape[0],:] = ["atmosphere",unit_name] + [data[data.index.year==year_i].sum() for year_i in map_years]

    analyzed_vehs = ["car","van","bus","truck","aviation","maritime","rail"]
    for param_map in result_db.get_parameter_value_items(parameter_definition_name = "connection_flow"):
        scenario_name, timestamp = param_map["alternative_name"].split("@")
        timestamp = pd.Timestamp(timestamp)
        if scenario_name in latest_alternatives:
            if timestamp == latest_alternatives[scenario_name]:
                alte_name = scenario_name
                if "from_node" in param_map["entity_byname"]:
                    link_name = param_map["entity_byname"][1]
                    node_name = param_map["entity_byname"][2]

                    if any(analyzed_node in node_name for analyzed_node in analyzed_nodes) and any(analyzed_veh in link_name for analyzed_veh in analyzed_vehs):
                        map_table = convert_map_to_table(param_map["parsed_value"])
                        index_names = nested_index_names(param_map["parsed_value"])
                        data = pd.DataFrame(map_table, columns=index_names + [link_name]).set_index(index_names[0])[link_name]
                        data.index = [pd.Timestamp(i) for i in data.index.astype("string")]
                        if rps:
                            data = data.loc[rps[scenario_name.split("__")[0]].index]*rps[scenario_name.split("__")[0]].loc[:,"weight"]

                        unit_to_node_map[link_name] = link_name.split("_")[1]
                        energy_map[alte_name].loc[energy_map[alte_name].shape[0],:] = [node_name.split("_")[0],link_name] + [data[data.index.year==year_i].sum() for year_i in map_years]

    for param_map in result_db.get_parameter_value_items(parameter_definition_name = "connection_flow"):
        scenario_name, timestamp = param_map["alternative_name"].split("@")
        timestamp = pd.Timestamp(timestamp)
        if scenario_name in latest_alternatives:
            if timestamp == latest_alternatives[scenario_name]:
                alte_name = scenario_name
                if "from_node" in param_map["entity_byname"]:
                    link_name = param_map["entity_byname"][1]
                    node_name = param_map["entity_byname"][2]

                    if node_name.split("_")[0] == link_name.split("_")[1] and node_name.split("_")[1] == link_name.split("_")[0]:
                        map_table = convert_map_to_table(param_map["parsed_value"])
                        index_names = nested_index_names(param_map["parsed_value"])
                        data = pd.DataFrame(map_table, columns=index_names + [link_name]).set_index(index_names[0])[link_name]
                        data.index = [pd.Timestamp(i) for i in data.index.astype("string")]
                        if rps:
                            data = data.loc[rps[scenario_name.split("__")[0]].index]*rps[scenario_name.split("__")[0]].loc[:,"weight"]

                        if len(node_name.split("_")) > 1:
                            #print(link_name)
                            p1 = node_name.split("_")[1]
                            p2 = link_name.split("_")[2]
                            commodity = link_name.split("_")[1]
                            #print(p1,p2,commodity)
                            flows_map[alte_name].loc[flows_map[alte_name].shape[0],:] = [p1,p2,commodity] + [data[data.index.year==year_i].sum() for year_i in map_years]
                    elif "CH4-geo-formation" in link_name or "salt-cavern" in link_name:
                        map_table = convert_map_to_table(param_map["parsed_value"])
                        index_names = nested_index_names(param_map["parsed_value"])
                        data = pd.DataFrame(map_table, columns=index_names + [link_name]).set_index(index_names[0])[link_name]
                        data.index = [pd.Timestamp(i) for i in data.index.astype("string")]
                        if rps:
                            data = data.loc[rps[scenario_name.split("__")[0]].index]*rps[scenario_name.split("__")[0]].loc[:,"weight"]
                        flows_sto[alte_name].loc[flows_sto[alte_name].shape[0],:] = [node_name,link_name] + [data[data.index.year==year_i].sum() for year_i in map_years]

    for param_map in result_db.get_parameter_value_items(parameter_definition_name = "demand"):
        scenario_name, timestamp = param_map["alternative_name"].split("@")
        timestamp = pd.Timestamp(timestamp)
        if scenario_name in latest_alternatives:
            if timestamp == latest_alternatives[scenario_name]:
                alte_name = scenario_name
                node_name = param_map["entity_byname"][1]
                if any(analyzed_node == node_name.split("_")[0] and len(node_name.split("_"))==2  for analyzed_node in analyzed_nodes):
                    map_table = convert_map_to_table(param_map["parsed_value"])
                    index_names = nested_index_names(param_map["parsed_value"])
                    data = pd.DataFrame(map_table, columns=index_names + [node_name]).set_index(index_names[0])[node_name]
                    data.index = [pd.Timestamp(i) for i in data.index.astype("string")]
                    if rps:
                        data = data.loc[rps[scenario_name.split("__")[0]].index]*rps[scenario_name.split("__")[0]].loc[:,"weight"]

                    unit_to_node_map[node_name] = "residual-"+node_name.split("_")[0]
                    energy_map[alte_name].loc[energy_map[alte_name].shape[0],:] = [node_name.split("_")[0],node_name] + [data[data.index.year==year_i].sum() for year_i in map_years]

    for param_map in result_db.get_parameter_value_items(parameter_definition_name = "units_invested_available"):
        scenario_name, timestamp = param_map["alternative_name"].split("@")
        timestamp = pd.Timestamp(timestamp)
        if scenario_name in latest_alternatives:
            if timestamp == latest_alternatives[scenario_name]:
                alte_name = scenario_name
                unit_name = param_map["entity_byname"][1]

                map_table = convert_map_to_table(param_map["parsed_value"])
                index_names = nested_index_names(param_map["parsed_value"])
                data = pd.DataFrame(map_table, columns=index_names + [unit_name]).set_index(index_names[0])
                
                capacity_value = [parameter_i["parsed_value"].values[0] for parameter_i in result_db.get_parameter_value_items(parameter_definition_name = "unit_capacity") if unit_name in parameter_i["entity_byname"]][0]
                units_cap[alte_name].loc[units_cap[alte_name].shape[0],:] = [unit_name] + (capacity_value*data[unit_name]).to_list()
        
                invested = result_db.get_parameter_value_item(entity_class_name = param_map["entity_class_name"], alternative_name = param_map["alternative_name"],parameter_definition_name = "units_invested", entity_byname = param_map["entity_byname"])
                map_table = convert_map_to_table(invested["parsed_value"])
                index_names = nested_index_names(invested["parsed_value"])
                data = pd.DataFrame(map_table, columns=index_names + [unit_name]).set_index(index_names[0])
                
                units_inv[alte_name].loc[units_inv[alte_name].shape[0],:] = [unit_name] + (capacity_value*data[unit_name]).to_list()

                decommissioned = result_db.get_parameter_value_item(entity_class_name = param_map["entity_class_name"], alternative_name = param_map["alternative_name"],parameter_definition_name = "units_mothballed", entity_byname = param_map["entity_byname"])
                map_table = convert_map_to_table(decommissioned["parsed_value"])
                index_names = nested_index_names(decommissioned["parsed_value"])
                data = pd.DataFrame(map_table, columns=index_names + [unit_name]).set_index(index_names[0])
                
                units_dec[alte_name].loc[units_inv[alte_name].shape[0],:] = [unit_name] + (capacity_value*data[unit_name]).to_list()

    for param_map in result_db.get_parameter_value_items(parameter_definition_name = "node_state"):
        scenario_name, timestamp = param_map["alternative_name"].split("@")
        timestamp = pd.Timestamp(timestamp)
        if scenario_name in latest_alternatives:
            if timestamp == latest_alternatives[scenario_name]:
                storage_name = param_map["entity_byname"][1]

                map_table = convert_map_to_table(param_map["parsed_value"])
                index_names = nested_index_names(param_map["parsed_value"])
                data = pd.DataFrame(map_table, columns=index_names + [storage_name]).set_index(index_names[0])[storage_name]
                data.index = [pd.Timestamp(i) for i in data.index.astype("string")]
                node_state[scenario_name] = pd.concat([node_state[scenario_name],data],ignore_index=False)
            node_state[scenario_name] = node_state[scenario_name].sort_index()
    return unit_to_flows, energy_map, unit_to_node_map, emission_map, units_cap, units_inv, units_dec, flows_map, flows_sto, node_state

def df_to_sankey(energy_df, emission_df, years_map):
 
    # Generate Sankey diagram for each scenario
    for scenario in energy_df.scenario.unique():
        df =  energy_df[energy_df.scenario == scenario].copy()
        # Get unique labels
        labels = pd.unique(df[['source', 'target']].values.ravel())
        label_indices = {label: idx for idx, label in enumerate(labels)}

        # Map labels to indices
        df['source_idx'] = df['source'].map(label_indices)
        df['target_idx'] = df['target'].map(label_indices)

        # Generate maximally distinct colors
        golden_ratio = (1 + 5**0.5) / 2
        colors = [f'hsl({int((i * 360 / golden_ratio) % 360)}, 70%, 50%)' for i in range(len(labels))]
        link_colors = [f'hsla({int((source_idx * 360 / golden_ratio) % 360)}, 70%, 50%, 0.4)' for source_idx in df['source_idx']]
        
        for year in list(years_map.values()): 
            # Create Sankey diagram
            fig = go.Figure(data=[go.Sankey(node=dict(pad=30,thickness=20,line=dict(color="black", width=0.5),label=labels.tolist(),color=colors),
                                            link=dict(source=df['source_idx'],target=df['target_idx'],value=df[year],color=link_colors))])

            # Layout and export
            fig.update_layout(title_text=f"Sankey Diagram - {scenario} - {year}", font_size=10)
            fig.write_html(f"pictures/sankey_{scenario}_{year}.html")

    for scenario in emission_df.scenario.unique():
        df =  energy_df[energy_df.scenario == scenario].copy()
        # Get unique labels
        labels = pd.unique(df[['source', 'target']].values.ravel())
        label_indices = {label: idx for idx, label in enumerate(labels)}

        # Map labels to indices
        df['source_idx'] = df['source'].map(label_indices)
        df['target_idx'] = df['target'].map(label_indices)

        # Generate maximally distinct colors
        golden_ratio = (1 + 5**0.5) / 2
        colors = [f'hsl({int((i * 360 / golden_ratio) % 360)}, 70%, 50%)' for i in range(len(labels))]
        link_colors = [f'hsla({int((source_idx * 360 / golden_ratio) % 360)}, 70%, 50%, 0.4)' for source_idx in df['source_idx']]
        
        for year in list(years_map.values()): 
            # Create Sankey diagram
            fig = go.Figure(data=[go.Sankey(node=dict(pad=30,thickness=20,line=dict(color="black", width=0.5),label=labels.tolist(),color=colors),
                                            link=dict(source=df['source_idx'],target=df['target_idx'],value=df[year],color=link_colors))])

            # Layout and export
            fig.update_layout(title_text=f"Sankey Diagram - {scenario} - {year}", font_size=10)
            fig.write_html(f"pictures/sankey_emissions_{scenario}_{year}.html")

def get_representative_periods():

    rps={}
    for entity_tb in sopt_db.get_parameter_value_items(parameter_definition_name = "block_start"):

        if "representative_period" in entity_tb["entity_byname"][0]:
            if entity_tb["entity_byname"][0] not in rps:
                rps[entity_tb["entity_byname"][0]] = {}

            if entity_tb["alternative_name"] not in rps[entity_tb["entity_byname"][0]]:
                rps[entity_tb["entity_byname"][0]][entity_tb["alternative_name"]] = {}
                 
            rps[entity_tb["entity_byname"][0]][entity_tb["alternative_name"]]["start"] = json.loads(entity_tb["value"])["data"]
            block_end = sopt_db.get_parameter_value_item(entity_class_name = "temporal_block", alternative_name = entity_tb["alternative_name"], entity_byname = entity_tb["entity_byname"], parameter_definition_name = "block_end")
            rps[entity_tb["entity_byname"][0]][entity_tb["alternative_name"]]["end"] = (pd.Timestamp(json.loads(block_end["value"])["data"])-pd.Timedelta("1h")).isoformat()
            weight = sopt_db.get_parameter_value_item(entity_class_name = "temporal_block", alternative_name = entity_tb["alternative_name"], entity_byname = entity_tb["entity_byname"], parameter_definition_name = "weight")
            rps[entity_tb["entity_byname"][0]][entity_tb["alternative_name"]]["weight"] = weight["parsed_value"]
            rps[entity_tb["entity_byname"][0]][entity_tb["alternative_name"]]["dates"] = pd.date_range(start=rps[entity_tb["entity_byname"][0]][entity_tb["alternative_name"]]["start"],end=rps[entity_tb["entity_byname"][0]][entity_tb["alternative_name"]]["end"],freq="1h").tolist()

    if rps:
        indexes = {}
        weights = {}
        for rp in rps:
            for alternative in rps[rp]:
                if alternative not in indexes:
                    indexes[alternative] = []
                indexes[alternative] += rps[rp][alternative]["dates"]
                if alternative not in weights:
                    weights[alternative] = []
                weights[alternative] += [rps[rp][alternative]["weight"]]*len(rps[rp][alternative]["dates"])

        all_rps_years = {alternative:pd.DataFrame(weights[alternative],index=indexes[alternative],columns=["weight"]).sort_index() for alternative in indexes}
        concat_alter = {}
        for alternative in all_rps_years:
            if alternative.split("_")[1] not in concat_alter:
                concat_alter[alternative.split("_")[1]] = []
            concat_alter[alternative.split("_")[1]].append(alternative)
        
        all_rps={alternative:pd.concat([all_rps_years[alt_i] for alt_i in concat_alter[alternative]],ignore_index=False) for alternative in concat_alter}
    else:
        all_rps = {}
    return all_rps

def run_streamlit_app(app_path: str):
    original_argv = sys.argv.copy()
    try:
        sys.argv = ["streamlit", "run", app_path]
        stcli.main()
    finally:
        sys.argv = original_argv

def main():

    resolution = 1
    map_years = {2030:"y2030",2041:"y2040",2050:"y2050"}
    unit_to_flows, energy_map, unit_to_node_map, emission_map, units_cap, units_inv, units_dec, flows_map, flows_sto, node_state = copy.deepcopy(from_DB_to_df(map_years))

    node_state = {scenario_map.get(k, k): v for k, v in node_state.items()}
    with open('files_out/node_state.dill', 'wb') as file:
        dill.dump(node_state,file)

    energy_list   = []
    emission_list = []
    for alternative_name in energy_map:
        energy_map[alternative_name]["polygon"] = energy_map[alternative_name]["target"].map(extract_polygon)
        energy_map[alternative_name]["technology"] = energy_map[alternative_name]["target"].values
        energy_map[alternative_name]["target"] = energy_map[alternative_name]["target"].map(unit_to_node_map)
        energy_map[alternative_name]["target"] = energy_map[alternative_name]["target"].map(node_map_sankey)
        energy_map[alternative_name]["source"] = energy_map[alternative_name]["source"].map(node_map_sankey)
        energy_map[alternative_name][list(map_years.values())] /= 1e6
        energy_map[alternative_name] = energy_map[alternative_name][energy_map[alternative_name]["source"] != energy_map[alternative_name]["target"]]
        energy_map[alternative_name] = energy_map[alternative_name][energy_map[alternative_name][list(map_years.values())].sum(axis=1) > 0.001]
        energy_map[alternative_name]["scenario"] = alternative_name
        energy_list.append(energy_map[alternative_name])

        emission_map[alternative_name]["polygon"] = emission_map[alternative_name]["target"].map(extract_polygon)
        emission_map[alternative_name].loc[emission_map[alternative_name]["polygon"]=="re","polygon"] = emission_map[alternative_name]["source"].map(extract_polygon).loc[emission_map[alternative_name]["polygon"]=="re"].values
        emission_map[alternative_name]["technology"] = emission_map[alternative_name]["target"].values
        emission_map[alternative_name]["target"] = emission_map[alternative_name]["target"].map(unit_to_node_map|{"atmosphere":"atmosphere"})
        emission_map[alternative_name]["source"] = emission_map[alternative_name]["source"].map(unit_to_node_map|{"atmosphere":"atmosphere"})
        emission_map[alternative_name]["target"] = emission_map[alternative_name]["target"].map(node_map)
        emission_map[alternative_name]["source"] = emission_map[alternative_name]["source"].map(node_map)
        # [alternative_name] = emission_map[alternative_name].groupby(["source","target"]).sum().reset_index()
        emission_map[alternative_name] = emission_map[alternative_name][emission_map[alternative_name][list(map_years.values())].sum(axis=1) > 0.001]
        emission_map[alternative_name]["scenario"] = alternative_name
        emission_list.append(emission_map[alternative_name])

    energy_df = pd.concat(energy_list,axis=0,ignore_index=True)
    emission_df = pd.concat(emission_list,axis=0,ignore_index=True)
    energy_df["scenario"] = energy_df["scenario"].map(scenario_map)
    emission_df["scenario"] = emission_df["scenario"].map(scenario_map)
    energy_df.to_csv(f"files_out/energy_flows.csv")
    emission_df.to_csv(f"files_out/emissions_flows.csv")

    installed_cap = []
    for alte_name in units_cap:
        units_cap[alte_name]["node"] = units_cap[alte_name]["unit_name"].values
        units_cap[alte_name]["node"] = units_cap[alte_name]["node"].map(unit_to_node_map)
        units_cap[alte_name]["node"] = units_cap[alte_name]["node"].map(node_map)
        units_cap[alte_name] = units_cap[alte_name][units_cap[alte_name][list(map_years.values())].sum(axis=1) > 0.001]
        units_cap[alte_name]["scenario"] = alte_name
        installed_cap.append(units_cap[alte_name])
    installed_cap_df = pd.concat(installed_cap,axis=0,ignore_index=True)
    installed_cap_df['polygon'] = installed_cap_df["unit_name"].map(extract_polygon)
    installed_cap_df['technology'] = installed_cap_df["unit_name"].map(apply_unit_name)
    installed_cap_df["scenario"] = installed_cap_df["scenario"].map(scenario_map)
    installed_cap_df_total = (installed_cap_df.groupby(["node","technology","scenario"], dropna=False)[["y2030","y2040","y2050"]].sum().reset_index()).assign(polygon="Europe", unit_name="EU_total")
    installed_cap_df = pd.concat([installed_cap_df,installed_cap_df_total],ignore_index=True)
    installed_cap_df.round(2).to_csv("files_out/installed_capacity.csv")

    invested_cap = []
    for alte_name in units_inv:
        units_inv[alte_name]["node"] = units_inv[alte_name]["unit_name"].values
        units_inv[alte_name]["node"] = units_inv[alte_name]["node"].map(unit_to_node_map)
        units_inv[alte_name]["node"] = units_inv[alte_name]["node"].map(node_map)
        units_inv[alte_name] = units_inv[alte_name][units_inv[alte_name][list(map_years.values())].sum(axis=1) > 0.001]
        units_inv[alte_name]["scenario"] = alte_name
        invested_cap.append(units_inv[alte_name])
    invested_cap_df = pd.concat(invested_cap,axis=0,ignore_index=True)
    invested_cap_df['polygon'] = invested_cap_df["unit_name"].map(extract_polygon)
    invested_cap_df['technology'] = invested_cap_df["unit_name"].map(apply_unit_name)
    invested_cap_df["scenario"] = invested_cap_df["scenario"].map(scenario_map)
    invested_cap_df_total = (invested_cap_df.groupby(["node","technology","scenario"], dropna=False)[["y2030","y2040","y2050"]].sum().reset_index()).assign(polygon="Europe", unit_name="EU_total")
    invested_cap_df = pd.concat([invested_cap_df,invested_cap_df_total],ignore_index=True)
    invested_cap_df.round(2).to_csv("files_out/invested_capacity.csv")

    decommissioned = []
    for alte_name in units_dec:
        units_dec[alte_name]["node"] = units_dec[alte_name]["unit_name"].values
        units_dec[alte_name]["node"] = units_dec[alte_name]["node"].map(unit_to_node_map)
        units_dec[alte_name]["node"] = units_dec[alte_name]["node"].map(node_map)
        units_dec[alte_name] = units_dec[alte_name][units_dec[alte_name][list(map_years.values())].sum(axis=1) > 0.001]
        units_dec[alte_name]["scenario"] = alte_name
        decommissioned.append(units_dec[alte_name])
    decommissioned_df = pd.concat(decommissioned,axis=0,ignore_index=True)
    decommissioned_df['polygon'] = decommissioned_df["unit_name"].map(extract_polygon)
    decommissioned_df['technology'] = decommissioned_df["unit_name"].map(apply_unit_name)
    decommissioned_df["scenario"] = decommissioned_df["scenario"].map(scenario_map)
    decommissioned_df_total = (decommissioned_df.groupby(["node","technology","scenario"], dropna=False)[["y2030","y2040","y2050"]].sum().reset_index()).assign(polygon="Europe", unit_name="EU_total")
    decommissioned_df = pd.concat([decommissioned_df,decommissioned_df_total],ignore_index=True)
    decommissioned_df.round(2).to_csv("files_out/decommissioned_capacity.csv")

    unit2node_flow = []
    for alte_name in unit_to_flows:
        unit_to_flows[alte_name]["node"] = unit_to_flows[alte_name]["node"].map(node_map)
        unit_to_flows[alte_name] = unit_to_flows[alte_name][unit_to_flows[alte_name][list(map_years.values())].sum(axis=1) > 0.001]
        unit_to_flows[alte_name]["scenario"] = alte_name
        unit2node_flow.append(unit_to_flows[alte_name])
    unit2node_flow_df = pd.concat(unit2node_flow,axis=0,ignore_index=True)
    unit2node_flow_df['polygon'] = unit2node_flow_df["unit_name"].map(extract_polygon)
    unit2node_flow_df['technology'] = unit2node_flow_df["unit_name"].map(apply_unit_name)
    unit2node_flow_df["scenario"] = unit2node_flow_df["scenario"].map(scenario_map)
    unit2node_flow_df_total = (unit2node_flow_df.groupby(["node","technology","scenario"], dropna=False)[["y2030","y2040","y2050"]].sum().reset_index()).assign(polygon="Europe", unit_name="EU_total")
    unit2node_flow_df = pd.concat([unit2node_flow_df,unit2node_flow_df_total],ignore_index=True)
    unit2node_flow_df.round(2).to_csv("files_out/unit_to_flows.csv")

    flows = []
    for alternative_name in flows_map:
        flows_map[alternative_name]["commodity"] = flows_map[alternative_name]["commodity"].map(node_map)
        flows_map[alternative_name][list(map_years.values())] *= resolution/1e6
        flows_map[alternative_name] = flows_map[alternative_name][flows_map[alternative_name]["source"] != flows_map[alternative_name]["target"]]
        flows_map[alternative_name] = flows_map[alternative_name][flows_map[alternative_name][list(map_years.values())].sum(axis=1) > 0.001]
        # Merging both directions of the flow
        '''flows_map[alternative_name]['pair'] = flows_map[alternative_name].apply(lambda x: '-'.join(sorted([x['source'], x['target']])), axis=1)
        flows_map[alternative_name] = flows_map[alternative_name].groupby(['pair', 'commodity'], as_index=False)[['y2030','y2040','y2050']].sum()
        flows_map[alternative_name][['source','target']] = flows_map[alternative_name]['pair'].str.split('-', expand=True)
        flows_map[alternative_name] = flows_map[alternative_name].drop(columns=['pair'])'''
        flows_map[alternative_name]["scenario"] = alternative_name
        flows_map[alternative_name] = flows_map[alternative_name][["source","target","commodity","y2030","y2040","y2050","scenario"]]
        flows.append(flows_map[alternative_name])
    flows_df = pd.concat(flows,axis=0,ignore_index=True)
    flows_df["scenario"] = flows_df["scenario"].map(scenario_map)
    flows_df.to_csv("files_out/crossborder_flows.csv")

    #df_to_plots(installed_cap_df, invested_cap_df, decommissioned_df, flows, map_years, "node")
    #df_to_sankey(energy_df, emission_df, map_years)
    
if __name__ == "__main__":
    main()
    # run_streamlit_app("app.py")
