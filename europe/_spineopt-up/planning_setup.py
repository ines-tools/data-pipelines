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

if len(sys.argv) > 1:
    url_spineopt = sys.argv[1]
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

def update_parameter_value(db_map : DatabaseMapping, id_int : int, class_name : str,parameter : str,alternative : str,elements : tuple,value : any) -> None:
    db_value, value_type = api.to_database(value)
    _, error = db_map.update_parameter_value_item(id=id_int, entity_class_name=class_name,entity_byname=elements,parameter_definition_name=parameter,alternative_name=alternative,value=db_value,type=value_type)
    if error:
        raise RuntimeError(error)

def add_or_update_parameter_value(db_map : DatabaseMapping, class_name : str,parameter : str,alternative : str,elements : tuple,value : any) -> None:
    db_value, value_type = api.to_database(value)
    db_map.add_or_update_parameter_value(entity_class_name=class_name,entity_byname=elements,parameter_definition_name=parameter,alternative_name=alternative,value=db_value,type=value_type)

def add_entity_group(db_map : DatabaseMapping, class_name : str, group : str, member : str) -> None:
    _, error = db_map.add_entity_group_item(group_name = group, member_name = member, entity_class_name=class_name)
    if error is not None:
        raise RuntimeError(error)

def add_entity(db_map : DatabaseMapping, class_name : str, name : tuple, ent_description = None) -> None:
    _, error = db_map.add_entity_item(entity_byname=name, entity_class_name=class_name, description = ent_description)
    if error is not None:
        raise RuntimeError(error)

def add_parameter_value(db_map : DatabaseMapping,class_name : str,parameter : str,alternative : str,elements : tuple,value : any) -> None:
    db_value, value_type = api.to_database(value)
    _, error = db_map.add_parameter_value_item(entity_class_name=class_name,entity_byname=elements,parameter_definition_name=parameter,alternative_name=alternative,value=db_value,type=value_type)
    if error:
        raise RuntimeError(error)

def add_scenario(db_map : DatabaseMapping,name_scenario : str) -> None:
    _, error = db_map.add_scenario_item(name=name_scenario)
    if error is not None:
        raise RuntimeError(error)

def add_scenario_alternative(db_map : DatabaseMapping,name_scenario : str, name_alternative : str, rank_int = None) -> None:
    _, error = db_map.add_scenario_alternative_item(scenario_name = name_scenario, alternative_name = name_alternative, rank = rank_int)
    if error is not None:
        raise RuntimeError(error)

def investment_cost_update():
    
    with DatabaseMapping(url_spineopt) as sopt_db:

        entities = ["unit","connection","node"]
        icost    = ["unit_investment_cost","connection_investment_cost","storage_investment_cost"]
        fcost    = ["fom_cost","","storage_fom_cost"]
        ilife    = ["unit_investment_econ_lifetime","connection_investment_econ_lifetime","storage_investment_econ_lifetime"]
        tlife    = ["unit_investment_tech_lifetime","connection_investment_tech_lifetime","storage_investment_tech_lifetime"]
        isense   = ["unit_investment_lifetime_sense","connection_investment_lifetime_sense","storage_investment_lifetime_sense"]
        irate    = ["unit_discount_rate_technology_specific","connection_discount_rate_technology_specific","storage_discount_rate_technology_specific"]
        
        for index, entity_class_name in enumerate(entities): 

            for parameter_map in sopt_db.get_parameter_value_items(entity_class_name = entities[index], parameter_definition_name = icost[index]):
                
                lifetime_dict = sopt_db.get_parameter_value_item(entity_class_name = entities[index], parameter_definition_name = ilife[index], alternative_name = parameter_map["alternative_name"], entity_byname = parameter_map["entity_byname"])
                if not lifetime_dict:
                    exit("Annuities are implemented using economic lifetime. Economic lifetime not found.")
                else:
                    lifetime = int(json.loads(lifetime_dict["value"])["data"][:-1])
                    techlife_dict = sopt_db.get_parameter_value_item(entity_class_name = entities[index], parameter_definition_name = tlife[index], alternative_name = parameter_map["alternative_name"], entity_byname = parameter_map["entity_byname"])
                    techlife = int(json.loads(techlife_dict["value"])["data"][:-1])
                    add_or_update_parameter_value(sopt_db,entity_class_name,isense[index],"Base",techlife_dict["entity_byname"],"<=")
                
                rate_dict = sopt_db.get_parameter_value_item(entity_class_name = entities[index], parameter_definition_name = irate[index], alternative_name = parameter_map["alternative_name"], entity_byname = parameter_map["entity_byname"])
                if not rate_dict:
                    rate_list = sopt_db.get_parameter_value_items(parameter_definition_name = "discount_rate")
                    if not rate_list:
                        print("Model discount rate not found. Using 0.05 as default")
                        rate = 0.05
                    else:
                        rate = rate_list[0]["parsed_value"]
                else:
                    rate = rate_dict["parsed_value"]
                annuity_factor = 1/lifetime #rate*(1+rate)**(lifetime)/((1+rate)**(lifetime)-1)

                # fom cost
                fom_dict = sopt_db.get_parameter_value_item(entity_class_name = entities[index], parameter_definition_name = fcost[index], alternative_name = parameter_map["alternative_name"], entity_byname = parameter_map["entity_byname"])
                if not fom_dict:
                    fom_cost_condition = False
                    print("FOM cost not found for ", parameter_map["entity_name"])
                else:
                    fom_cost_condition = True
                    fom_cost = fom_dict["parsed_value"]
                    # print("new value for the fom cost", (fom_cost.values[0] if fom_dict["type"]=="time_series" else fom_cost))
                    add_or_update_parameter_value(sopt_db, parameter_map["entity_class_name"], fcost[index], fom_dict["alternative_name"], fom_dict["entity_byname"], (fom_cost.values[2] if fom_dict["type"]=="time_series" else fom_cost))

                
                dates = ["2030-01-01T00:00:00","2040-01-01T00:00:00","2050-01-01T00:00:00","2060-01-01T00:00:00"]
                year_dur = [30,20,10]
                if parameter_map["type"] == "float":
                    annual_cost = (parameter_map["parsed_value"] * annuity_factor)
                    new_values  = [annual_cost*min(lifetime,year_dur[0]),annual_cost*min(lifetime,year_dur[1]),annual_cost*min(lifetime,year_dur[2]),annual_cost*min(lifetime,year_dur[2])]
                    new_value   = {"type":"time_series","data":dict(zip(dates,new_values))}
                else:
                    annual_cost = parameter_map["parsed_value"].values * annuity_factor
                    new_values = [(annual_cost[0] + ((fom_cost.values[0] - fom_cost.values[2])*8760 if fom_cost_condition else 0.0))*min(lifetime,year_dur[0]),(annual_cost[1] + ((fom_cost.values[1] - fom_cost.values[2])*8760 if fom_cost_condition else 0.0))*min(lifetime,year_dur[1]),annual_cost[2]*min(lifetime,year_dur[2]),annual_cost[2]*min(lifetime,year_dur[2])]
                    new_value = {"type":parameter_map["type"], "data": dict(zip(dates,new_values))}   
                
                # print("new value for the value cost", parameter_map["entity_class_name"], parameter_map["parameter_definition_name"], parameter_map["alternative_name"], parameter_map["entity_byname"], new_value)
                add_or_update_parameter_value(sopt_db, parameter_map["entity_class_name"], parameter_map["parameter_definition_name"], parameter_map["alternative_name"], parameter_map["entity_byname"], new_value)

        
        try:
            sopt_db.commit_session("Update Investment Costs")
        except DBAPIError as e:
            print("###################################################################### commit error")  

def air_ground_heatpump():

    with DatabaseMapping(url_spineopt) as sopt_db:
        
        for entity_name in [element["name"] for element in sopt_db.get_entity_items(entity_class_name = "unit") if "ground-heatpump_" in element["name"]]:
            polygon_name = entity_name.split("_")[1]
            add_entity(sopt_db,"user_constraint",("heatpump-ratio"+"_"+polygon_name,))
            add_entity(sopt_db,"unit__user_constraint",(entity_name,"heatpump-ratio"+"_"+polygon_name))
            add_parameter_value(sopt_db,"unit__user_constraint","units_invested_coefficient","Base",(entity_name,"heatpump-ratio"+"_"+polygon_name),1.0)
            add_entity(sopt_db,"unit__user_constraint",("air-heatpump"+"_"+polygon_name,"heatpump-ratio"+"_"+polygon_name))
            add_parameter_value(sopt_db,"unit__user_constraint","units_invested_coefficient","Base",("air-heatpump"+"_"+polygon_name,"heatpump-ratio"+"_"+polygon_name),-0.3)
        
        try:
            sopt_db.commit_session("Add User Constraint Heat Pumps")
        except DBAPIError as e:
            print("commit error")  

def manage_output():
    with DatabaseMapping(url_spineopt) as sopt_db:

        report_name = "default_report"
        add_entity(sopt_db,"report",(report_name,))
        add_entity(sopt_db,"model__report",("capacity_planning",report_name))
        outputs = ["unit_capacity","connection_capacity","node_state_cap","demand",
                   "connections_invested","connections_invested_available","connections_decommissioned","units_invested","units_invested_available","units_mothballed",
                   "storages_invested","storages_invested_available","storages_decommissioned","unit_flow","connection_flow","node_state","node_injection","weight","fractional_demand",
                   "unit_investment_cost","connection_investment_cost","storage_investment_cost",
                   "unit_investment_costs","connection_investment_costs","storage_investment_costs","fixed_om_costs","variable_om_costs","fuel_costs","connection_flow_costs","taxes","objective_penalties",
                   "node_slack_neg","node_slack_pos",
                   "constraint_nodal_balance","constraint_units_available",
                   "bound_units_on"]
        
        for output in outputs:
            add_entity(sopt_db,"output",(output,))
            add_entity(sopt_db,"report__output",(report_name,output))
        sopt_db.commit_session("Added outputs")

def solver_options():
    with DatabaseMapping(url_spineopt) as sopt_db:
        map_options = {"type":"map","index_type":"str","index_name":"x","data":
                       {"HiGHS.jl" :{"type":"map","index_type":"str","index_name":"x","data":{"presolve":"on","time_limit":3600.01,"user_bound_scale":-9}},
                        "Gurobi.jl":{"type":"map","index_type":"str","index_name":"x","data":{"Method":2.0,"NumericFocus":2.0,"Crossover":0.0}}}}
        
        add_parameter_value(sopt_db,"model","db_mip_solver_options","Base",("capacity_planning",),map_options)
        add_parameter_value(sopt_db,"model","db_mip_solver","Base",("capacity_planning",),"HiGHS.jl")
        sopt_db.commit_session("Added solver_options")

def hydro_TB():

    with DatabaseMapping(url_spineopt) as sopt_db:

        reservoir_names =[]
        for reservoir_relation in [entity_res for entity_res in sopt_db.get_entity_items(entity_class_name = "node__temporal_block") if "reservoir" in entity_res["entity_byname"][0]]:
            if reservoir_relation["entity_byname"][0] not in reservoir_names:
                reservoir_names.append(reservoir_relation["entity_byname"][0])
            sopt_db.remove_entity(entity_class_name = "node__temporal_block", name = reservoir_relation["name"])

        for entity_tb in [entity_i for entity_i in sopt_db.get_entity_items(entity_class_name = "temporal_block") if "operations" in entity_i["name"]]:
            add_entity(sopt_db,"temporal_block",("hydro_"+entity_tb["name"],))
            block_start = json.loads(sopt_db.get_parameter_value_item(entity_class_name = "temporal_block",entity_byname = entity_tb["entity_byname"], alternative_name = "Base", parameter_definition_name = "block_start")["value"])["data"]
            block_end = json.loads(sopt_db.get_parameter_value_item(entity_class_name = "temporal_block",entity_byname = entity_tb["entity_byname"], alternative_name = "Base", parameter_definition_name = "block_end")["value"])["data"]
            add_parameter_value(sopt_db,"temporal_block","block_start","Base",("hydro_"+entity_tb["name"],),{"type":"date_time","data":block_start})
            add_parameter_value(sopt_db,"temporal_block","block_end","Base",("hydro_"+entity_tb["name"],),{"type":"date_time","data":block_end})
            add_parameter_value(sopt_db,"temporal_block","weight","Base",("hydro_"+entity_tb["name"],),10.0)
            add_parameter_value(sopt_db,"temporal_block","has_free_start","Base",("hydro_"+entity_tb["name"],),True)

            for alternative_name in ["wy1995","wy2008","wy2009"]:

                param_map = [parameter_i for parameter_i in sopt_db.get_parameter_value_items(entity_class_name = "node", alternative_name = alternative_name, parameter_definition_name = "demand") if "reservoir" in parameter_i["entity_byname"][0]][0]

                map_table = convert_map_to_table(param_map["parsed_value"])
                index_names = nested_index_names(param_map["parsed_value"])
                data = pd.DataFrame(map_table, columns=index_names + ["value"]).set_index(index_names[0])
                data.index = data.index.astype("string")

                data_inflow = data[~data["value"].duplicated()]
                indexes = data_inflow.index.tolist() + ["2019-01-01T00:00:00"]
                array_resolution = [f"{int((pd.Timestamp(indexes[i])-pd.Timestamp(indexes[i-1]))/pd.Timedelta("1D"))}D" for i in range(1,len(indexes))]

                add_parameter_value(sopt_db,"temporal_block","resolution",alternative_name,("hydro_"+entity_tb["name"],),{"type":"array","value_type":"duration","data":array_resolution})

            for reservoir in reservoir_names:
                add_entity(sopt_db,"node__temporal_block",(reservoir,"hydro_"+entity_tb["name"]))
                add_parameter_value(sopt_db,"node__temporal_block","cyclic_condition","Base",(reservoir,"hydro_"+entity_tb["name"]),True)

            
        sopt_db.commit_session("Added hydro_tb")

def industry_TB():
    industrial_nodes = ["steel-secondary","steel-primary","MeOH","glass-float","glass-fibre","glass-container","chemical-PEA","chemical-PE","chemical-olefins,cement","fertiliser-ammonia-NH3","cement"]
    with DatabaseMapping(url_spineopt) as sopt_db:
        inodes = [entity_node["name"] for entity_node in sopt_db.get_entity_items(entity_class_name = "node") if entity_node["name"].split("_")[0] in industrial_nodes]
        iunits = [entity_unit["entity_byname"][0] for entity_unit in sopt_db.get_entity_items(entity_class_name = "unit__to_node") if entity_unit["entity_byname"][1].split("_")[0] in industrial_nodes]
        ref_units = [entity_unit["name"] for entity_unit in sopt_db.get_entity_items(entity_class_name = "unit") if "REF" in entity_unit["name"]]
        for entity_tb in [entity_i for entity_i in sopt_db.get_entity_items(entity_class_name = "temporal_block") if "operations" in entity_i["name"] and "hydro" not in entity_i["name"]]:
            add_entity(sopt_db,"temporal_block",("industry_"+entity_tb["name"],))
            block_start = json.loads(sopt_db.get_parameter_value_item(entity_class_name = "temporal_block",entity_byname = entity_tb["entity_byname"], alternative_name = "Base", parameter_definition_name = "block_start")["value"])["data"]
            block_end = json.loads(sopt_db.get_parameter_value_item(entity_class_name = "temporal_block",entity_byname = entity_tb["entity_byname"], alternative_name = "Base", parameter_definition_name = "block_end")["value"])["data"]
            add_parameter_value(sopt_db,"temporal_block","block_start","Base",("industry_"+entity_tb["name"],),{"type":"date_time","data":block_start})
            add_parameter_value(sopt_db,"temporal_block","block_end","Base",("industry_"+entity_tb["name"],),{"type":"date_time","data":block_end})
            add_parameter_value(sopt_db,"temporal_block","weight","Base",("industry_"+entity_tb["name"],),10.0)
            add_parameter_value(sopt_db,"temporal_block","has_free_start","Base",("industry_"+entity_tb["name"],),True)
            add_parameter_value(sopt_db,"temporal_block","resolution","Base",("industry_"+entity_tb["name"],),{"type":"duration","data":"365D"})
            for inode in inodes:
                add_entity(sopt_db,"node__temporal_block",(inode,"industry_"+entity_tb["name"]))
            for iunit in iunits+ref_units:
                add_entity(sopt_db,"units_on__temporal_block",(iunit,"industry_"+entity_tb["name"]))
        
        sopt_db.commit_session("Added industry_tb")

def main():

    print("Updating invesment costs and FOM costs")
    investment_cost_update()
    print("Heat pump constraints")
    air_ground_heatpump()
    print("managing outputs")
    manage_output()
    print("adding solver options")
    solver_options()


if __name__ == "__main__":
    main()