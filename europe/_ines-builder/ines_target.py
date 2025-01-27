import spinedb_api as api
from spinedb_api import DatabaseMapping
import datetime
import pandas as pd
import sys
from openpyxl import load_workbook
import numpy as np
import json
import yaml 
import time as time_lib

def add_superclass_subclass(db_map : DatabaseMapping, superclass_name : str, subclass_name : str) -> None:
    _, error = db_map.add_superclass_subclass_item(superclass_name=superclass_name, subclass_name=subclass_name)
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

def add_alternative(db_map : DatabaseMapping,name_alternative : str) -> None:
    _, error = db_map.add_alternative_item(name=name_alternative)
    if error is not None:
        raise RuntimeError(error)

def define_polygons(config : dict, region_data : dict) -> dict:
    polygons={"onshore":{},"offshore":{}}
    for country in config["countries"]:
        on_level  = config["countries"][country]["onshore"]
        off_level = config["countries"][country]["offshore"]
        on_poly   = region_data[on_level][region_data[on_level].country == country].id.tolist()
        off_poly  = region_data[off_level][region_data[off_level].country == country].id.tolist()
        polygons["onshore"].update(dict(zip(on_poly,[config["countries"][country]["onshore"]]*len(on_poly))))
        polygons["offshore"].update({item_p:[off_level,region_data[off_level+"_map"][region_data[off_level+"_map"].source==item_p][on_level].tolist()[0]] for item_p in off_poly})
    return polygons

def user_entity_condition(config,entity_class_elements,entity_names,poly,poly_type):

    if poly_type == "off":
        poly_level,poly_connection = config[f"{poly_type}shore_polygons"][poly]
    else:
        poly_level = config[f"{poly_type}shore_polygons"][poly]

    entity_target_names = []
    definition_condition = True
    # Processing entity to get target names and statuses
    for index,element in enumerate(entity_class_elements):
        entity_dict = config["user"].get(element,{}).get(entity_names[index],{})
        status = config["user"][element][entity_names[index]]["status"] if entity_dict else True
        entity_new_name = entity_names[index]+status*("_"+(poly_connection if poly_type == "off" and element == "commodity" else poly))
        entity_target_names.append(entity_new_name)
        if element != "commodity":
            definition_condition *= status

    return entity_target_names,definition_condition,poly_level

def ines_aggregrate(db_source : DatabaseMapping,transformer_df : pd.DataFrame,target_poly : str ,entity_class : tuple,entity_names : tuple,alternative : str,source_parameter : str,weight : str,defaults = None) -> dict:

    # db_source : Spine DB
    # transformer : dataframes
    # target/source_poly : spatial resolution name
    # weight : conversion factor 
    # defaults : default value implemented

    value_ = None
        
    for source_poly in transformer_df.loc[transformer_df.target == target_poly,"source"].tolist():
        
        entity_bynames = entity_names+(source_poly,)
        multiplier = transformer_df.loc[transformer_df.source == source_poly,weight].tolist()[0]
        parameter_value = db_source.get_parameter_value_item(entity_class_name=entity_class,entity_byname=entity_bynames,parameter_definition_name=source_parameter,alternative_name=alternative)
        
        if parameter_value:
            if parameter_value["type"] == "time_series":
                param_value = json.loads(parameter_value["value"].decode("utf-8"))["data"]
                keys = list(param_value.keys())
                vals = multiplier*np.fromiter(param_value.values(), dtype=float)
                if not value_:
                    value_ = {"type":"time_series","data":dict(zip(keys,vals))}
                else:
                    prev_vals = np.fromiter(value_["data"].values(), dtype=float)
                    value_ = {"type":"time_series","data":dict(zip(keys,prev_vals + vals))}  
            elif parameter_value["type"] == "map":
                param_dict = json.loads(parameter_value["value"].decode("utf-8"))
                param_map = param_dict["data"]
                if not value_:
                    value_ = {"type":"map","index_type":param_dict["index_type"],"index_name":param_dict["index_name"],"data":{}}
                    for key, item in param_map.items():
                        if isinstance(item,float):
                            value_["data"][key] = multiplier*item
                        else: # time_series
                            keys = list(param_map["data"][key].keys())
                            vals = multiplier*np.fromiter(param_map["data"][key].values(), dtype=float)
                            value_["data"][key] = {"type":"time_series","data":dict(zip(keys,vals.round(3)))}
                else:
                    for key, item in param_map.items():
                        if isinstance(item,float):
                            value_["data"][key] += multiplier*item
                        else: # time_series
                            prev_vals = np.fromiter(value_["data"][key].values(), dtype=float)
                            keys = list(param_map["data"][key].keys())
                            vals = multiplier*np.fromiter(param_map["data"][key].values(), dtype=float)
                            value_["data"][key] = {"type":"time_series","data":dict(zip(keys,(prev_vals+vals).round(3)))}
            elif parameter_value["type"] == "float":
                value_ = value_ + multiplier*parameter_value["parsed_value"] if value_ else multiplier*parameter_value["parsed_value"]
            # ADD MORE Parameter Types HERE            
        elif defaults != None:
            value_ = defaults if not value_ else value_+defaults
    
    return value_
        
def spatial_transformation(db_source, config, sector):
    
    spatial_data = {}
    for entity_class in config["sys"][sector]["entities"]:
        entity_class_region = f"{entity_class}__region"
        dynamic_params = config["sys"][sector]["parameters"]["dynamic"].get(entity_class_region, {})
        
        if dynamic_params:
            spatial_data[entity_class] = {}
            for entity_class_target, param_source_dict in dynamic_params.items():
                for source_parameter in param_source_dict:
                    spatial_data[entity_class][source_parameter] = {}
                    entities = db_source.get_entity_items(entity_class_name = entity_class)
                    for entity in entities:
                        entity_name = entity["name"]
                        entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
                        entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]
                        
                        spatial_data[entity_class][source_parameter][entity_name] = {}
                        poly_type = "off" if "wind-off" in entity_name else "on"

                        param_list_target = dynamic_params[entity_class_target][source_parameter]  
                        defaults = param_list_target[3]
                        source_level = param_list_target[4][poly_type] if isinstance(param_list_target[4],dict) else param_list_target[4]     
                        multipliers = param_list_target[2]
                        if not multipliers[1]:
                            weight = multipliers[0] 
                        else:
                            for particular_case in multipliers[1]:
                                weight = multipliers[1][particular_case] if any(particular_case in entity_item for entity_item in entity_names) else multipliers[0]
                                break

                        for target_poly in config[f"{poly_type}shore_polygons"]:
                            _,definition_condition,target_level = user_entity_condition(config,entity_class_elements,entity_names,target_poly,poly_type)

                            if definition_condition == True:
                                
                                if source_level != target_level:  
                                    spatial_data[entity_class][source_parameter][entity_name][target_poly] = ines_aggregrate(db_source,config["transformer"][f"{source_level}_{target_level}"],target_poly,entity_class_region,entity_names,"Base",source_parameter,weight,defaults)
                                else:
                                    entity_bynames = entity_names+(target_poly,)
                                    parameter_value = db_source.get_parameter_value_item(entity_class_name=entity_class_region,entity_byname=entity_bynames,parameter_definition_name=source_parameter,alternative_name="Base")
                                    if parameter_value:
                                        if parameter_value["type"] == "time_series":
                                            param_value = json.loads(parameter_value["value"].decode("utf-8"))["data"]
                                            keys = list(param_value.keys())
                                            vals = np.fromiter(param_value.values(), dtype=float)
                                            value_ = {"type":"time_series","data":dict(zip(keys,vals))}     
                                        elif parameter_value["type"] == "map":
                                            param_dict = json.loads(parameter_value["value"].decode("utf-8"))
                                            param_map = param_dict["data"]
                                            value_ = {"type":"map","index_type":param_dict["index_type"],"index_name":param_dict["index_name"],"data":{}}
                                            for key, item in param_map.items():
                                                if isinstance(item,float):
                                                    value_["data"][key] = item
                                                else: # time_series
                                                    keys = list(param_map["data"][key].keys())
                                                    vals = np.fromiter(param_map["data"][key].values(), dtype=float)
                                                    value_["data"][key] = {"type":"time_series","data":dict(zip(keys,vals))}       
                                        elif parameter_value["type"] == "float":
                                            value_ = parameter_value["parsed_value"]
                                    elif defaults != None:
                                        value_ = defaults
                                    else:
                                        value_ = None
                                    spatial_data[entity_class][source_parameter][entity_name][target_poly] = value_   
    return spatial_data

def add_timeline(db_map : DatabaseMapping,config : dict):

    duration_dict = {"type": "array","value_type": "duration","data": []}
    period_dict = {"type": "array","value_type": "str","data": []}
    for year in config["user"]["model"]["planning_years"]:
        add_entity(db_map, "period", ("y"+year,))
        add_parameter_value(db_map,"period","years_represented","Base",("y"+year,),10.0)
        add_parameter_value(db_map,"period","start_time","Base",("y"+year,),{"type":"date_time","data":f"{year if int(year) != 2040 else str(int(year)+1)}-01-01T00:00:00"})
        duration_dict["data"].append("y"+year)
        period_dict["data"].append(config["user"]["model"]["planning_resolution"])

    # temporality
    wy_dict = {"type": "array","value_type": "date_time","data": [config["user"]["timeline"]["historical_alt"][i]["start"] for i in config["user"]["timeline"]["historical_alt"]]}
    add_entity(db_map, "solve_pattern", ("capacity_planning",))
    add_parameter_value(db_map,"solve_pattern","time_resolution","Base",("capacity_planning",),{"type":"duration","data":config["user"]["model"]["operations_resolution"]})
    add_parameter_value(db_map,"solve_pattern","duration","Base",("capacity_planning",),duration_dict)
    add_parameter_value(db_map,"solve_pattern","period","Base",("capacity_planning",),period_dict)
    add_parameter_value(db_map,"solve_pattern","start_time","Base",("capacity_planning",),wy_dict)

def add_nodes(db_map : DatabaseMapping, db_com : DatabaseMapping, config : dict) -> None:
    
    for entity_class in config["sys"]["commodities"]["entities"]:
        entities = db_com.get_entity_items(entity_class_name = entity_class)
        for entity in entities:
            entity_name  = entity["name"] 
            entity_class_target = config["sys"]["commodities"]["entities"][entity_class]
            if config["user"]["commodity"][entity_name]["status"] == True:
                for poly in config["onshore_polygons"]:
                    entity_target_name = entity_name+"_"+poly
                    add_entity(db_map,"node",(entity_target_name,))
                    # default
                    add_parameter_value(db_map,entity_class_target,"node_type","Base",(entity_target_name,),"balance")
            else:
                if (entity_name=="fossil-HC" and config["user"]["commodity"]["HC"]["status"] == False) or (entity_name=="fossil-CH4" and config["user"]["commodity"]["CH4"]["status"] == False):
                    pass
                else:
                    entity_target_name = entity_name
                    add_entity(db_map,"node",(entity_target_name,))
                    # default
                    add_parameter_value(db_map,entity_class_target,"node_type","Base",(entity_target_name,),"commodity")
                    param_list = config["sys"]["commodities"]["parameters"][entity_class][entity_class_target]
                    for param_source in param_list:
                        param_target = param_list[param_source][0]
                        multiplier = param_list[param_source][1]
                        value_ = db_com.get_parameter_value_item(entity_class_name="commodity",entity_byname=(entity_name,),parameter_definition_name=param_source,alternative_name="Base")
                        if value_:
                            value_param = multiplier*value_["parsed_value"] if value_["type"] != "map" else {"type":"map","index_type":"str","index_name":"period","data":{key:multiplier*item for key,item in dict(json.loads(value_["value"])["data"]).items()}}
                            add_parameter_value(db_map,entity_class_target,param_target,"Base",(entity_target_name,),value_param)

def add_electricity_demand(db_map : DatabaseMapping, db_source : DatabaseMapping, config : dict) -> None:
    db_name = "elec_demand"
    start_time = time_lib.time()
    region_params = spatial_transformation(db_source, config, db_name)
    print(f"Time Calculating Aggregation: {time_lib.time()-start_time} s")

    print("ADDING ELEC DEMAND TIME SERIES")
    for entity_class in config["sys"][db_name]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]
            entity_target_names   = []
            status = False 

            for poly in config["onshore_polygons"]:
                entity_target_names,definition_condition,poly_level = user_entity_condition(config,entity_class_elements,entity_names,poly,"on")

                if definition_condition == True:
                    for entity_class_target in config["sys"][db_name]["entities"][entity_class]:
                        
                        # Default Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["default"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["default"][entity_class]:
                                for param_items in config["sys"][db_name]["parameters"]["default"][entity_class][entity_class_target]:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_items[2]])
                                    add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,param_items[1])

                        # Regional Parameter
                        entity_class_region = f"{entity_class}__region"
                        if entity_class_region in config["sys"][db_name]["parameters"]["dynamic"]:
                            dynamic_params = config["sys"][db_name]["parameters"]["dynamic"][entity_class_region].get(entity_class_target, {})
                            for param_source, param_values in dynamic_params.items():
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_values[1]])
                                add_parameter_value(db_map,entity_class_target,param_values[0],"Base",entity_target_name,region_params[entity_class][param_source][entity_name][poly])
                                            
def add_power_sector(db_map : DatabaseMapping, db_source : DatabaseMapping, config : dict) -> None:

    db_name = "power_sector"
    start_time = time_lib.time()
    region_params = spatial_transformation(db_source, config, db_name)
    print(f"Time Calculating Aggregation: {time_lib.time()-start_time} s")

    print("ADDING POWER ELEMENTS")
    for entity_class in config["sys"][db_name]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]
            entity_target_names   = []
            status = False 

            for poly in config["onshore_polygons"]:
                entity_target_names,definition_condition,poly_level = user_entity_condition(config,entity_class_elements,entity_names,poly,"on")
            
                # checking hard-coding conditions
                if "technology" in entity_class_elements and definition_condition == True:
                    for index_in_class in [i for i in range(len(entity_class_elements)) if entity_class_elements[i]=="technology"]:
                        if region_params["technology"]["units_existing"][entity_names[index_in_class]][poly] == 0.0 and config["user"]["technology"][entity_names[index_in_class]]["investment_method"] == "not_allowed":
                            definition_condition *= False

                if definition_condition == True:
                    for entity_class_target in config["sys"][db_name]["entities"][entity_class]:
                        if isinstance(config["sys"][db_name]["entities"][entity_class][entity_class_target],list):
                            for entity_target_building in config["sys"][db_name]["entities"][entity_class][entity_class_target]:
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in entity_target_building])
                                add_entity(db_map,entity_class_target,entity_target_name)
                        # User Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["user"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["user"][entity_class]:
                                param_list = config["sys"][db_name]["parameters"]["user"][entity_class][entity_class_target]
                                for param_target in param_list:
                                    entity_source_name = "__".join([entity_names[i-1] for k in param_list[param_target][2] for i in k])
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_list[param_target][3]])
                                    add_parameter_value(db_map,entity_class_target,param_target,"Base",entity_target_name,config["user"][param_list[param_target][0]][entity_source_name][param_list[param_target][1]])

                        # Default Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["default"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["default"][entity_class]:
                                for param_items in config["sys"][db_name]["parameters"]["default"][entity_class][entity_class_target]:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_items[2]])
                                    add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,param_items[1])
                        # Fixed Parameters
                        if entity_class in config["sys"][db_name]["parameters"]["fixed"]:
                            if entity_class_target in config["sys"][db_name]["parameters"]["fixed"][entity_class]:
                                param_list = config["sys"][db_name]["parameters"]["fixed"][entity_class][entity_class_target]
                                for param_source in param_list:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_list[param_source][2]])
                                    value_ = db_source.get_parameter_value_item(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name=param_source,alternative_name="Base")
                                    if value_:
                                        value_param = param_list[param_source][1]*value_["parsed_value"] if value_["type"] != "map" else {"type":"map","index_type":"str","index_name":"period","data":{key:param_list[param_source][1]*item for key,item in dict(json.loads(value_["value"])["data"]).items()}}
                                        add_parameter_value(db_map,entity_class_target,param_list[param_source][0],"Base",entity_target_name,value_param)
                        
                        # Regional Parameter
                        entity_class_region = f"{entity_class}__region"
                        if entity_class_region in config["sys"][db_name]["parameters"]["dynamic"]:
                            dynamic_params = config["sys"][db_name]["parameters"]["dynamic"][entity_class_region].get(entity_class_target, {})
                            for param_source, param_values in dynamic_params.items():
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_values[1]])
                                add_parameter_value(db_map,entity_class_target,param_values[0],"Base",entity_target_name,region_params[entity_class][param_source][entity_name][poly])
                            
def add_vre_sector(db_map : DatabaseMapping, db_source : DatabaseMapping, config : dict) -> None:

    start_time = time_lib.time()
    region_params = spatial_transformation(db_source, config, "vre")
    print(f"Time Calculating Aggregation: {time_lib.time()-start_time} s")
    print("ADDING VRE ELEMENTS")
    for entity_class in config["sys"]["vre"]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        # print(f"{entity_class} turn")
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]
            
            poly_type = "off" if "wind-off" in entity_name else "on"
            for poly in config[f"{poly_type}shore_polygons"]:
                entity_target_names,definition_condition,poly_level = user_entity_condition(config,entity_class_elements,entity_names,poly,poly_type)

                # checking hard-coding conditions
                if "technology" in entity_class_elements and definition_condition == True:
                    if region_params["technology"]["units_existing"][entity_names[entity_class_elements.index("technology")]][poly] == 0.0 and config["user"]["technology"][entity_names[entity_class_elements.index("technology")]]["investment_method"] == "not_allowed":
                        definition_condition *= False
                    if not region_params["technology__to_commodity"]["profile_limit_upper"][entity_names[entity_class_elements.index("technology")]+"__elec"][poly]:
                        definition_condition *= False

                if definition_condition == True:
                    for entity_class_target in config["sys"]["vre"]["entities"][entity_class]:
                        # Entity Definitions
                        for entity_target_building in config["sys"]["vre"]["entities"][entity_class][entity_class_target]:
                            entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in entity_target_building])
                            add_entity(db_map,entity_class_target,entity_target_name)
                        # User Parameters
                        if entity_class in config["sys"]["vre"]["parameters"]["user"]:
                            user_params = config["sys"]["vre"]["parameters"]["user"][entity_class].get(entity_class_target, {})
                            for param_target, param_values in user_params.items():
                                entity_source_name = "__".join([entity_names[i-1] for k in param_values[2] for i in k])
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_values[3]])
                                add_parameter_value(db_map,entity_class_target,param_target,"Base",entity_target_name,config["user"][param_values[0]][entity_source_name][param_values[1]])
                        # Default Parameters
                        if entity_class in config["sys"]["vre"]["parameters"]["default"]:
                            if entity_class_target in config["sys"]["vre"]["parameters"]["default"][entity_class]:
                                for param_items in config["sys"]["vre"]["parameters"]["default"][entity_class][entity_class_target]:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_items[2]])
                                    add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,param_items[1])
                        # Fixed Parameters
                        if entity_class in config["sys"]["vre"]["parameters"]["fixed"]:
                            if entity_class_target in config["sys"]["vre"]["parameters"]["fixed"][entity_class]:
                                param_list = config["sys"]["vre"]["parameters"]["fixed"][entity_class][entity_class_target]
                                for param_source in param_list:
                                    entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_list[param_source][2]])
                                    value_ = db_source.get_parameter_value_item(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name=param_source,alternative_name="Base")
                                    if value_:
                                        value_param = param_list[param_source][1]*value_["parsed_value"] if value_["type"] != "map" else {"type":"map","index_type":"str","index_name":"period","data":{key:param_list[param_source][1]*item for key,item in dict(json.loads(value_["value"])["data"]).items()}}
                                        add_parameter_value(db_map,entity_class_target,param_list[param_source][0],"Base",entity_target_name,value_param) 
                        # Regional Parameter
                        entity_class_region = f"{entity_class}__region"
                        if entity_class_region in config["sys"]["vre"]["parameters"]["dynamic"]:
                            dynamic_params = config["sys"]["vre"]["parameters"]["dynamic"][entity_class_region].get(entity_class_target, {})
                            for param_source, param_values in dynamic_params.items():
                                print(entity_class_region,entity_class_target,param_source)
                                entity_target_name = tuple(["__".join([entity_target_names[i-1] for i in k]) for k in param_values[1]])
                                add_parameter_value(db_map,entity_class_target,param_values[0],"Base",entity_target_name,region_params[entity_class][param_source][entity_name][poly])

def add_hydro(db_map : DatabaseMapping, db_source : DatabaseMapping, config : dict) -> None:

    db_name = "hydro_systems"
    print("ADDING HYDRO_SYSTEMS")
    for entity_class in config["sys"][db_name]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]
            entity_target_names   = []

            # entity_target_names,definition_condition,poly_level = user_entity_condition(config,entity_class_elements,entity_names,poly,"on")
            
            if entity_names[-1] in config["onshore_polygons"]:
                for entity_class_target in config["sys"][db_name]["entities"][entity_class]:
                    if isinstance(config["sys"][db_name]["entities"][entity_class][entity_class_target],list):
                        for entity_target_building in config["sys"][db_name]["entities"][entity_class][entity_class_target]:
                            entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in entity_target_building])
                            add_entity(db_map,entity_class_target,entity_target_name)

                    # Default Parameters
                    if entity_class in config["sys"][db_name]["parameters"]["default"]:
                        if entity_class_target in config["sys"][db_name]["parameters"]["default"][entity_class]:
                            for param_items in config["sys"][db_name]["parameters"]["default"][entity_class][entity_class_target]:
                                entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in param_items[2]])
                                add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,param_items[1])
                    
                    # Fixed Parameters
                    if entity_class in config["sys"][db_name]["parameters"]["fixed"]:
                        if entity_class_target in config["sys"][db_name]["parameters"]["fixed"][entity_class]:
                            param_list = config["sys"][db_name]["parameters"]["fixed"][entity_class][entity_class_target]
                            for param_source in param_list:
                                entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in param_list[param_source][2]])
                                value_ = db_source.get_parameter_value_item(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name=param_source,alternative_name="Base")
                                if value_:
                                    if value_["type"] == "map": 
                                        param_map = json.loads(value_["value"])
                                        value_param = {"type":"map","index_type":param_map["index_type"],"index_name":param_map["index_name"],"data":{key:param_list[param_source][1]*item for key,item in dict(param_map["data"]).items()}}
                                    elif value_["type"] == "time_series":
                                        param_map = json.loads(value_["value"].decode("utf-8"))["data"]
                                        keys = list(param_map.keys())
                                        vals = param_list[param_source][1]*np.fromiter(param_map.values(), dtype=float)
                                        value_param = {"type":"time_series","data":dict(zip(keys,vals))}
                                    else:
                                        value_param = param_list[param_source][1]*value_["parsed_value"] 
                                    add_parameter_value(db_map,entity_class_target,param_list[param_source][0],"Base",entity_target_name,value_param)
                        
def add_power_transmission(db_map : DatabaseMapping, db_source : DatabaseMapping, config : dict) -> None:

    db_name = "power_transmission"
    print("ADDING POWER TRANSMISSION")
    for entity_class in config["sys"][db_name]["entities"]:
        entities = db_source.get_entity_items(entity_class_name = entity_class)
        
        for entity in entities:
            entity_name = entity["name"]
            entity_class_elements = (entity_class,) if len(entity["dimension_name_list"]) == 0 else entity["dimension_name_list"]
            entity_names          = (entity_name,) if len(entity["element_name_list"]) == 0 else entity["element_name_list"]

            if entity_names[0] in config["onshore_polygons"] and entity_names[-1] in config["onshore_polygons"] and config["user"][entity_class_elements[1]][entity_names[2]]["status"]:               
                for entity_class_target in config["sys"][db_name]["entities"][entity_class]:
                    if isinstance(config["sys"][db_name]["entities"][entity_class][entity_class_target],list):
                        for entity_target_building in config["sys"][db_name]["entities"][entity_class][entity_class_target]:
                            entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in entity_target_building])
                            add_entity(db_map,entity_class_target,entity_target_name)

                    # Default Parameters
                    if entity_class in config["sys"][db_name]["parameters"]["default"]:
                        if entity_class_target in config["sys"][db_name]["parameters"]["default"][entity_class]:
                            for param_items in config["sys"][db_name]["parameters"]["default"][entity_class][entity_class_target]:
                                entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in param_items[2]])
                                if param_items[0] == "investment_method": # Particular Case Screening Out
                                    if not db_source.get_parameter_value_item(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name="links_potentials",alternative_name="Base"):
                                        param_items[1] = "not_allowed"
                                add_parameter_value(db_map,entity_class_target,param_items[0],"Base",entity_target_name,param_items[1])
                    
                    # Fixed Parameters
                    if entity_class in config["sys"][db_name]["parameters"]["fixed"]:
                        if entity_class_target in config["sys"][db_name]["parameters"]["fixed"][entity_class]:
                            param_list = config["sys"][db_name]["parameters"]["fixed"][entity_class][entity_class_target]
                            for param_source in param_list:
                                entity_target_name = tuple(["_".join([entity_names[i-1] for i in k]) for k in param_list[param_source][2]])
                                value_ = db_source.get_parameter_value_item(entity_class_name=entity_class,entity_byname=entity_names,parameter_definition_name=param_source,alternative_name="Base")
                                if value_:
                                    value_param = param_list[param_source][1]*value_["parsed_value"] if value_["type"] != "map" else {"type":"map","index_type":"str","index_name":"period","data":{key:param_list[param_source][1]*item for key,item in dict(json.loads(value_["value"])["data"]).items()}}
                                    add_parameter_value(db_map,entity_class_target,param_list[param_source][0],"Base",entity_target_name,value_param)
                        
def add_policy_constraints(db_map : DatabaseMapping, config : dict):

    co2_budget = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":config["user"]["global_constraints"]["co2_budget"][year] for year in config["user"]["global_constraints"]["co2_budget"]}}
    # Atmosphere entity is created
    entity_name = "node"
    entity_byname = ("atmosphere",)
    add_entity(db_map,entity_name,entity_byname)
    add_parameter_value(db_map,"node","node_type","Base",("atmosphere",),"storage")
    add_parameter_value(db_map,"node","storage_capacity","Base",("atmosphere",),co2_budget)
        
def main():
    url_db_out = sys.argv[1]
    url_db_com = sys.argv[2]
    url_db_pow = sys.argv[3]
    url_db_vre = sys.argv[4]
    url_db_tra = sys.argv[5]
    url_db_hyd = sys.argv[6]
    url_db_dem = sys.argv[7]


    db_com = DatabaseMapping(url_db_com)
    db_pow = DatabaseMapping(url_db_pow)
    db_vre = DatabaseMapping(url_db_vre)
    db_tra = DatabaseMapping(url_db_tra)
    db_hyd = DatabaseMapping(url_db_hyd)
    db_dem = DatabaseMapping(url_db_dem)

    
    with open("ines_structure.json", 'r') as f:
        ines_spec = json.load(f)

    config = {"sys":yaml.safe_load(open("sysconfig.yaml", "rb")),"user":yaml.safe_load(open("userconfig.yaml", "rb"))}
    config["transformer"] = pd.read_excel("region_transformation.xlsx",sheet_name=None)
    polygons = define_polygons(config["user"],config["transformer"])
    config["onshore_polygons"]  = polygons["onshore"]
    config["offshore_polygons"] = polygons["offshore"]   

    with DatabaseMapping(url_db_out) as db_map:

        # Importing Map
        api.import_data(db_map,
                    entity_classes=ines_spec["entity_classes"],
                    parameter_value_lists=ines_spec["parameter_value_lists"],
                    parameter_definitions=ines_spec["parameter_definitions"],
                    )
        add_superclass_subclass(db_map,"unit_flow","node__to_unit")
        add_superclass_subclass(db_map,"unit_flow","unit__to_node")
        print("ines_map_added")
        db_map.commit_session("ines_map_added")

        # Base alternative
        add_alternative(db_map,"Base")

        # Timeline Structure
        add_timeline(db_map,config)
        print("timeline_added")
        db_map.commit_session("timeline_added")

        # Nodes involved
        add_nodes(db_map,db_com,config)
        print("nodes_added")
        db_map.commit_session("nodes_added")

        # Electricity Demand
        add_electricity_demand(db_map,db_dem,config)
        print("electricity_demand_added")
        db_map.commit_session("electricity_demand_added")

        # Power Sector Representation
        add_power_sector(db_map,db_pow,config)
        print("power_sector_added")
        db_map.commit_session("power_sector_added")

        # Power vre Representation
        add_vre_sector(db_map,db_vre,config)
        print("vre_added")
        db_map.commit_session("vre_added")

        # Hydro Systems
        add_hydro(db_map,db_hyd,config)
        print("hydro_systems_added")
        db_map.commit_session("hydro_systems_added")

        # Power Transmission Representation
        add_power_transmission(db_map,db_tra,config)
        print("power_transmission_added")
        db_map.commit_session("power_transmission_added")

        # Policy Constraints
        add_policy_constraints(db_map,config)
        print("policy_constraints")
        db_map.commit_session("policy_constraints")

if __name__ == "__main__":
    main()