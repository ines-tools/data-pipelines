import spinedb_api as api
from spinedb_api import DatabaseMapping
import sys
import pandas as pd
import numpy as np
import json
import yaml

def add_entity(db_map : DatabaseMapping, class_name : str, element_names : tuple) -> None:
    _, error = db_map.add_entity_item(entity_byname=element_names, entity_class_name=class_name)
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

def add_tech_parameters(target_db,industry,node,sheets):

    planning_years =  ["2030","2040","2050"]
    # lifetime
    entity_name = "technology"
    entity_byname = (industry,)
    df = sheets["ind_process_route_life"]
    value_life = df[(df.Industry==industry)]["life"].tolist()[0]
    add_parameter_value(target_db, entity_name, "lifetime", "Base", entity_byname, value_life)

    # capex
    entity_name = "technology__to_commodity"
    entity_byname = (industry,node)
    df = sheets["ind_process_routes_capex"]
    if industry in df.Industry.unique():
        array_p = (df[(df.Industry==industry)][planning_years].values.flatten().round(2)*8760.0).tolist()
        print(array_p)
        param_type = "map" if not all(array_p[0] == i for i in array_p) else "float"
        print(param_type,industry)
        if array_p[0] > 0.0:
            if param_type == "map":
                value_param =  dict(zip([f"y{year}" for year in planning_years],array_p))
                map_param = {"type": "map", "index_type": "str", "index_name": "period", "data": value_param}
                add_parameter_value(target_db, entity_name, "investment_cost", "Base", entity_byname, map_param)
            elif param_type == "float":
                add_parameter_value(target_db, entity_name, "investment_cost", "Base", entity_byname, array_p[0])

    # fom
    entity_name = "technology__to_commodity"
    entity_byname = (industry,node)
    df = sheets["ind_process_routes_fom"]
    if industry in df.Industry.unique():
        array_p = (df[(df.Industry==industry)][planning_years].values.flatten().round(2)*8760.0).tolist()
        param_type = "map" if not all(array_p[0] == i for i in array_p) else "float"
        if array_p[0] > 0.0:
            if param_type == "map":
                value_param =  dict(zip([f"y{year}" for year in planning_years],array_p))
                map_param = {"type": "map", "index_type": "str", "index_name": "period", "data": value_param}
                add_parameter_value(target_db, entity_name, "fixed_cost", "Base", entity_byname, map_param)
            elif param_type == "float":
                add_parameter_value(target_db, entity_name, "fixed_cost", "Base", entity_byname, array_p[0])


    # co2_captured
    df = sheets["ind_process_routes_co2_capture"]  
    if industry in df.Industry.unique(): 
        value_param = {f"y{year}":df[(df.Industry==industry)][year].tolist()[0] for year in planning_years}
        if value_param["y2030"] > 0.0:
            entity_name = "technology__to_commodity"
            entity_byname = (industry,"CO2")
            add_entity(target_db, entity_name, entity_byname)
            entity_name = "technology__to_commodity__to_commodity"
            entity_byname = (industry,node,"CO2")
            add_entity(target_db, entity_name, entity_byname)
            map_param = {"type": "map", "index_type": "str", "index_name": "period", "data": value_param}
            add_parameter_value(target_db, entity_name, "CO2_captured", "Base", entity_byname, np.array(list(value_param.values())).mean().round(3))

def conversion_sectors(target_db,sheet,com_sheet,nodes):

    for i in list(set(sheet.from_node.unique().tolist() + sheet.to_node.unique().tolist())):
        condition = False
        if i in sheet.from_node.unique().tolist():
            condition = True
        else:
            if i in nodes:
                condition = True
        if condition:
            entity_name = "commodity"
            entity_byname = (i,)
            add_entity(target_db, entity_name, entity_byname)

    for i in sheet.index:

        if sheet.at[i,"to_node"] in nodes:
            try:
                entity_name = "technology"
                entity_byname = (sheet.at[i,"Industry"],)
                add_entity(target_db, entity_name, entity_byname)
                add_parameter_value(target_db, "technology", "investment_method", "Base", entity_byname, ("not_allowed" if "reference" in sheet.at[i,"Industry"] else "no_limits"))
                entity_name = "technology__to_commodity"
                entity_byname = (sheet.at[i,"Industry"],sheet.at[i,"to_node"])
                add_entity(target_db, entity_name, entity_byname)
                add_parameter_value(target_db, entity_name, "capacity", "Base", entity_byname,1.0)
                add_tech_parameters(target_db,sheet.at[i,"Industry"],sheet.at[i,"to_node"],com_sheet)
            except:
                print("error conversion")
                pass

            value_dict = {f"y{year}":1/sheet.at[i,year] for year in ["2030","2040","2050"]}
            entity_name = "commodity__to_technology__to_commodity"
            entity_byname = (sheet.at[i,"from_node"],sheet.at[i,"Industry"],sheet.at[i,"to_node"])
            if value_dict["y2030"] > 0.0:
                add_entity(target_db, entity_name, entity_byname)
                map_param = {"type": "map", "index_type": "str", "index_name": "period", "data": value_dict}
                add_parameter_value(target_db, entity_name, "conversion_rate", "Base", entity_byname, np.array(list(value_dict.values())).mean().round(3))
                entity_name = "commodity__to_technology"
                entity_byname = (sheet.at[i,"from_node"],sheet.at[i,"Industry"])
                add_entity(target_db, entity_name, entity_byname)

def capacity_sectors(target_db,sheet,nodes):

    for i in sheet.index:

        if sheet.at[i,"to_node"] in nodes:
            print("node:",sheet.at[i,"to_node"])
            entity_name = "region"
            poly_column = "nuts3" if "nuts3" in sheet.columns else "country_code"
            poly_name = sheet.at[i,poly_column] 
            entity_byname = (poly_name,)
            try:
                add_entity(target_db, entity_name, entity_byname)
            except:
                pass

            entity_name = "technology__region"
            entity_byname = (sheet.at[i,"Industry"],poly_name)
            print(entity_name, entity_byname)
            add_entity(target_db, entity_name, entity_byname)
            add_parameter_value(target_db, entity_name, "units_existing", "Base", entity_byname, {"type": "map", "index_type": "str", "index_name": "period", "data": {"y2030":sheet.at[i,"2018"]*1000.0/8760.0 if "kt" in sheet.at[i,"unit"] else sheet.at[i,"2018"]/8760.0}})

def demand_sectors(target_db,sheet,nodes):

    for i in sheet.index:
        if sheet.at[i,"to_node"] in nodes:
            print("node:",sheet.at[i,"to_node"])
            entity_name = "region"
            poly_column = "nuts3" if "nuts3" in sheet.columns else "country_code"
            poly_name = sheet.at[i,poly_column] 
            entity_byname = (poly_name,)
            try:
                add_entity(target_db, entity_name, entity_byname)
            except:
                pass

            if sheet.at[i,"to_node"] not in []:
                entity_name = "commodity__region"
                entity_byname = (sheet.at[i,"to_node"],poly_name)
                add_entity(target_db, entity_name, entity_byname)
                multiplier = 1000.0/8760.0 if "kt" in sheet.at[i,"unit"] else 1/8760.0
                map_param = {"type": "map", "index_type": "str", "index_name": "year", "data": {"y2030":None,"y2040":None,"y2050":None}}
                map_param["data"]["y2030"] = -1*multiplier*float(sheet.at[i,"2030"])
                map_param["data"]["y2050"] = -1*multiplier*float(sheet.at[i,"2050"])
                map_param["data"]["y2040"] = (map_param["data"]["y2030"] + map_param["data"]["y2050"])/2
                if sheet.at[i,"to_node"] != "HC":
                    add_parameter_value(target_db, entity_name, "demand", "Base", entity_byname, -1*multiplier*float(sheet.at[i,"2030"])) # same demand for every year

def add_scenario(db_map : DatabaseMapping,name_scenario : str) -> None:
    _, error = db_map.add_scenario_item(name=name_scenario)
    if error is not None:
        raise RuntimeError(error)
    
def remove_items(db_map : DatabaseMapping):
    for entity_name in ["steam","heat"]:
        for entity_map in db_map.get_entity_items(entity_class_name="commodity",name=entity_name):
            item_id = entity_map["id"]
            db_map.remove_item("entity",item_id)

def main():

    # Spine Inputs
    dbs_dict = {
        "part1" : [sys.argv[1],"nuts3",
                   ["cement","chemical-chlorine","chemical-olefins","chemical-PE","chemical-PEA",
                    "fertiliser-ammonia-NH3","glass-container","glass-fibre","glass-float",
                    "HC","steel-primary","steel-secondary","MeOH"]],
        "part2" : [sys.argv[2],"nuts0",
                   ["alumina","aluminium-primary","aluminium-secondary","integrated-stealworks-steel"
                    "other-industrial-sectors","ceramics-and-other-non-metalic-minerals"
                    "other-chemicals","pharmaceuticals","food-beverages-tobacco",
                    "machinery-equipment","other-non-ferrous-metals","paper","electric-arc-steel",
                    "printing-and-media","pulp","leather-and-textile",
                    "transport-equipment","wood-and-wood-products"]],
        }
    ind_df = pd.read_excel(sys.argv[3],sheet_name=None)

    for part in dbs_dict:
        print(f"############### Filling the output DB ############### {part}")
        url_db_out = dbs_dict[part][0]
        resolution = dbs_dict[part][1]
        nodes = dbs_dict[part][2]
        with DatabaseMapping(url_db_out) as target_db:

            ## Empty the database
            target_db.purge_items('entity')
            target_db.purge_items('parameter_value')
            target_db.purge_items('alternative')
            target_db.purge_items('scenario')
            target_db.refresh_session()

            versionconfig = yaml.safe_load(open(sys.argv[-1], "rb"))
            add_scenario(target_db,f"v_{versionconfig["industry"]["version"]}")

            with open("industry_template_DB.json", 'r') as f:
                db_template = json.load(f)
            # Importing Map
            api.import_data(target_db,
                        entity_classes=db_template["entity_classes"],
                        parameter_definitions=db_template["parameter_definitions"],
                        )

            for alternative_name in ["Base"]:
                add_alternative(target_db,alternative_name)


            conversion_sectors(target_db,ind_df["ind_process_routes_sec"],ind_df,nodes)
            target_db.commit_session("conversion added")
            print("conversion added")
            capacity_sectors(target_db,ind_df[f"ind_production_2018_{resolution}"],nodes)
            target_db.commit_session("capacity added")
            print("capacity added")
            demand_sectors(target_db,ind_df[f"ind_production_30_50_{resolution}"],nodes)
            target_db.commit_session("demand added")
            print("demand added")
            remove_items(target_db)
            target_db.commit_session("removal")
        
if __name__ == "__main__":
    main()