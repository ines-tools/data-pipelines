import spinedb_api as api
from spinedb_api import DatabaseMapping
import sys
import pandas as pd

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

    # lifetime
    entity_name = "technology"
    entity_byname = (industry,)
    df = sheets["ind_process_route_life"]
    value_param = df[(df.Industry==industry)]["life"].tolist()[0]
    add_parameter_value(target_db, entity_name, "lifetime", "Base", entity_byname, value_param)

    # capex
    entity_name = "technology__to_commodity"
    entity_byname = (industry,node)
    df = sheets["ind_process_routes_capex"]
    value_param = {f"y{year}":df[(df.Industry==industry)][year].tolist()[0]*8760.0 for year in ["2030","2040","2050"]}
    if value_param["y2030"] > 0.0:
        map_param = {"type": "map", "index_type": "str", "index_name": "period", "data": value_param}
        add_parameter_value(target_db, entity_name, "investment_cost", "Base", entity_byname, map_param)

    # fom
    entity_name = "technology__to_commodity"
    entity_byname = (industry,node)
    df = sheets["ind_process_routes_fom"]
    value_param = {f"y{year}":df[(df.Industry==industry)][year].tolist()[0]*8760.0 for year in ["2030","2040","2050"]}
    if value_param["y2030"] > 0.0:
        map_param = {"type": "map", "index_type": "str", "index_name": "period", "data": value_param}
        add_parameter_value(target_db, entity_name, "fixed_cost", "Base", entity_byname, map_param)

    # co2_captured
    df = sheets["ind_process_routes_co2_capture"]
    value_param = {f"y{year}":df[(df.Industry==industry)][year].tolist()[0] for year in ["2030","2040","2050"]}
    if value_param["y2030"] > 0.0:
        entity_name = "technology__to_commodity"
        entity_byname = (industry,"CO2")
        add_entity(target_db, entity_name, entity_byname)
        entity_name = "technology__to_commodity__to_commodity"
        entity_byname = (industry,node,"CO2")
        add_entity(target_db, entity_name, entity_byname)
        map_param = {"type": "map", "index_type": "str", "index_name": "period", "data": value_param}
        add_parameter_value(target_db, entity_name, "CO2_captured", "Base", entity_byname, map_param)

def conversion_sectors(target_db,sheet,com_sheet):

    for i in list(set(sheet.from_node.unique().tolist() + sheet.to_node.unique().tolist())):
        entity_name = "commodity"
        entity_byname = (i,)
        add_entity(target_db, entity_name, entity_byname)

    for i in sheet.index:

        try:
            entity_name = "technology"
            entity_byname = (sheet.at[i,"Industry"],)
            add_entity(target_db, entity_name, entity_byname)
            entity_name = "technology__to_commodity"
            entity_byname = (sheet.at[i,"Industry"],sheet.at[i,"to_node"])
            add_entity(target_db, entity_name, entity_byname)
            add_parameter_value(target_db, entity_name, "capacity", "Base", entity_byname,1.0)
            add_tech_parameters(target_db,sheet.at[i,"Industry"],sheet.at[i,"to_node"],com_sheet)
        except:
            pass

        value_dict = {f"y{year}":sheet.at[i,year] for year in ["2030","2040","2050"]}
        entity_name = "commodity__to_technology__to_commodity"
        entity_byname = (sheet.at[i,"from_node"],sheet.at[i,"Industry"],sheet.at[i,"to_node"])
        if value_dict["y2030"] > 0.0:
            add_entity(target_db, entity_name, entity_byname)
            map_param = {"type": "map", "index_type": "str", "index_name": "period", "data": value_dict}
            add_parameter_value(target_db, entity_name, "conversion_rate", "Base", entity_byname, map_param)
            entity_name = "commodity__to_technology"
            entity_byname = (sheet.at[i,"from_node"],sheet.at[i,"Industry"])
            add_entity(target_db, entity_name, entity_byname)

def capacity_sectors(target_db,sheet):

    for i in sheet.index:

        entity_name = "region"
        entity_byname = (sheet.at[i,"nuts3"],)
        try:
            add_entity(target_db, entity_name, entity_byname)
        except:
            pass

        entity_name = "technology__region"
        entity_byname = (sheet.at[i,"Industry"],sheet.at[i,"nuts3"])
        add_entity(target_db, entity_name, entity_byname)
        add_parameter_value(target_db, entity_name, "units_existing", "Base", entity_byname, {"type": "map", "index_type": "str", "index_name": "period", "data": {"y2030":sheet.at[i,"2018"]*1000.0/8760.0 if sheet.at[i,"unit"] == "kt_yr" else sheet.at[i,"2018"]/8760.0}})

def demand_sectors(target_db,sheet):

    for i in sheet.index:

        entity_name = "region"
        entity_byname = (sheet.at[i,"nuts3"],)
        try:
            add_entity(target_db, entity_name, entity_byname)
        except:
            pass

        entity_name = "commodity__region"
        entity_byname = (sheet.at[i,"to_node"],sheet.at[i,"nuts3"])
        add_entity(target_db, entity_name, entity_byname)
        multiplier = 1000.0/8760.0 if sheet.at[i,"unit"] == "kt_yr" else 1/8760.0
        map_param = {"type": "map", "index_type": "str", "index_name": "year", "data": {"y2030":None,"y2040":None,"y2050":None}}
        map_param["data"]["y2030"] = multiplier*float(sheet.at[i,"2030"])
        map_param["data"]["y2050"] = multiplier*float(sheet.at[i,"2050"])
        map_param["data"]["y2040"] = (map_param["data"]["y2030"] + map_param["data"]["y2050"])/2
        add_parameter_value(target_db, entity_name, "demand", "Base", entity_byname, map_param)
               


def main():

    # Spine Inputs
    url_db_out = sys.argv[1]
    ind_df = pd.read_excel(sys.argv[2],sheet_name=None)

    print("############### Filling the output DB ###############")
    with DatabaseMapping(url_db_out) as target_db:

        ## Empty the database
        target_db.purge_items('entity')
        target_db.purge_items('parameter_value')
        target_db.purge_items('alternative')
        target_db.purge_items('scenario')
        target_db.refresh_session()

        for alternative_name in ["Base"]:
            add_alternative(target_db,alternative_name)

        conversion_sectors(target_db,ind_df["ind_process_routes_sec"],ind_df)
        target_db.commit_session("conversion added")
        print("conversion added")
        capacity_sectors(target_db,ind_df["ind_production_2018_nuts1"])
        target_db.commit_session("capacity added")
        print("capacity added")
        demand_sectors(target_db,ind_df["ind_production_30_50_nuts1"])
        target_db.commit_session("demand added")
        print("demand added")
        
if __name__ == "__main__":
    main()