import spinedb_api as api
from spinedb_api import DatabaseMapping
import sys
import json
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
import yaml
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
    
def process_single_parameter(sheet, config_file, target_db, entity_name, entity_byname, param_name,multiplier=1.0):

    source_param_name = config_file[entity_name][param_name]
    cell_value = float(sheet.loc[[i for i in sheet.index if source_param_name in i]].mean())
    if isinstance(cell_value, (float, int)) and pd.notna(cell_value):
        add_parameter_value(target_db, entity_name, param_name, "Base", entity_byname, round(multiplier*cell_value,3))

def process_map_parameter(sheet, config_file, target_db, entity_name, entity_byname, param_name, multiplier=1.0):
    map_param = {"type": "map", "index_type": "str", "index_name": "period", "data": {}}
    source_param_name = config_file[entity_name][param_name]
    for source_param in [i for i in sheet.index if source_param_name in i]:
        origin_name,param_alternative = source_param.split("_")
        cell_value = sheet.at[source_param]
        if isinstance(cell_value, (float, int)) and pd.notna(cell_value):
            map_param["data"][f"y{param_alternative}"] = round(cell_value * multiplier, 2)
    if len(map_param["data"]) > 1:
        add_parameter_value(target_db, entity_name, param_name, "Base", entity_byname, map_param)

def process_storage_data(sheet, config_file, target_db,commodities):
    for storage_name in sheet.index:
        connected_to = sheet.commodity.at[storage_name]
        if connected_to in commodities:
            entity_name = "storage"
            entity_byname = (storage_name,)
            add_entity(target_db, entity_name, entity_byname)
            process_single_parameter(sheet.T[storage_name], config_file, target_db, entity_name, entity_byname, "lifetime",)
            process_map_parameter(sheet.T[storage_name], config_file, target_db, entity_name, entity_byname, "investment_cost", multiplier=1e6)
            
            entity_name = "storage_connection"
            entity_byname = (storage_name, connected_to)
            add_entity(target_db, entity_name, entity_byname)
            process_single_parameter(sheet.T[storage_name], config_file, target_db, entity_name, entity_byname, "lifetime",)
            for param_name in ["investment_cost", "fixed_cost", "operational_cost", "efficiency_in", "efficiency_out"]:
                multiplier = 1e6 if param_name == "investment_cost" else 1
                process_map_parameter(sheet.T[storage_name], config_file, target_db, entity_name, entity_byname, param_name, multiplier)

def process_units(sheet, config_file, target_db, commodities, technologies_excluded):

    co2_content = {"CH4":0.2,"HC":0.25,"coal":0.37,"waste":0.13,"bio":0.35}
    
    for unit_name in sheet.index:

        to_node = sheet.to_node.at[unit_name]
        if unit_name not in technologies_excluded and to_node in commodities:
            entity_name = "technology"
            entity_byname = (unit_name,)
            add_entity(target_db, entity_name, entity_byname)
            process_single_parameter(sheet.T[unit_name], config_file, target_db, entity_name, entity_byname, "lifetime")

            try:
                add_entity(target_db, "commodity", (to_node,))
            except:
                pass
            entity_name = "technology__to_commodity"
            entity_byname = (unit_name, to_node)
            add_entity(target_db, entity_name, (unit_name, to_node))
            add_parameter_value(target_db,entity_name,"capacity","Base",entity_byname,1.0)
            for param_name in ["investment_cost", "fixed_cost", "operational_cost"]:
                multiplier = 1e6 if param_name == "investment_cost" else 1
                process_map_parameter(sheet.T[unit_name], config_file, target_db, entity_name, entity_byname, param_name, multiplier)

            from_node = sheet.from_node.at[unit_name]
            if isinstance(from_node,str):
                try:
                    add_entity(target_db, "commodity", (from_node,))
                except:
                    pass
                entity_name = "commodity__to_technology"
                entity_byname = (from_node, unit_name)
                add_entity(target_db, entity_name, entity_byname)
                entity_name = "commodity__to_technology__to_commodity"
                entity_byname = (from_node, unit_name, to_node)
                add_entity(target_db, entity_name, entity_byname)
                process_single_parameter(sheet.T[unit_name], config_file, target_db, entity_name, entity_byname,"conversion_rate")
                if "+CC" in unit_name:
                    try:
                        add_entity(target_db, "commodity", ("CO2",))
                    except:
                        pass
                    entity_name = "technology__to_commodity"
                    entity_byname = (unit_name,"CO2")
                    add_entity(target_db, entity_name, entity_byname)
                    entity_name = "commodity__to_technology__to_commodity"
                    entity_byname = (from_node, unit_name, "CO2")
                    add_entity(target_db, entity_name, entity_byname)
                    multiplier = co2_content[from_node]
                    process_single_parameter(sheet.T[unit_name], config_file, target_db, entity_name, entity_byname,"CO2_captured",multiplier)

            


def process_all_sectors(tech_wb, config_file, target_db, sector_commodity):

    technologies_excluded = [
        "wind-on-SP335-HH100", "wind-on-SP335-HH150", "wind-on-SP277-HH100", "wind-on-SP277-HH150",
        "wind-on-SP198-HH100", "wind-on-SP198-HH150", "solar-PV-no-tracking", "solar-PV-rooftop",
        "solar-PV-tracking", "wind-off-FB-SP316-HH155", "wind-off-FB-SP370-HH155", "wind-on-existing",
        "wind-off-existing","hydro-turbine"
    ]

    ################ COMMODITIES
    sheet = tech_wb["commodity"]
    
    for sector,commodities in sector_commodity.items():

        sheet = tech_wb[sector]
        ################ UNITS
        process_units(sheet, config_file, target_db, commodities, technologies_excluded)

        ################ STORAGES
        sheet = tech_wb["storage"]  
        process_storage_data(sheet, config_file, target_db, commodities)

def existing_data(target_db,existing_tech):

    for country in existing_tech.index:    
        try:
            add_entity(target_db,"region",(country,))
            add_parameter_value(target_db,"region","type","Base",(country,),"onshore")
            add_parameter_value(target_db,"region","GIS_level","Base",(country,),"PECD1")
        except:
            pass
        try:
            add_entity(target_db,"node",("elec",country))
        except:
            pass

        for tech in existing_tech.columns:
            if round(float(existing_tech.at[country,tech]),1 > 0.0) and tech != "hydro-turbine":
                add_entity(target_db,"technology__region",(tech,country))
                map_existing = {"type":"map","index_type":"str","index_name":"period","data":{"y2030":round(float(existing_tech.at[country,tech]),1)}}
                add_parameter_value(target_db,"technology__region","units_existing","Base",(tech,country),map_existing)    


def main():

    # Spine Inputs
    url_db_out = sys.argv[1]
    existing_tech = pd.read_csv(sys.argv[2],index_col=0)
    tech_wb = pd.read_excel(sys.argv[3],sheet_name=None,index_col=0)

    parameters_map = {"storage":{"investment_cost":"capex-energy",
                                 "lifetime":"lifetime"},
                      "storage_connection":{"investment_cost":"capex",
                                            "fixed_cost":"fom",
                                            "operational_cost":"vom",
                                            "efficiency_in":"charge-efficiency",
                                            "efficiency_out":"discharge-efficiency",
                                            "lifetime": "lifetime"},
                    "commodity":{"co2_content":"CO2_content",
                                 "commodity_price":"price"},
                    "technology":{"lifetime":"Lifetime"},
                    "technology__to_commodity":{"investment_cost":"capex",
                                                "fixed_cost":"fom",
                                                "operational_cost":"vom"},
                    "commodity__to_technology__to_commodity":{"conversion_rate":"conversion",
                                                              "CO2_captured":"CC"}}
                        

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

        sector_commodity = {"power":["elec"]}

        process_all_sectors(tech_wb, parameters_map, target_db,sector_commodity)
                        
        existing_data(target_db,existing_tech)
        target_db.commit_session("catalogue added")

if __name__ == "__main__":
    main()