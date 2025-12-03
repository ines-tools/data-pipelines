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
            process_single_parameter(sheet.T[storage_name], config_file, target_db, entity_name, entity_byname, "efficiency_in",)
            process_single_parameter(sheet.T[storage_name], config_file, target_db, entity_name, entity_byname, "efficiency_out",)
            for param_name in ["investment_cost", "fixed_cost", "operational_cost"]:
                multiplier = 1e6 if param_name == "investment_cost" else 1
                process_map_parameter(sheet.T[storage_name], config_file, target_db, entity_name, entity_byname, param_name, multiplier)

def process_units(sheet, config_file, target_db, commodities):

    co2_content = {"CH4":0.2,"HC":0.25,"coal":0.37,"waste":0.13,"bio":0.35}
    
    for unit_name in sheet.index:

        to_node = sheet.to_node.at[unit_name]
        if to_node in commodities:
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

 
    for sector,commodities in sector_commodity.items():

        sheet = tech_wb[sector]
        ################ UNITS
        process_units(sheet, config_file, target_db, commodities)

        ################ STORAGES
        sheet = tech_wb["storage"]  
        process_storage_data(sheet, config_file, target_db, commodities)

def existing_data(target_db,existing_tech,existing_param):

    for country in existing_tech.index:    
        try:
            add_entity(target_db,"region",(country,))
        except:
            pass
        try:
            add_entity(target_db,"node",("elec",country))
        except:
            pass

        for techname in existing_tech.columns:
            if techname not in ["hydro-turbine","geothermal"]:
                tech = techname+"-existing"
                from_node = existing_param.at[techname,"from_node"]
                try:
                    add_entity(target_db,"technology",(tech,))
                    add_entity(target_db,"technology__to_commodity",(tech,"elec"))
                    add_parameter_value(target_db,"technology__to_commodity","capacity","Base",(tech,"elec"),1.0)  
                    add_parameter_value(target_db,"technology__to_commodity","operational_cost","Base",(tech,"elec"),float(existing_param.at[techname,"vom_cost"]))
                    add_parameter_value(target_db,"technology__to_commodity","fixed_cost","Base",(tech,"elec"),float(existing_param.at[techname,"fom_cost"]))
                    add_entity(target_db,"commodity__to_technology",(from_node,tech))
                    add_entity(target_db,"commodity__to_technology__to_commodity",(from_node,tech,"elec"))
                    add_parameter_value(target_db,"commodity__to_technology__to_commodity","conversion_rate","Base",(from_node,tech,"elec"),float(existing_param.at[techname,"efficiency"]))
                except:
                    pass

                if round(float(existing_tech.at[country,techname]),1 > 0.0):
                    add_entity(target_db,"technology__region",(tech,country))
                    map_max = {"type":"map","index_type":"str","index_name":"period","data":{f"y{str(year)}":round(float(existing_tech.at[country,techname])*existing_param.at[techname,f"expected_{str(year)}"],3) for year in [2030,2040,2050]}}
                    add_parameter_value(target_db,"technology__region","units_existing","Base",(tech,country),map_max) 


def main():

    # Spine Inputs
    url_db_out = sys.argv[1]
    existing_tech = pd.read_csv(sys.argv[2],index_col=0)
    tech_wb = pd.read_excel(sys.argv[3],sheet_name=None,index_col=0)
    existing_param = pd.read_csv(sys.argv[4],index_col=0)

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

        with open("power_template_DB.json", 'r') as f:
            db_template = json.load(f)
        # Importing Map
        api.import_data(target_db,
                    entity_classes=db_template["entity_classes"],
                    parameter_definitions=db_template["parameter_definitions"],
                    )

        for alternative_name in ["Base"]:
            add_alternative(target_db,alternative_name)

        sector_commodity = {"power":["elec"]}

        process_all_sectors(tech_wb, parameters_map, target_db,sector_commodity)
                        
        existing_data(target_db,existing_tech,existing_param)
        target_db.commit_session("catalogue added")

if __name__ == "__main__":
    main()