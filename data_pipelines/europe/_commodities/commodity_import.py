import spinedb_api as api
from spinedb_api import DatabaseMapping
import pandas as pd
import sys
from openpyxl import load_workbook
import numpy as np
import json 
import math
import yaml

def add_entity(db_map : DatabaseMapping, class_name : str, name : tuple, ent_description = None) -> None:
    _, error = db_map.add_entity_item(entity_byname=name, entity_class_name=class_name, description = ent_description)
    if error is not None:
        raise RuntimeError(error)

def add_relationship(db_map : DatabaseMapping,class_name : str,element_names : str) -> None:
    _, error = db_map.add_entity_item(element_name_list=element_names, entity_class_name=class_name)
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

def add_scenario(db_map : DatabaseMapping,name_scenario : str) -> None:
    _, error = db_map.add_scenario_item(name=name_scenario)
    if error is not None:
        raise RuntimeError(error)
    
def main():

    url_db_out = sys.argv[1]
    com_df = pd.read_csv(sys.argv[2],index_col=0)
    inflation = pd.read_csv(sys.argv[3],index_col=1)
    inflation.index = inflation.index.astype(int)

    with DatabaseMapping(url_db_out) as db_map:
        
        ## Empty the database
        db_map.purge_items('entity')
        db_map.purge_items('parameter_value')
        db_map.purge_items('alternative')
        db_map.purge_items('scenario')
        db_map.refresh_session()

        versionconfig = yaml.safe_load(open(sys.argv[-1], "rb"))
        add_scenario(db_map,f"v_{versionconfig["commodities"]["version"]}")

        with open("commodities_template_DB.json", 'r') as f:
            db_template = json.load(f)

        # Importing Map
        api.import_data(db_map,
                    entity_classes=db_template["entity_classes"],
                    parameter_definitions=db_template["parameter_definitions"],
                    )
    
        add_alternative(db_map,"Base")
        
        for carrier, row in com_df.iterrows():
            currency = row.iloc[5]

            add_entity(db_map,"commodity",(carrier,))
            if pd.notna(row.iloc[0]):
                inflation_factor = math.prod([1+float(value_)*1e-2 for value_ in inflation.loc[int(currency)+1:,"HICP"].tolist()]) if int(currency) < 2025 else 1.0
                map_price  = {"type":"map","index_type":"str","index_name":"period","data":{"y2030":round(inflation_factor*row.iloc[0],1),"y2040":round(inflation_factor*row.iloc[1],1),"y2050":round(inflation_factor*row.iloc[2],1)}}
                add_parameter_value(db_map,"commodity","commodity_price","Base",(carrier,),map_price)
            if pd.notna(row.iloc[3]):
                add_parameter_value(db_map,"commodity","co2_content","Base",(carrier,),row.iloc[3])

        print("Commodity Data Added")

        db_map.commit_session("DB added")


if __name__ == "__main__":
    main()