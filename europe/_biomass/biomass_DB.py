import spinedb_api as api
from spinedb_api import DatabaseMapping
import pandas as pd
import sys
from openpyxl import load_workbook
import numpy as np
import json 

def add_entity(db_map : DatabaseMapping, class_name : str, name : str, ent_description = None) -> None:
    _, error = db_map.add_entity_item(name=name, entity_class_name=class_name,description=ent_description)
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
    
def main():

    url_db_out = sys.argv[1]
    bio_db = pd.read_csv(sys.argv[2]).fillna(0.0)

    with DatabaseMapping(url_db_out) as db_map:
        
        ## Empty the database
        db_map.purge_items('entity')
        db_map.purge_items('parameter_value')
        db_map.purge_items('alternative')
        db_map.purge_items('scenario')
        db_map.refresh_session()

        with open("biomass_template_DB.json", 'r') as f:
            db_template = json.load(f)
        # Importing Map
        api.import_data(db_map,
                    entity_classes=db_template["entity_classes"],
                    parameter_definitions=db_template["parameter_definitions"],
                    )
        
        add_alternative(db_map,"Base")
        add_entity(db_map,"commodity","bio")
        add_entity(db_map,"stock","biomass-stock")
        add_relationship(db_map,"stock__to_commodity",("biomass-stock","bio"))
        for scenario in bio_db["scenario"].unique():
            add_alternative(db_map,scenario+"_bio")

            for region_i in bio_db["nuts0"].unique():
                region = region_i if region_i != "EL" else "GR"

                try:
                    add_entity(db_map,"region",region)
                except:
                    pass
                
                try:
                    add_relationship(db_map,"stock__to_commodity__region",("biomass-stock","bio",region))
                except:
                    pass

                filter_db = bio_db[(bio_db.nuts0 == region_i)&(bio_db.scenario == scenario)]
                
                value_converted = filter_db["quantity"].sum()*277777.77
                add_parameter_value(db_map,"stock__to_commodity__region","annual_production",scenario+"_bio",("biomass-stock","bio",region),round(value_converted,1))
                transport_cost = 7.0 # moving biomass to final destination, average value
                value_converted = 1.32*np.dot(filter_db["quantity"].values,filter_db["roadsidecost"].values)/filter_db["quantity"].sum()/0.277778 + transport_cost if filter_db["quantity"].sum() > 0 else transport_cost
                add_parameter_value(db_map,"stock__to_commodity__region","operational_cost",scenario+"_bio",("biomass-stock","bio",region),round(value_converted,1))

        print("Biomass Data Added")

        db_map.commit_session("entities added")

if __name__ == "__main__":
    main()