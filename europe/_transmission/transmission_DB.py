import spinedb_api as api
from spinedb_api import DatabaseMapping
import pandas as pd
import sys
from openpyxl import load_workbook
import numpy as np
import json 

def add_entity(db_map : DatabaseMapping, class_name : str, entity_byname : str, ent_description = None) -> None:
    _, error = db_map.add_entity_item(entity_byname=entity_byname, entity_class_name=class_name,description=ent_description)
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
    power_links = pd.read_csv(sys.argv[2],index_col=0)

    with DatabaseMapping(url_db_out) as db_map:

        ## Empty the database
        db_map.purge_items('entity')
        db_map.purge_items('parameter_value')
        db_map.purge_items('alternative')
        db_map.purge_items('scenario')
        db_map.refresh_session()

        with open("transmission_template_DB.json", 'r') as f:
            db_template = json.load(f)
        # Importing Map
        api.import_data(db_map,
                    entity_classes=db_template["entity_classes"],
                    parameter_definitions=db_template["parameter_definitions"],
                    )
        
        # Base alternative
        add_alternative(db_map,"Base")

        transmission_type = "HV"
        add_entity(db_map,"transmission",(transmission_type,))
        add_entity(db_map,"commodity",("elec",))
        

        # Electricity transmission
        for from_node in power_links["From_node"].unique():
            index_l = power_links[power_links.From_node == from_node].index
            for l in index_l:
                to_node = power_links.at[l,"To_node"]
                if power_links.at[l,"Capacity (MW)"] > 0 or power_links.at[l,"Potentials (MW)"] > 0:
                    
                    try:
                        add_entity(db_map,"region",(from_node,))
                        add_parameter_value(db_map,"region","type","Base",(from_node,),"onshore")
                        add_parameter_value(db_map,"region","GIS_level","Base",(from_node,),"PECD1")
                    except:
                        pass

                    try:
                        add_entity(db_map,"region",(to_node,))
                        add_parameter_value(db_map,"region","type","Base",(to_node,),"onshore")
                        add_parameter_value(db_map,"region","GIS_level","Base",(to_node,),"PECD1")
                    except:
                        pass

                    entity_byname = (from_node,transmission_type,"elec",to_node)
                    add_entity(db_map,"region__transmission__commodity__region",entity_byname)
                    add_parameter_value(db_map,"region__transmission__commodity__region","connection_type","Base",entity_byname,power_links.at[l,"Type"]) 

                    value_existing = round(float(power_links.at[l,"Capacity (MW)"]),1)
                    map_existing = {"type":"map","index_type":"str","index_name":"period","data":{"y2030":float(value_existing)}}
                    add_parameter_value(db_map,"region__transmission__commodity__region","links_existing","Base",entity_byname,map_existing)

                    value_potentials = round(float(power_links.at[l,"Potentials (MW)"]),1)
                    value_capex = round(float(power_links.at[l,"CAPEX (M€/MW/Km)"]*power_links.at[l,"Length (Km)"]*1e6),1) # estimate
                    if  value_potentials > 0.0 or value_capex > 0.0:
                        add_parameter_value(db_map,"region__transmission__commodity__region","links_potentials","Base",entity_byname,float(value_existing)+(float(value_potentials)))
                        add_parameter_value(db_map,"region__transmission__commodity__region","investment_cost","Base",entity_byname,float(value_capex)) 
                        add_parameter_value(db_map,"region__transmission__commodity__region","lifetime","Base",entity_byname,float(power_links.at[l,"Lifetime"]))

        print("Tranmission loaded")
        db_map.commit_session("transmission_added")

if __name__ == "__main__":
    main()