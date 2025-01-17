import spinedb_api as api
from spinedb_api import DatabaseMapping
import pandas as pd
import sys
from openpyxl import load_workbook
import numpy as np
import json 
import datetime

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
    electricity_demand = pd.read_csv(sys.argv[2],index_col=0)

    # Just for now in order to avoid a heavy DB
    index_filter = [str(i) for i in pd.date_range("1995-01-01 00:00:00","1995-12-31 23:00:00",freq="1h").tolist() + pd.date_range("2008-01-01 00:00:00","2008-12-31 23:00:00",freq="1h").tolist() + pd.date_range("2009-01-01 00:00:00","2009-12-31 23:00:00",freq="1h").tolist() if not (i.month == 2 and i.day == 29)]
    with DatabaseMapping(url_db_out) as db_map:

        ## Empty the database
        db_map.purge_items('entity')
        db_map.purge_items('parameter_value')
        db_map.purge_items('alternative')
        db_map.purge_items('scenario')
        db_map.refresh_session()

        add_alternative(db_map,"Base")
        
        add_entity(db_map,"commodity","elec")
        for country in electricity_demand.columns:    

            add_entity(db_map,"region",country)
            add_parameter_value(db_map,"region","type","Base",(country,),"onshore")
            add_parameter_value(db_map,"region","GIS_level","Base",(country,),"PECD1")
            add_relationship(db_map,"commodity__region",("elec",country))
            demand_v = (-1*electricity_demand.loc[index_filter,country].values).round(1)
            demand_i = [pd.Timestamp(i).isoformat() for i in electricity_demand.loc[index_filter,:].index.tolist()]
            value_de = {"type":"map","index_type":"date_time","index_name":"t","data":dict(zip(demand_i,demand_v))}
            add_parameter_value(db_map,"commodity__region","flow_profile","Base",("elec",country),value_de)
             
        print("Demand loaded")

        db_map.commit_session("PECD1_info")

if __name__ == "__main__":
    main()