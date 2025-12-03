import spinedb_api as api
from spinedb_api import DatabaseMapping
import sys
import json
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
import yaml
import pandas as pd
import os
import numpy as np

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

def add_network(target_db, network,cost):

    for commodity in cost:
        add_entity(target_db,"commodity",(commodity,))
        print(commodity)
        for idx, row in network.iterrows():
            try:
                add_entity(target_db,"region",(row.iloc[0],))
            except:
                pass

            try:
                add_entity(target_db,"region",(row.iloc[1],))
            except:
                pass

            print(row.iloc[0],commodity,row.iloc[1])
            entity_class  = "region__commodity__region"
            entity_byname = (row.iloc[0],commodity,row.iloc[1])
            add_entity(target_db,entity_class,entity_byname)
            sea_condition = "road" if row.iloc[3] else "maritime"
            add_parameter_value(target_db,entity_class,"operational_cost","Base",entity_byname,round(np.mean(cost[commodity][sea_condition])*row.iloc[2],1))

def main():

    # Spine Inputs
    url_db_out = sys.argv[1]
    network = pd.read_csv(sys.argv[2],index_col=0)
    costs   = yaml.safe_load(open(sys.argv[3], "rb"))

    print("############### Filling the output DB ###############")
    with DatabaseMapping(url_db_out) as target_db:

        ## Empty the database
        target_db.purge_items('entity')
        target_db.purge_items('parameter_value')
        target_db.purge_items('alternative')
        target_db.purge_items('scenario')
        target_db.refresh_session()

        with open("cargo_template_DB.json", 'r') as f:
            db_template = json.load(f)
        
        # Importing Map
        api.import_data(target_db,
                    entity_classes=db_template["entity_classes"],
                    parameter_definitions=db_template["parameter_definitions"],
                    )

        for alternative_name in ["Base"]:
            add_alternative(target_db,alternative_name)

        add_network(target_db,network,costs)
        target_db.commit_session("Parameters added")
        print("Network added")

if __name__ == "__main__":
    main()