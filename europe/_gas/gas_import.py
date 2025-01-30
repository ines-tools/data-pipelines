import spinedb_api as api
from spinedb_api import DatabaseMapping
import sys
import json
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
import yaml
import pandas as pd

# Spine Inputs
url_db_out = sys.argv[1]
existing_tech = pd.read_csv(sys.argv[2],index_col=0)
tech_wb = pd.read_excel(sys.argv[3],sheet_name=None,index_col=0)

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
    
def main():

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

if __name__ == "__main__":
    main()