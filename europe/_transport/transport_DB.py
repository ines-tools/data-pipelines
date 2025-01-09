import spinedb_api as api
from spinedb_api import DatabaseMapping
import sys
import pandas as pd
import json
import os 
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
    
def main(url_db_out,data):
    breakpoint()
    return 1
    
if __name__ == "__main__":

    # Spine Inputs
    url_db_out = sys.argv[1]

    data = {}
    path = "../../../Transport"
    files = [os.path.join(path,filename) for filename in os.listdir(path)]

    print("Loading all the CSV files")
    for file in files:
        elements = file.split("\\")[-1].split("_")
        data_type = "hourly" if elements[3] == "profile" else "weekly"
        data[(elements[0],elements[1],elements[2],data_type)] = pd.read_csv(file,index_col=0)

    main(url_db_out,data)

# country, vehicle, year, data type