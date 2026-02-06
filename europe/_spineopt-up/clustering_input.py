import spinedb_api as api
from spinedb_api import DatabaseMapping
import pandas as pd
import sys
import numpy as np
import os
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy.exc import DBAPIError

sopt_db =  DatabaseMapping(sys.argv[1])

def add_entity(db_map : DatabaseMapping, class_name : str, name : tuple, ent_description = None) -> None:
    _, error = db_map.add_entity_item(entity_byname=name, entity_class_name=class_name, description = ent_description)
    if error is not None:
        raise RuntimeError(error)

def add_entity_group(db_map : DatabaseMapping, class_name : str, group : str, member : str) -> None:
    _, error = db_map.add_entity_group_item(group_name = group, member_name = member, entity_class_name=class_name)
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

def input_data():

    dict_df = {}
    for alternative_name in ["wy2009"]:
        columns_names = []
        array_ts = np.array([])
        for name_parameter in ["unit_availability_factor","demand","fix_unit_flow"]:
            for param_map in sopt_db.get_parameter_value_items(parameter_definition_name = name_parameter,alternative_name = alternative_name):
                if param_map["type"] == "time_series":
                    columns_names.append(param_map["entity_name"])
                    array_ts = np.hstack((array_ts, param_map["parsed_value"].values.reshape(-1, 1))) if  array_ts.size != 0 else param_map["parsed_value"].values.reshape(-1, 1)
        # Initialize the scaler
        scaler = MinMaxScaler()
        df_ts = pd.DataFrame(scaler.fit_transform(array_ts),index = range(1,array_ts.shape[0]+1), columns = columns_names).rename_axis("timestep").reset_index()
        dict_df[alternative_name] = pd.melt(df_ts, id_vars=df_ts.columns.tolist()[:1], value_vars=df_ts.columns.tolist()[1:], var_name='profile_name', value_name='value')[["profile_name","timestep","value"]]
        header_row = pd.DataFrame([dict_df[alternative_name].columns.tolist()], columns=dict_df[alternative_name].columns)
        new_row = pd.DataFrame([["","","MW/pu"]],columns=dict_df[alternative_name].columns)
        #pd.concat([new_row,header_row,dict_df[alternative_name]],ignore_index=True).to_csv(f"profiles/profiles_{alternative_name}.csv",index=False,header=False)
        pd.concat([header_row,dict_df[alternative_name]],ignore_index=True).to_csv(f"profiles/profiles_{alternative_name}.csv",index=False,header=False)



if __name__ == "__main__":
    input_data()

