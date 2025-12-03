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
    for alternative_name in ["wy1995","wy2008","wy2009"]:
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


def ouput_data():
    
    rp_days = pd.read_csv("results/representative_periods.csv")
    rp_days.index = range(1,rp_days.shape[0]+1)

    add_entity(sopt_db,"temporal_block",("all_rps",))
    add_entity(sopt_db,"model__default_temporal_block",("capacity_planning","all_rps"))

    total_rps = rp_days.shape[0]
    for year in ["2030","2040","2050"]:
        for alternative in rp_days.columns:
            weights = pd.read_csv(f"results/weights_{alternative}.csv").pivot(index='period', columns='rep_period', values='weight').fillna(0.0)
            alternative_name = f"{year}_{alternative}"
            add_alternative(sopt_db,alternative_name)
            for rp_day in weights.columns:
                try:
                    entity_name = (f"representative_period_{year}_{rp_day}",)
                    add_entity(sopt_db,"temporal_block",entity_name)
                    add_entity_group(sopt_db,"temporal_block","all_rps",f"representative_period_{year}_{rp_day}")
                    add_parameter_value(sopt_db,"temporal_block","resolution","Base",entity_name,{"type":"duration","data":"1h"})
                except:
                    pass
                add_parameter_value(sopt_db, "temporal_block", "representative_period_index", alternative_name, entity_name, int(["2030","2040","2050"].index(year)*total_rps+rp_day))
                add_parameter_value(sopt_db, "temporal_block", "weight", alternative_name, entity_name, weights[rp_day].sum())

                time_index = pd.date_range(start=f"{(year if year != '2040' else '2041')}-01-01 00:00:00",end=f"{(year if year != '2040' else '2041')}-12-31 23:00:00",freq="1h")

                year_start = pd.Timestamp(f"{(year if year != '2040' else '2041')}-01-01 00:00:00")
                block_start = (year_start + pd.Timedelta(f"{int(24*3600*(float(rp_days.at[rp_day,alternative])-1))}s")).isoformat()
                add_parameter_value(sopt_db,"temporal_block","block_start",alternative_name,entity_name,{"type":"date_time","data":block_start})
                block_end   = (year_start + pd.Timedelta(f"{int(24*3600*float(rp_days.at[rp_day,alternative]))}s")).isoformat()
                add_parameter_value(sopt_db,"temporal_block","block_end",alternative_name,entity_name,{"type":"date_time","data":block_end})

            
            map_rp = {"type":"map","index_type":"date_time","index_name":"t","data":[((time_index[24*(i-1)]).isoformat(),{"type":"array","data":[weights.at[i,j]*(year == year_i) for year_i in ["2030","2040","2050"] for j in weights.columns],"value_type": "float",}) for i in weights.index]}
            add_parameter_value(sopt_db,"temporal_block","representative_periods_mapping",alternative_name,(f"operations_y{year}",),map_rp)
    try:
        sopt_db.commit_session("Added representative periods")
    except DBAPIError as e:
        print("commit representative periods error")  


if __name__ == "__main__":
    if len(os.listdir("profiles")) == 0:
        input_data()
    else:
        print("input data generated")

    if len(os.listdir("results")) != 0:
        print("writting spineopt model")
        ouput_data()
    else:
        print("clustering not carried out")
