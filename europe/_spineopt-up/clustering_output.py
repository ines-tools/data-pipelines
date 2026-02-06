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

def add_or_update_parameter_value(db_map : DatabaseMapping, class_name : str,parameter : str,alternative : str,elements : tuple,value : any) -> None:
    db_value, value_type = api.to_database(value)
    db_map.add_or_update_parameter_value(entity_class_name=class_name,entity_byname=elements,parameter_definition_name=parameter,alternative_name=alternative,value=db_value,type=value_type)

def remove_previous_representatives():

    for entity_map in sopt_db.get_entity_items(entity_class_name = "temporal_block"):
        if "representative_period" in entity_map["name"] or "all_rps" in entity_map["name"]:
            item_id = entity_map["id"]
            sopt_db.remove_item("entity",item_id)
    try:
        sopt_db.commit_session("Removed representative periods")
    except:
        print("commit representative period removal error")  

def ouput_data():
    
    rp_days = pd.read_csv("results/representative_periods.csv")
    rp_days.index = range(1,rp_days.shape[0]+1)

    add_entity(sopt_db,"temporal_block",("all_rps",))
    add_entity(sopt_db,"model__default_temporal_block",("capacity_planning","all_rps"))

    total_rps = rp_days.shape[0]
    for year in ["2030","2040","2050"]:
        for alternative in rp_days.columns:
            weights = pd.read_csv(f"results/weights_{alternative}.csv").pivot(index='period', columns='rep_period', values='weight').fillna(0.0)
            alternative_name = f"y{year}_{alternative}"
            try:
                add_alternative(sopt_db,alternative_name)
            except:
                print(f"WARNING: alternative {alternative_name} already added")
                pass

            for rp_day in weights.columns:
                try:
                    entity_name = (f"representative_period_{year}_{rp_day}",)
                    add_entity(sopt_db,"temporal_block",entity_name)
                    add_entity_group(sopt_db,"temporal_block","all_rps",f"representative_period_{year}_{rp_day}")
                except:
                    print(f"WARNING: Error creating the temporal block {year}_{rp_day}")
                    pass
                add_or_update_parameter_value(sopt_db, "temporal_block","resolution","Base",entity_name,{"type":"duration","data":"1h"})
                add_or_update_parameter_value(sopt_db, "temporal_block", "representative_period_index", alternative_name, entity_name, int(["2030","2040","2050"].index(year)*total_rps+rp_day))
                add_or_update_parameter_value(sopt_db, "temporal_block", "weight", alternative_name, entity_name, weights[rp_day].sum())

                time_index = pd.date_range(start=f"{(year if year != '2040' else '2041')}-01-01 00:00:00",end=f"{(year if year != '2040' else '2041')}-12-31 23:00:00",freq="1h")

                year_start = pd.Timestamp(f"{(year if year != '2040' else '2041')}-01-01 00:00:00")
                block_start = (year_start + pd.Timedelta(f"{int(24*3600*(float(rp_days.at[rp_day,alternative])-1))}s")).isoformat()
                add_or_update_parameter_value(sopt_db,"temporal_block","block_start",alternative_name,entity_name,{"type":"date_time","data":block_start})
                block_end   = (year_start + pd.Timedelta(f"{int(24*3600*float(rp_days.at[rp_day,alternative]))}s")).isoformat()
                add_or_update_parameter_value(sopt_db,"temporal_block","block_end",alternative_name,entity_name,{"type":"date_time","data":block_end})

            map_rp = {"type":"map","index_type":"date_time","index_name":"t","data":[((time_index[24*(i-1)]).isoformat(),{"type":"array","data":[weights.at[i,j]*(year == year_i) for year_i in ["2030","2040","2050"] for j in weights.columns],"value_type": "float",}) for i in weights.index]}
            print(map_rp)
            add_or_update_parameter_value(sopt_db,"temporal_block","representative_periods_mapping",alternative_name,(f"operations_y{year}",),map_rp)
    try:
        sopt_db.commit_session("Added representative periods")
    except:
        print("commit representative periods error")  


if __name__ == "__main__":

    if len(os.listdir("results")) != 0:
        print("writting spineopt model")
        remove_previous_representatives()
        ouput_data()
    else:
        print("clustering not carried out")
