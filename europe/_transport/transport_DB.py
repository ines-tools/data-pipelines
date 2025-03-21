import spinedb_api as api
from spinedb_api import DatabaseMapping
import sys
import pandas as pd
import json
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

''' 
Reference charge drawn from network (MWh)	
Connected Battery space (kWh)	
Connected State of charge (kWh)	
Connected Battery capacity (kWh)	
Demand for next leg (MWh) (from network)	
Demand for next leg (MWh) (to vehicle)	
Connected vehicles (thousands)	
Charging Power from Network (MW)	
Charging Power to Vehicles (MW)	
Vehicle Discharge Power (MW)	
Discharge Power to Network (MW)	
Effective charging efficiency	
Effective discharge efficiency
'''
'''
gasoline consumption thousand litres	
diesel consumption thousand litres	
hydrogen consumption tonnes
'''
def add_vehicle_timeseries(target_db,data):

    index_time = []
    for alternative in df_index["year"].unique():
        pd_range = pd.date_range(str(int(year))+"-01-01 00:00:00",str(int(year))+"-12-31 23:00:00",freq="h")
        index_time += [i.isoformat() for i in pd_range if not (i.month==2 and i.day==29)]
        add_alternative(target_db,alternative)
    gasoline_factor = 9.5 # 9.5 kWh/litre
    diesel_factor = 10.0 # 10 kWh/litre
    h2_factor = 33.3 # kWh/kg
    index_time = [pd.Timestamp(i).isoformat() for i in data[list(data.keys())[0]].index]
    df_index = pd.DataFrame(data.keys(),columns=["region","vehicle","year","profile"])
    for region in df_index["region"].unique():
        add_entity(target_db,"region",(region,))
        for vehicle in df_index["vehicle"].unique():
            if vehicle not in ["van","truck","bus"]:
                for profile in df_index["profile"].unique():
                    data_hour = {}
                    if profile == "hourly":
                        condition_ = False
                        for year in df_index["year"].unique():
                            data_hour[year] = data[(region,vehicle,year,profile)]["Reference charge drawn from network (MWh)"].to_list()*3
                            if sum(data_hour[year]) > 0.0:
                                condition_ = True
                        if condition_:
                            try:
                                add_entity(target_db,"vehicle",(vehicle,))
                            except:
                                pass#print("entity created",vehicle)
                            entity_name   = "commodity__vehicle__region"
                            entity_byname = ("elec",vehicle,region)
                            add_entity(target_db,entity_name,entity_byname)
                            for year in df_index["year"].unique():
                                map_fixed_flow_profile = {"type":"map","index_type":"str","index_name":"t","data":dict(zip(index_time,data_hour[year]))}
                                add_parameter_value(target_db,entity_name,"flow_profile",alternative,entity_byname,map_fixed_flow_profile)
                        
                            try:
                                add_entity(target_db,"vehicle",(vehicle+"-DR",))
                            except:
                                pass#print("entity created",vehicle)
                            entity_name   = "commodity__vehicle__region"
                            entity_byname = ("elec",vehicle+"-DR",region)
                            add_entity(target_db,entity_name,entity_byname)
                            #flexible parameters
                        else:
                            print(vehicle,region,"elec not defined")
                    else:
                        condition_ = False
                        data_week = {}
                        for year in df_index["year"].unique():
                            value_array = (gasoline_factor*data[(region,vehicle,year,profile)]["gasoline consumption thousand litres"].values + diesel_factor*data[(region,vehicle,year,profile)]["diesel consumption thousand litres"].values)/np.concatenate((168*np.ones(52),24*np.ones(1)))
                            value_lists = [[value_array[ien]]*int(ven) for ien,ven in enumerate(np.concatenate((168*np.ones(52),24*np.ones(1))))]
                            data_week[year] = sum(value_lists, [])*3
                            if sum(data_week[year]) > 0.0:
                                condition_ = True
                        if condition_:
                            try:
                                add_entity(target_db,"vehicle",(vehicle,))
                            except:
                                pass
                            entity_name   = "commodity__vehicle__region"
                            entity_byname = ("HC",vehicle,region)
                            add_entity(target_db,entity_name,entity_byname)
                            map_fixed_flow_profile = {"type":"map","index_type":"str","index_name":"period",
                                "data":{f"y{str(year)}":{"type":"map","index_type":"str","index_name":"t","data":dict(zip(index_time,data_week[year]))} for year in df_index["year"].unique()}}
                            add_parameter_value(target_db,entity_name,"flow_profile","Base",entity_byname,map_fixed_flow_profile)
                        else:
                            print(vehicle,region,"HC not defined")

                        condition_ = False
                        data_week = {}
                        for year in df_index["year"].unique():
                            value_array = (h2_factor*data[(region,vehicle,year,profile)]["hydrogen consumption tonnes"].values)/np.concatenate((168*np.ones(52),24*np.ones(1)))
                            value_lists = [[value_array[ien]]*int(ven) for ien,ven in enumerate(np.concatenate((168*np.ones(52),24*np.ones(1))))]
                            data_week[year] = sum(value_lists, [])*3
                            if sum(data_week[year]) > 0.0:
                                condition_ = True
                        if condition_:
                            entity_byname = ("H2",vehicle,region)
                            add_entity(target_db,entity_name,entity_byname)
                            map_fixed_flow_profile = {"type":"map","index_type":"str","index_name":"period",
                                "data":{f"y{str(year)}":{"type":"map","index_type":"str","index_name":"t","data":dict(zip(index_time,data_week[year]))} for year in df_index["year"].unique()}}
                            add_parameter_value(target_db,entity_name,"flow_profile","Base",entity_byname,map_fixed_flow_profile)
                        else:
                            print(vehicle,region,"H2 not defined")

def main():

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

    with DatabaseMapping(url_db_out) as target_db:

        ## Empty the database
        target_db.purge_items('entity')
        target_db.purge_items('parameter_value')
        target_db.purge_items('alternative')
        target_db.purge_items('scenario')
        target_db.refresh_session()

        add_entity(target_db,"commodity",("elec",))
        add_entity(target_db,"commodity",("H2",))
        add_entity(target_db,"commodity",("HC",))

        for alternative_name in ["Base"]:
            add_alternative(target_db,alternative_name)

        add_vehicle_timeseries(target_db,data)
        print("parameters added for transport")        
        target_db.commit_session("parameters_added")  
    
if __name__ == "__main__":
    main()

# country, vehicle, year, data type