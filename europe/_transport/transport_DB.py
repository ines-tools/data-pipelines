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

def time_index(year) -> list:
    pd_range = pd.date_range(str(int(year))+"-01-01 00:00:00",str(int(year))+"-12-31 23:00:00",freq="h")
    time_list  = [i.isoformat() for i in pd_range if not (i.month==2 and i.day==29)]
    return time_list


def week_to_hourly(commodity,target_db,veh_type,data,key_df,factor,df_index,region,vehicle,profile,cyears,scenario_fleet,fleet_column):
    
    condition_ = False
    try:
        value_array = sum(factor*data[(region,vehicle,year,profile)][key_df].values for year in df_index["year"].unique())/len(df_index["year"].unique())/np.concatenate((168*np.ones(52),24*np.ones(1)))
        value_lists = [[value_array[ien]]*int(ven) for ien,ven in enumerate(np.concatenate((168*np.ones(52),24*np.ones(1))))]
        output = np.array(sum(value_lists, [])).round(3)
        condition_ = True
    except:
        print(region,veh_type,key_df,"does not exist")

    if condition_:

        try:
            add_entity(target_db,"vehicle",(veh_type,))
        except:
            pass

        entity_name   = "commodity__vehicle__region"
        entity_byname = (commodity,veh_type,region)
        add_entity(target_db,entity_name,entity_byname)

        map_fixed_flow_profile = {"type":"map","index_type":"str","index_name":"t","data":profile_historical_wy(output,cyears)}
        add_parameter_value(target_db,entity_name,"flow_profile","Base",entity_byname,map_fixed_flow_profile)
        add_parameter_value(target_db,entity_name,"node_type","Base",entity_byname,"balance")

        for alternative_name in ["GA","DE"]:
            map_profile = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round(scenario_fleet.at[(region,vehicle,int(year),alternative_name),"Total Fleet"]*scenario_fleet.at[(region,vehicle,int(year),alternative_name),fleet_column]/1e3,1) for year in df_index["year"].unique()}}
            add_parameter_value(target_db,entity_name,"scale_demand",alternative_name,entity_byname,map_profile)
    
def profile_historical_wy(array,cyears):
    
    year_day_index = {"1995":6,"2008":1,"2009":3}
    index_time = []
    value_time = []
    for alternative in cyears:
        weeks_52_array = np.concatenate([array[year_day_index[alternative]*24:52*7*24],array[:year_day_index[alternative]*24]])
        value_time += np.concatenate([weeks_52_array,weeks_52_array[-7*24:-6*24]]).tolist()
        index_time += time_index(alternative)

    data = dict(zip(index_time,value_time))
    return data

def add_vehicle_timeseries(target_db,data,scenario_fleet,flex_range):

    cyears = ["1995","2008","2009"]
    gasoline_factor = 9.5 # kWh/litre
    diesel_factor = 10.0 # kWh/litre
    h2_factor = 33.3 # kWh/kg
    CNG_factor = 13.1 # kWh/kg
    LNG_factor = 13.9 # kWh/kg
    LPG_factor = 7.08 # kWh/kg

    df_index = pd.DataFrame(data.keys(),columns=["region","vehicle","year","profile"])
    for region in df_index["region"].unique():
        add_entity(target_db,"region",(region,))
        for vehicle in df_index["vehicle"].unique():
            print("Doing",region,vehicle)
            for profile in df_index["profile"].unique():
                data_hour = {}
                if profile == "hourly":
                    condition_ = True
                    data_fixed_charging    = (sum(data[(region,vehicle,year,profile)]["Reference charge drawn from network (kWh)"].values for year in df_index["year"].unique())/len(df_index["year"].unique())).round(3)
                    data_flex_charging     = {year: data[(region,vehicle,year,profile)]["Charging Power to Vehicles (kW)"].max()  for year in df_index["year"].unique()}
                    data_power_av          = (sum(data[(region,vehicle,year,profile)]["Charging Power to Vehicles (kW)"].values/data[(region,vehicle,year,profile)]["Charging Power to Vehicles (kW)"].max() for year in df_index["year"].unique())/len(df_index["year"].unique())).round(3)
                    data_flex_cap          = {year: data[(region,vehicle,year,profile)]["Connected Battery capacity (kWh)"].max()  for year in df_index["year"].unique()}
                    data_flex_demand       = (sum(data[(region,vehicle,year,profile)]["Demand for next leg (kWh) (to vehicle)"].values for year in df_index["year"].unique())/len(df_index["year"].unique())).round(3)
                    data_efficiency_in     = {"y"+year: round(data[(region,vehicle,year,profile)]["Effective charging efficiency"].values.mean(),3) for year in df_index["year"].unique()}
                    data_efficiency_out    = {"y"+year: round(data[(region,vehicle,year,profile)]["Effective discharge efficiency"].values.mean(),3) for year in df_index["year"].unique()}
                    data_connected         = (sum(data[(region,vehicle,year,profile)]["Connected vehicles (%)"].values for year in df_index["year"].unique())/len(df_index["year"].unique())).round(3)
                    
                    if condition_:
                        try:
                            add_entity(target_db,"vehicle",(vehicle,))
                        except:
                            pass#print("entity created",vehicle)
                        entity_name   = "commodity__vehicle__region"
                        entity_byname = ("elec",vehicle,region)
                        add_entity(target_db,entity_name,entity_byname)
                        map_profile = {"type":"map","index_type":"str","index_name":"t","data":profile_historical_wy(data_fixed_charging,cyears)}
                        add_parameter_value(target_db,entity_name,"flow_profile","Base",entity_byname,map_profile)
                        add_parameter_value(target_db,entity_name,"node_type","Base",entity_byname,"balance")

                        for alternative_name in ["GA","DE"]:
                            for flex_scenario in ["0","10","20"]:
                                map_profile = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round((1.0-float(flex_scenario)/1e2)*scenario_fleet.at[(region,vehicle,int(year),alternative_name),"Total Fleet"]*scenario_fleet.at[(region,vehicle,int(year),alternative_name),"electricity proportion"]/1e3,1) for year in df_index["year"].unique()}}
                                add_parameter_value(target_db,entity_name,"scale_demand",alternative_name+f"_flex{flex_scenario}",entity_byname,map_profile)    
                        try:
                            add_entity(target_db,"vehicle",(vehicle+"-DR",))
                        except:
                            pass#print("entity created",vehicle)

                        entity_name   = "commodity__vehicle__region"
                        entity_byname = ("elec",vehicle+"-DR",region)
                        add_entity(target_db,entity_name,entity_byname)
                        add_parameter_value(target_db,entity_name,"node_type","Base",entity_byname,"storage")

                        for flex_scenario in ["0","10","20"]:
                            for alternative_name in ["GA","DE"]:
                                map_profile = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round((float(flex_scenario)/1e2)*data_flex_charging[year]*scenario_fleet.at[(region,vehicle,int(year),alternative_name),"Total Fleet"]*scenario_fleet.at[(region,vehicle,int(year),alternative_name),"electricity proportion"]/1e3,1) for year in df_index["year"].unique()}}
                                add_parameter_value(target_db,entity_name,"power_capacity",alternative_name+f"_flex{flex_scenario}",entity_byname,map_profile)
                                map_profile = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round((float(flex_scenario)/1e2)*data_flex_cap[year]*scenario_fleet.at[(region,vehicle,int(year),alternative_name),"Total Fleet"]*scenario_fleet.at[(region,vehicle,int(year),alternative_name),"electricity proportion"]/1e3,1) for year in df_index["year"].unique()}}
                                add_parameter_value(target_db,entity_name,"energy_max",alternative_name+f"_flex{flex_scenario}",entity_byname,map_profile)
                                map_profile = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round((float(flex_scenario)/1e2)*scenario_fleet.at[(region,vehicle,int(year),alternative_name),"Total Fleet"]*scenario_fleet.at[(region,vehicle,int(year),alternative_name),"electricity proportion"]/1e3,1) for year in df_index["year"].unique()}}
                                add_parameter_value(target_db,entity_name,"scale_demand",alternative_name+f"_flex{flex_scenario}",entity_byname,map_profile)
                        
                        # historical data
                        map_profile = {"type":"map","index_type":"str","index_name":"t","data":profile_historical_wy(data_flex_demand,cyears)}
                        add_parameter_value(target_db,entity_name,"flow_profile","Base",entity_byname,map_profile)
                        map_profile = {"type":"map","index_type":"str","index_name":"t","data":profile_historical_wy(data_connected,cyears)}
                        add_parameter_value(target_db,entity_name,"connected_vehicles","Base",entity_byname,map_profile)
                        map_profile = {"type":"map","index_type":"str","index_name":"t","data":profile_historical_wy(data_power_av,cyears)}
                        add_parameter_value(target_db,entity_name,"power_available","Base",entity_byname,map_profile)
                        add_parameter_value(target_db,entity_name,"efficiency_in","Base",entity_byname,{"type":"map","index_type":"str","index_name":"period","data":data_efficiency_in})
                        add_parameter_value(target_db,entity_name,"efficiency_out","Base",entity_byname,{"type":"map","index_type":"str","index_name":"period","data":data_efficiency_out})
                    else:
                        print(vehicle,region,"elec not defined")
                else:
                    condition_ = True
                    
                    week_to_hourly("HC",target_db,vehicle+"-diesel",data,"gasoline consumption litres",gasoline_factor,df_index,region,vehicle,profile,cyears,scenario_fleet,"diesel proportion")
                    week_to_hourly("HC",target_db,vehicle+"-gasoline",data,"diesel consumption litres",diesel_factor,df_index,region,vehicle,profile,cyears,scenario_fleet,"gasoline proportion")
                    week_to_hourly("H2",target_db,vehicle+"-H2",data,"hydrogen consumption kg",h2_factor,df_index,region,vehicle,profile,cyears,scenario_fleet,"hydrogen proportion")
                    week_to_hourly("CH4",target_db,vehicle+"-CNG",data,"CNG consumption kg",CNG_factor,df_index,region,vehicle,profile,cyears,scenario_fleet,"CNG proportion")
                    week_to_hourly("CH4",target_db,vehicle+"-LNG",data,"LNG consumption kg",LNG_factor,df_index,region,vehicle,profile,cyears,scenario_fleet,"LNG proportion")
                    week_to_hourly("HC",target_db,vehicle+"-LPG",data,"LPG consumption litres",LPG_factor,df_index,region,vehicle,profile,cyears,scenario_fleet,"LPG proportion")

def add_nonroad_timeseries(target_db,data):

    cyears = ["1995","2008","2009"]
    factor = 277777.78

    df_index = pd.DataFrame(data.keys(),columns=["region","year"])
    for region in df_index["region"].unique():
        try:
            add_entity(target_db,"region",(region,))
        except:
            pass

        veh_headers = ["domestic-aviation_thermal_PJ","domestic-navigation_thermal_PJ","international-aviation_thermal_PJ","international-maritime-bunkers_thermal_PJ","rail_non_thermal_PJ","rail_thermal_PJ"]
        veh_types = ["aviation","maritime","int-aviation","int-maritime","rail","thermal-rail"]
        veh_fuels = ["HC","HC","HC","HC","elec","HC"]
        veh_map = dict(zip(veh_headers,veh_types))
        fuel_map = dict(zip(veh_types,veh_fuels))
        
        for key_df in veh_map.keys():
            veh_type = veh_map[key_df]
            condition_ = False
            try:
                output = {}
                annual_scale = {}
                for year in df_index["year"].unique():
                    value_df    = factor*data[(region,year)][key_df].values
                    value_array = value_df/(168*np.ones(53))
                    value_lists = [[value_array[ien]]*int(ven) for ien,ven in enumerate(np.concatenate((168*np.ones(52),24*np.ones(1))))]
                    output_array = np.array(sum(value_lists, [])).round(3)
                    output[year] = output_array/output_array.sum()*1000 if output_array.sum() > 0.0 else output_array
                    annual_scale[year] = output_array.sum()/1000
                if all(output[year].sum() > 0.0 for year in df_index["year"].unique()):
                    condition_ = True
            except:
                print(region,veh_type,key_df,"does not exist")

            if condition_:
                print(region,veh_type)
                try:
                    add_entity(target_db,"vehicle",(veh_type,))
                except:
                    pass

                entity_name   = "commodity__vehicle__region"
                entity_byname = (fuel_map[veh_type],veh_type,region)
                add_entity(target_db,entity_name,entity_byname)

                for year in df_index["year"].unique():
                    if output[year].sum() > 0.0:
                        year_selected = year
                map_fixed_flow_profile = {"type":"map","index_type":"str","index_name":"t","data":profile_historical_wy(output[year_selected].round(3),cyears)}
                add_parameter_value(target_db,entity_name,"flow_profile","Base",entity_byname,map_fixed_flow_profile)
                add_parameter_value(target_db,entity_name,"node_type","Base",entity_byname,"balance")

                map_profile = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round(annual_scale[year],1) for year in df_index["year"].unique()}}
                add_parameter_value(target_db,entity_name,"scale_demand","Base",entity_byname,map_profile)

def main():

    # Spine Inputs
    url_db_out = sys.argv[1]
    print(" "*10)
    print("road transport")
    print("-"*20)
    data = {}
    path = sys.argv[2]
    files = [os.path.join(path,filename) for filename in os.listdir(path) if "profile" in filename or "weekly" in filename]

    print("Loading all the CSV files")
    for file in files:
        elements = file.split("\\")[-1].split("_")
        data_type = "hourly" if "profile" in elements[3] else "weekly"
        data[(elements[0],elements[1],elements[2],data_type)] = pd.read_csv(file,index_col=0)

    scenario_fleet = pd.read_csv(sys.argv[4],index_col=[0,1,2,3])

    with DatabaseMapping(url_db_out) as target_db:

        ## Empty the database
        target_db.purge_items('entity')
        target_db.purge_items('parameter_value')
        target_db.purge_items('alternative')
        target_db.purge_items('scenario')
        target_db.refresh_session()

        with open("transport_template_DB.json", 'r') as f:
            db_template = json.load(f)
        # Importing Map
        api.import_data(target_db,
                    entity_classes=db_template["entity_classes"],
                    parameter_definitions=db_template["parameter_definitions"],
                    )

        add_entity(target_db,"commodity",("elec",))
        add_entity(target_db,"commodity",("H2",))
        add_entity(target_db,"commodity",("HC",))
        add_entity(target_db,"commodity",("CH4",))

        flex_range = ["0","10","20"]
        add_alternative(target_db,"Base")
        for alternative_name in ["GA","DE"]:
            add_alternative(target_db,alternative_name)
            for flex_scenario in flex_range:
                add_alternative(target_db,alternative_name+f"_flex{flex_scenario}")

        add_vehicle_timeseries(target_db,data,scenario_fleet,flex_range)
        print("parameters added for transport")        
        target_db.commit_session("parameters_added")  
    
    print("-"*20)
    print("non-road transport")
    print("-"*20)

    data = {}
    path = sys.argv[3]
    files = [os.path.join(path,filename) for filename in os.listdir(path)]

    print("Loading all the CSV files")
    for file in files:
        elements = file.split("\\")[-1].split(".")[0].split("_")
        data[(elements[0],elements[1])] = pd.read_csv(file,index_col=0)

    with DatabaseMapping(url_db_out) as target_db:

        add_nonroad_timeseries(target_db,data)
        print("parameters added for nonroad transport")        
        target_db.commit_session("parameters_added")

if __name__ == "__main__":
    main()

# country, vehicle, year, data type