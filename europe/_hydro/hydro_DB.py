import spinedb_api as api
from spinedb_api import DatabaseMapping
import sys
import json
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
import yaml
import pandas as pd
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

def add_scenario(db_map : DatabaseMapping,name_scenario : str) -> None:
    _, error = db_map.add_scenario_item(name=name_scenario)
    if error is not None:
        raise RuntimeError(error)

def process_parameters(target_db, sheet):

    add_entity(target_db, "commodity", ("elec",))
    entity_name = "technology"
    entity_byname = ("hydro-turbine",)
    add_entity(target_db, entity_name, entity_byname)
    entity_name = "storage"
    entity_byname = ("reservoir",)
    add_entity(target_db, entity_name, entity_byname)
    entity_name = "technology__to_commodity"
    entity_byname = ("hydro-turbine","elec")
    add_entity(target_db, entity_name, entity_byname)
    add_parameter_value(target_db, entity_name, "operational_cost", "Base", entity_byname, 3.03)
    # add_parameter_value(target_db, entity_name, "fixed_cost", "Base", entity_byname, 65120.0)
    entity_name = "storage__to_technology"
    entity_byname = ("reservoir","hydro-turbine")
    add_entity(target_db, entity_name, entity_byname)
    entity_name = "storage__to_technology__to_commodity"
    entity_byname = ("reservoir","hydro-turbine","elec")
    add_entity(target_db, entity_name, entity_byname)
    add_parameter_value(target_db, entity_name, "efficiency", "Base", entity_byname, 1.0)

    param_source = ["initial capacity (MWh)","maximum capacity (MWh)","minimum capacity  (MWh)","maximum discharge  (MWh)","minimum discharge  (MWh)","maximum ramping in 1 hour(MWh)","maximum ramping in 3 hours(MWh)"]
    param_target = ["initial_capacity","storage_capacity","minimum_capacity","capacity","min_operating_point","maximum_ramp","maximum_ramp_3"]
    params =dict(zip(param_target,param_source))   
    for country in sheet.index:
        
        add_entity(target_db, "region", (country,))

        entity_name = "technology__to_commodity__region"
        entity_byname = ("hydro-turbine","elec",country)
        add_entity(target_db, entity_name, entity_byname)
        for parameter in ["capacity","maximum_ramp","maximum_ramp_3"]:
            add_parameter_value(target_db, entity_name, parameter, "Base", entity_byname, float(sheet.at[country,params[parameter]]))
        
        entity_name = "storage__region"
        entity_byname = ("reservoir",country)
        add_entity(target_db, entity_name, entity_byname)
        for parameter in ["initial_capacity","minimum_capacity","storage_capacity"]:
            value_param = float(sheet.at[country,params[parameter]]) if parameter == "storage_capacity" else round(float(sheet.at[country,params[parameter]])/float(sheet.at[country,params["storage_capacity"]]),3)
            add_parameter_value(target_db, entity_name, parameter, "Base", entity_byname, value_param)
        
        entity_name = "storage__to_technology__region"
        entity_byname = ("reservoir","hydro-turbine",country)
        add_entity(target_db, entity_name, entity_byname)

def pump_hydro_storage(target_db, sheet):

    entity_name = "technology"
    entity_byname = ("PH-discharge",)
    add_entity(target_db, entity_name, entity_byname)
    entity_byname = ("PH-charge",)
    add_entity(target_db, entity_name, entity_byname)
    entity_name = "storage"
    entity_byname = ("PH-reservoir",)
    add_entity(target_db, entity_name, entity_byname)
    entity_name = "technology__to_commodity"
    entity_byname = ("PH-discharge","elec")
    add_entity(target_db, entity_name, entity_byname)
    entity_name = "storage__to_technology"
    entity_byname = ("PH-reservoir","PH-discharge")
    add_entity(target_db, entity_name, entity_byname)
    entity_name = "technology__to_storage"
    entity_byname = ("PH-charge","PH-reservoir")
    add_entity(target_db, entity_name, entity_byname)
    entity_name = "commodity__to_technology"
    entity_byname = ("elec","PH-charge")
    add_entity(target_db, entity_name, entity_byname)
    entity_name = "commodity__to_technology__to_storage"
    entity_byname = ("elec","PH-charge","PH-reservoir")
    add_entity(target_db, entity_name, entity_byname)
    entity_name = "storage__to_technology__to_commodity"
    entity_byname = ("PH-reservoir","PH-discharge","elec")
    add_entity(target_db, entity_name, entity_byname)

    param_source = ["initial capacity (MWh)","maximum capacity (MWh)","maximum discharge  (MWh)","maximal consumption (MWh)","efficiency factor"]
    param_target = ["initial_capacity","storage_capacity","capacity","capacity","efficiency"]
    params = dict(zip(param_source,param_target)) 
    for country in sheet.index:
        
        try:
            add_entity(target_db, "region", (country,))
        except:
            pass
        
        # storage params
        entity_name = "storage__region"
        entity_byname = ("PH-reservoir",country)
        add_entity(target_db, entity_name, entity_byname)
        for parameter in ["initial capacity (MWh)","maximum capacity (MWh)"]:
            value_param = float(sheet.at[country,parameter]) if parameter == "maximum capacity (MWh)" else round(float(sheet.at[country,parameter])/float(sheet.at[country,"maximum capacity (MWh)"]),3)
            add_parameter_value(target_db, entity_name, params[parameter], "Base", entity_byname, value_param)

        # discharging params
        entity_name = "technology__to_commodity__region"
        entity_byname = ("PH-discharge","elec",country)
        add_entity(target_db, entity_name, entity_byname)
        for parameter in ["maximum discharge  (MWh)"]:
            add_parameter_value(target_db, entity_name, params[parameter], "Base", entity_byname, float(sheet.at[country,parameter]))

        # charging params
        entity_name = "technology__to_storage__region"
        entity_byname = ("PH-charge","PH-reservoir",country)
        add_entity(target_db, entity_name, entity_byname)
        for parameter in ["maximal consumption (MWh)"]:
            add_parameter_value(target_db, entity_name, params[parameter], "Base", entity_byname, float(sheet.at[country,parameter]))

        entity_name = "commodity__to_technology__to_storage__region"
        entity_byname = ("elec","PH-charge","PH-reservoir",country)
        add_entity(target_db, entity_name, entity_byname)
        for parameter in ["efficiency factor"]:
            add_parameter_value(target_db, entity_name, params[parameter], "Base", entity_byname,float(sheet.at[country,parameter]))

def ror_parameters(target_db, path, wyears):

    entity_name = "technology"
    entity_byname = ("RoR",)
    add_entity(target_db, entity_name, entity_byname)
    entity_name = "technology__to_commodity"
    entity_byname = ("RoR","elec")
    add_entity(target_db, entity_name, entity_byname)

    for file in os.listdir(path):
        sheet = pd.read_csv(os.path.join(path,file),index_col=0).iloc[:,0]

        time_index = [pd.Timestamp(i).tz_convert(None).isoformat() for i in sheet.index if not (pd.Timestamp(i).month == 2 and pd.Timestamp(i).day == 29) and pd.Timestamp(i).year in wyears]
        time_pick  = [i for i in sheet.index if not (pd.Timestamp(i).year%4 == 0 and pd.Timestamp(i).month == 12 and pd.Timestamp(i).day == 31) and pd.Timestamp(i).year in wyears]

        country = file.split("_")[0]
        print(f"run-of-river country {country}")
        try:
            add_entity(target_db, "region", (country,))
        except:
            pass
        entity_name = "technology__to_commodity__region"
        entity_byname = ("RoR","elec",country)
        add_entity(target_db, entity_name, entity_byname)

        param_map = {"type":"map","index_type":"date_time","index_name":"t","data":dict(zip(time_index,(sheet.loc[time_pick].values/sheet.max()).round(3)))}
        add_parameter_value(target_db, entity_name, "profile", "Base", entity_byname, param_map)
        add_parameter_value(target_db, entity_name, "capacity", "Base", entity_byname, sheet.round(1).max())


def inflow_parameters(target_db, path, inflow_factor, wyears):

    for file in os.listdir(path):
        sheet = pd.read_csv(os.path.join(path,file),index_col=0).iloc[:,0]

        time_index = [pd.Timestamp(i).tz_convert(None).isoformat() for i in sheet.index if not (pd.Timestamp(i).month == 2 and pd.Timestamp(i).day == 29) and pd.Timestamp(i).year in wyears]
        time_pick  = [i for i in sheet.index if not (pd.Timestamp(i).year%4 == 0 and pd.Timestamp(i).month == 12 and pd.Timestamp(i).day == 31) and pd.Timestamp(i).year in wyears]
    
        country = file.split("_")[0]
        if country in inflow_factor.index:
            print(f"inflows country {country}")
            entity_name = "storage__region"
            entity_byname = ("reservoir",country)

            param_map = {"type":"map","index_type":"date_time","index_name":"t","data":dict(zip(time_index,(inflow_factor.at[country]*sheet.loc[time_pick].values).round(1)))}
            add_parameter_value(target_db, entity_name, "inflow", "Base", entity_byname, param_map)


def main():

    # Spine Inputs
    url_db_out = sys.argv[1]
    static_params = pd.read_excel(sys.argv[2],sheet_name=None,index_col=0)
    inflow_params = sys.argv[3]
    ror_params = sys.argv[4]
    userconfig = yaml.safe_load(open(sys.argv[5], "rb"))
    weather_years = [pd.Timestamp(userconfig["timeline"]["historical_alt"][i]["start"]).year for i in userconfig["timeline"]["historical_alt"]]
    print("############### Filling the output DB ###############")
    with DatabaseMapping(url_db_out) as target_db:

        ## Empty the database
        target_db.purge_items('entity')
        target_db.purge_items('parameter_value')
        target_db.purge_items('alternative')
        target_db.purge_items('scenario')
        target_db.refresh_session()

        versionconfig = yaml.safe_load(open(sys.argv[-1], "rb"))
        add_scenario(target_db,f"v_{versionconfig["hydro"]["version"]}")

        with open("hydro_template_DB.json", 'r') as f:
            db_template = json.load(f)
        # Importing Map
        api.import_data(target_db,
                    entity_classes=db_template["entity_classes"],
                    parameter_definitions=db_template["parameter_definitions"],
                    )
        
        for alternative_name in ["Base"]:
            add_alternative(target_db,alternative_name)

        process_parameters(target_db,static_params["WP2.3 hydro Reservoir"])
        target_db.commit_session("static_params_added")
        print("static_params_added")

        pump_hydro_storage(target_db,static_params["Pump"])
        target_db.commit_session("pump_static_params_added")
        print("pump_static_params_added")

        inflow_parameters(target_db,inflow_params,static_params["WP2.3 hydro Reservoir"]["Inflow adjustment factor"],weather_years)
        target_db.commit_session("inflow_params_added")
        print("inflow_params_added")

        ror_parameters(target_db,ror_params,weather_years)
        target_db.commit_session("ror_params_added")
        print("ror_params_added")

if __name__ == "__main__":
    main()