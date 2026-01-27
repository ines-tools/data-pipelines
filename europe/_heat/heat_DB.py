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
    

def process_units(target_db, sheet):

    co2_content = {"CH4":0.2,"HC":0.25,"coal":0.37,"waste":0.13,"bio":0.35}

    for commodity in sheet.from_node.unique().tolist() + ["CO2"]:
        if pd.notna(commodity):
            entity_byname = (commodity,)
            add_entity(target_db, "commodity", entity_byname)

    nodes = {"heat":["nonres-DHW","nonres-space","res-DHW","res-space"],"cool":["res-cool","nonres-cool"]}
    for node_u in nodes:
        add_entity(target_db, "commodity", (node_u,))
        for node_l in nodes[node_u]:
            add_entity(target_db, "end-use", (node_l,))
            add_entity(target_db, "commodity__to_end-use", (node_u,node_l))

    for unit_name in sheet.index.unique():
        params ={"planning_years" : ["y"+str(i) for i in sheet.loc[unit_name,"year"].to_list()],
                 "elec_conv": round(sheet.loc[unit_name,"conversion_rate_elec_pu"].values.mean(),4),
                 "heat_conv": round(sheet.loc[unit_name,"conversion_rate_heat_pu"].values.mean(),4),
                 "co2_conv":  sheet.loc[unit_name,"CO2_captured_pu"].values.mean(),
                 "investment_cost": (sheet.loc[unit_name,"CAPEX_MEUR_MW"]*1e6).round(1).to_list(),
                 "fixed_cost": sheet.loc[unit_name,"FOM_EUR_MW_y"].to_list(),
                 "operational_cost": sheet.loc[unit_name,"VOM_EUR_MWh"].to_list(),
                 "lifetime": sheet.loc[unit_name,"lifetime_y"].to_list()[0]}
 
        if pd.notna(params["elec_conv"]):
            print(unit_name,"District Heating")
            to_node = "elec"
            to_node_2 = "DH"
        else:
            print(unit_name,"Individual Heating")
            to_node = sheet.loc[unit_name,"to_node"].tolist()[0]
            to_node_2 = None

        if to_node != "DH" and to_node_2 != "DH":
           
            entity_name = "technology"
            entity_byname = (unit_name,)
            add_entity(target_db, entity_name, entity_byname)
            add_parameter_value(target_db, entity_name, "lifetime", "Base", entity_byname, params["lifetime"])
        
            entity_name = "technology__to_commodity"
            entity_byname = (unit_name, to_node)
            add_entity(target_db, entity_name, entity_byname)
            for param_name in ["investment_cost", "fixed_cost","operational_cost"]:
                if sum(params[param_name]) > 0:
                    map_param = {"type": "map", "index_type": "str", "index_name": "period", "data": dict(zip(params["planning_years"],params[param_name]))}
                    add_parameter_value(target_db, entity_name, param_name, "Base", entity_byname, map_param)
            
            add_parameter_value(target_db, entity_name, "capacity", "Base", entity_byname, 1.0)
            from_node = sheet.loc[unit_name,"from_node"].tolist()[0]
            if pd.notna(from_node):
                entity_name = "commodity__to_technology"
                entity_byname = (from_node, unit_name)
                add_entity(target_db, entity_name, entity_byname)

                if to_node_2:
                    entity_name = "commodity__to_technology__to_commodity"
                    entity_byname = (from_node, unit_name, to_node)
                    add_entity(target_db, entity_name, entity_byname)
                    add_parameter_value(target_db, entity_name, "conversion_rate", "Base", entity_byname, params["elec_conv"])
                    entity_byname = (from_node, unit_name, to_node_2)
                    add_entity(target_db, entity_name, entity_byname)
                    add_parameter_value(target_db, entity_name, "conversion_rate", "Base", entity_byname, params["heat_conv"])
                else:
                    entity_name = "commodity__to_technology__to_commodity"
                    entity_byname = (from_node, unit_name, to_node)
                    add_entity(target_db, entity_name, entity_byname)
                    if pd.notna(params["heat_conv"]):
                        add_parameter_value(target_db, entity_name, "conversion_rate", "Base", entity_byname, params["heat_conv"])
                    else:
                        print("WARNING: NO STATIC CONVERSION RATES FOR",unit_name)
                    
                if "+CC" in unit_name:
                    entity_name = "technology__to_commodity"
                    entity_byname = (unit_name,"CO2")
                    add_entity(target_db, entity_name, entity_byname)
                    entity_name = "commodity__to_technology__to_commodity"
                    entity_byname = (from_node, unit_name, "CO2")
                    add_entity(target_db, entity_name, entity_byname)
                    #map_param = {"type": "map", "index_type": "str", "index_name": "year", "data": dict(zip(params["planning_years"],co2_content[from_node]*params["co2_conv"]))}
                    add_parameter_value(target_db, entity_name, "conversion_rate", "Base", entity_byname, round(co2_content[from_node]*params["co2_conv"],3))

def process_existing_units(target_db, sheet, existing_df):

    co2_content = {"CH4":0.2,"HC":0.25,"coal":0.37,"waste":0.13,"bio":0.35}

    for commodity in sheet.from_node.unique().tolist() + ["CO2"]:
        if pd.notna(commodity):
            entity_byname = (commodity,)
            try:
                add_entity(target_db, "commodity", entity_byname)
            except:
                pass

    for tech_name in sheet.index.unique():
        unit_name = tech_name + "-existing"
        params ={"elec_conv": round(sheet.loc[tech_name,"conversion_rate_elec_pu"],4),
                 "heat_conv": round(sheet.loc[tech_name,"conversion_rate_heat_pu"],4),
                 "co2_conv":  sheet.loc[tech_name,"CO2_captured_pu"],
                 "fixed_cost": sheet.loc[tech_name,"FOM_EUR_MW_y"],
                 "operational_cost": sheet.loc[tech_name,"VOM_EUR_MWh"]}
 
        if pd.notna(params["elec_conv"]):
            print(unit_name,"District Heating")
            to_node = "elec"
            to_node_2 = "DH"
        else:
            print(unit_name,"Individual Heating")
            to_node = sheet.loc[tech_name,"to_node"]
            to_node_2 = None

        if to_node != "DH" and to_node_2 != "DH" and existing_df[existing_df.technology == tech_name]["2025"].sum() > 0.0:
            entity_name = "technology"
            entity_byname = (unit_name,)
            add_entity(target_db, entity_name, entity_byname)

            entity_name = "technology__to_commodity"
            entity_byname = (unit_name, to_node)
            add_entity(target_db, entity_name, entity_byname)
            for param_name in ["fixed_cost","operational_cost"]:
                if params[param_name] > 0:
                    add_parameter_value(target_db, entity_name, param_name, "Base", entity_byname, params[param_name])
            
            add_parameter_value(target_db, entity_name, "capacity", "Base", entity_byname, 1.0)
            from_node = sheet.loc[tech_name,"from_node"]
            if pd.notna(from_node):
                entity_name = "commodity__to_technology"
                entity_byname = (from_node, unit_name)
                add_entity(target_db, entity_name, entity_byname)

                if to_node_2:
                    entity_name = "commodity__to_technology__to_commodity"
                    entity_byname = (from_node, unit_name, to_node)
                    add_entity(target_db, entity_name, entity_byname)
                    add_parameter_value(target_db, entity_name, "conversion_rate", "Base", entity_byname, params["elec_conv"])
                    entity_byname = (from_node, unit_name, to_node_2)
                    add_entity(target_db, entity_name, entity_byname)
                    add_parameter_value(target_db, entity_name, "conversion_rate", "Base", entity_byname, params["heat_conv"])
                else:
                    entity_name = "commodity__to_technology__to_commodity"
                    entity_byname = (from_node, unit_name, to_node)
                    add_entity(target_db, entity_name, entity_byname)
                    if pd.notna(params["heat_conv"]):
                        add_parameter_value(target_db, entity_name, "conversion_rate", "Base", entity_byname, params["heat_conv"])
                    else:
                        print("WARNING: NO STATIC CONVERSION RATES FOR",unit_name)
                    
                if "+CC" in unit_name:
                    entity_name = "technology__to_commodity"
                    entity_byname = (unit_name,"CO2")
                    add_entity(target_db, entity_name, entity_byname)
                    entity_name = "commodity__to_technology__to_commodity"
                    entity_byname = (from_node, unit_name, "CO2")
                    add_entity(target_db, entity_name, entity_byname)
                    add_parameter_value(target_db, entity_name, "conversion_rate", "Base", entity_byname, round(co2_content[from_node]*params["co2_conv"],3))

            for index, row in existing_df[existing_df.technology == tech_name].iterrows():
                
                map_param = {"type": "map", "index_type": "str", "index_name": "period", "data": dict(zip(["y2030","y2040","y2050"],[row.iloc[4],row.iloc[5],row.iloc[6]]))} 
                if sum([row.iloc[4],row.iloc[5],row.iloc[6]])>0.0:
                    region_name = row.iloc[0]
                    try:
                        add_entity(target_db,"region",(region_name,))
                    except:
                        pass    
                    try:
                        add_entity(target_db,"technology__region",(unit_name,region_name))
                    except:
                        pass
                    add_parameter_value(target_db,"technology__region","units_existing","Base",(unit_name,region_name),map_param)


def process_storages(target_db, sheet):

    for sto_name in sheet.index.unique():
        params ={"planning_years": ["y"+str(i) for i in sheet.loc[sto_name,"year"].to_list()],
                "investment_cost": (sheet.loc[sto_name,"CAPEX_energy_MEUR_GWh"]*1e3).round(1).to_list(),
                 "fixed_cost": (sheet.loc[sto_name,"FOM_energy_EUR_GWh_y"]/1e3).round(1).to_list(),
                 "hours_ratio": sheet.loc[sto_name,"energy_to_power_ratio_h"].to_list()[0],
                 "losses_day": sheet.loc[sto_name,"storage_losses_pu_day"].to_list()[0],
                 "lifetime": sheet.loc[sto_name,"lifetime_y"].to_list()[0],
                 "to_node": sheet.loc[sto_name,"to_node"].to_list()[0]}
        
        entity_name = "storage"
        entity_byname = (sto_name,)
        add_entity(target_db, entity_name, entity_byname)
        add_parameter_value(target_db, entity_name, "lifetime", "Base", entity_byname, params["lifetime"])
        add_parameter_value(target_db, entity_name, "hours_ratio", "Base", entity_byname, params["hours_ratio"])
        add_parameter_value(target_db, entity_name, "losses_day", "Base", entity_byname, params["losses_day"])

        for param_name in ["investment_cost", "fixed_cost",]:
            map_param = {"type": "map", "index_type": "str", "index_name": "period", "data": dict(zip(params["planning_years"],params[param_name]))}
            add_parameter_value(target_db, entity_name, param_name, "Base", entity_byname, map_param)

        entity_name = "storage_connection"
        to_node = params["to_node"]
        entity_byname = (sto_name,to_node)
        add_entity(target_db, entity_name, entity_byname)
        
def process_region_data(target_db,path_cop,path_demand,scenario_df):

    years = ["wy1995","wy2008","wy2009"]
    time_list = []
    for year in years:
        pd_range = pd.date_range(str(int(year[2:]))+"-01-01 00:00:00",str(int(year[2:]))+"-12-31 23:00:00",freq="h")
        time_list += [i.isoformat() for i in pd_range if not (i.month==2 and i.day==29)]

    map_tech = {"A2AHP-cooling":{"technology":"air-heatpump-cool","commodity":"heat","data":None},
                "A2WHP-DHW":{"technology":"air-heatpump","commodity":"heat","data":None},
                "A2WHP-radiators":{"technology":"air-heatpump","commodity":"heat","data":None},
                "G2WHP-DHW":{"technology":"ground-heatpump","commodity":"heat","data":None},
                "G2WHP-radiators":{"technology":"ground-heatpump","commodity":"heat","data":None}}

    for tech in map_tech:
        for cy in years:
            if isinstance(map_tech[tech]["data"],pd.DataFrame):
                map_tech[tech]["data"] = pd.concat([map_tech[tech]["data"],pd.read_csv(os.path.join(path_cop,f"COP_{tech}_{cy}.csv"),index_col=0).iloc[:8760,:]],axis=0,ignore_index=False)
            else:
                map_tech[tech]["data"] = pd.read_csv(os.path.join(path_cop,f"COP_{tech}_{cy}.csv"),index_col=0).iloc[:8760,:]

    demand_type = {"cooling_res":"res-cool","cooling_nonres":"nonres-cool","DHW_res":"res-DHW","DHW_nonres":"nonres-DHW","heating_res":"res-space","heating_nonres":"nonres-space"}
    map_demand = {}
    for dem in demand_type:
        for cy in years:
            if dem in map_demand.keys():
                map_demand[dem] = pd.concat([map_demand[dem],pd.read_csv(os.path.join(path_demand,f"{dem}_{cy}_normalised_MW_GWh.csv"),index_col=0).iloc[:8760,:]],axis=0,ignore_index=False)
            else:
                map_demand[dem] = pd.read_csv(os.path.join(path_demand,f"{dem}_{cy}_normalised_MW_GWh.csv"),index_col=0).iloc[:8760,:]
    
    for country in map_demand[dem].columns:

        for dem in demand_type:
            try:
                add_entity(target_db,"region",(country,))
            except:
                pass

            entity_name = "end-use__region"
            entity_byname = (demand_type[dem],country)
            add_entity(target_db, entity_name, entity_byname)
            value_dem = -1*map_demand[dem][country].values
            map_param = {"type": "map", "index_type": "str", "index_name": "t", "data":dict(zip(time_list,value_dem))}
            add_parameter_value(target_db, entity_name, "flow_profile", "Base", entity_byname, map_param)

            sector_i,type_i = dem.split("_")
            for scenario in scenario_df.scenario.unique():
                try:
                    add_alternative(target_db,scenario)
                except:
                    pass
                map_scale = {}
                for year in [2030,2040,2050]:
                    map_scale[f"y{year}"] = scenario_df[(scenario_df.scenario==scenario)&(scenario_df.scenario_year==year)&(scenario_df.building_category==type_i)&(scenario_df.demand==sector_i)][country].to_list()[0]
                map_param = {"type": "map", "index_type": "str", "index_name": "period", "data": map_scale}
                add_parameter_value(target_db, entity_name, "annual_scale", scenario, entity_byname, map_param)

        dem_space = scenario_df[((scenario_df.scenario_year==2030)|(scenario_df.scenario_year==2040)|(scenario_df.scenario_year==2050))&(scenario_df.demand=="heating")][country].values
        dem_dhw   = scenario_df[((scenario_df.scenario_year==2030)|(scenario_df.scenario_year==2040)|(scenario_df.scenario_year==2050))&(scenario_df.demand=="DHW")][country].values
        ratio_space = sum(dem_space[i]/(dem_space[i]+dem_dhw[i]) for i in range(len(dem_space)) if (dem_space[i]+dem_dhw[i]) > 0.0)/len(dem_space)
        ratio_DHW   = sum(dem_dhw[i]/(dem_space[i]+dem_dhw[i]) for i in range(len(dem_space)) if (dem_space[i]+dem_dhw[i]) > 0.0)/len(dem_space)
        print(ratio_DHW,ratio_space)
        for tech in ["A2AHP-cooling","A2WHP-radiators","G2WHP-radiators"]:
            entity_name = "commodity__to_technology__to_commodity__region"
            to_node = "cool" if tech == "A2AHP-cooling" else "heat"
            loop_entity = [""] if to_node == "cool" else ["","-existing"]
            for plus_item in loop_entity:
                entity_byname = ("elec",map_tech[tech]["technology"]+plus_item,to_node,country)
                add_entity(target_db, entity_name, entity_byname)
                value_cop = map_tech[tech]["data"][country].values if tech == "A2AHP-cooling" else map_tech[tech]["data"][country].values*ratio_space + map_tech[tech[:6]+"DHW"]["data"][country].values*ratio_DHW
                map_param = {"type": "map", "index_type": "str", "index_name": "t", "data":dict(zip(time_list,value_cop.round(4)))}
                add_parameter_value(target_db, entity_name, "conversion_rate", "Base", entity_byname, map_param)
        
def main():

    # Spine Inputs
    url_db_out = sys.argv[1]
    tech_info = pd.read_csv(sys.argv[2],index_col=0)
    stog_info = pd.read_csv(sys.argv[3],index_col=0)
    exis_info = pd.read_csv(sys.argv[4])
    scenarios = pd.read_csv(sys.argv[5])
    cop_timeseries  = sys.argv[6]
    demand_timeseries  = sys.argv[7]
    print("############### Filling the output DB ###############")
    with DatabaseMapping(url_db_out) as target_db:

        ## Empty the database
        target_db.purge_items('entity')
        target_db.purge_items('parameter_value')
        target_db.purge_items('alternative')
        target_db.purge_items('scenario')
        target_db.refresh_session()

        with open("heat_template_DB.json", 'r') as f:
            db_template = json.load(f)
        # Importing Map
        api.import_data(target_db,
                    entity_classes=db_template["entity_classes"],
                    parameter_definitions=db_template["parameter_definitions"],
                    )

        for alternative_name in ["Base"]:
            add_alternative(target_db,alternative_name)

        process_units(target_db,tech_info[tech_info.year != 2020])
        process_existing_units(target_db,tech_info[tech_info.year == 2020],exis_info)
        target_db.commit_session("units added")
        print("technologies_added")
        '''process_storages(target_db,stog_info)
        target_db.commit_session("storages added")
        print("storages_added")'''
        process_region_data(target_db,cop_timeseries,demand_timeseries,scenarios)
        target_db.commit_session("regions added")

if __name__ == "__main__":
    main()