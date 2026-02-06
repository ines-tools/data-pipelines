import spinedb_api as api
from spinedb_api import DatabaseMapping
import sys
from sqlalchemy.exc import DBAPIError
import yaml
import pandas as pd
import math 
import numpy as np
import json 

# Spine Inputs
url_db_out = sys.argv[1]
sheets = pd.read_excel(sys.argv[2],sheet_name = None)
inflation = pd.read_csv(sys.argv[3],index_col=1)

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

def tech_conversion(target_db,sheet):  
    for tech in sheet.technology.unique():
        add_entity(target_db,"technology",(tech,))
    for com in list(set(sheet["to"].unique().tolist() + sheet["from"].unique().tolist())):
        add_entity(target_db,"commodity",(com,))

    for index, row in sheet.iterrows():
        tech = row.iloc[0]
        from_node = row.iloc[1]
        to_node = row.iloc[2]
        try:
            add_entity(target_db, "commodity__to_technology", (from_node,tech))
        except:
            pass
        try:
            add_entity(target_db, "technology__to_commodity", (tech,to_node))
        except:
            pass
        add_entity(target_db, "commodity__to_technology__to_commodity", (from_node,tech,to_node))
        if pd.notna(row.iloc[3]):
            # map_conv = {"type":"map","index_type":"str","index_name":"period","data":{"y2030":row.iloc[3],"y2040":row.iloc[4],"y2050":row.iloc[5]}}
            add_parameter_value(target_db,"commodity__to_technology__to_commodity","conversion_rate","Base",(from_node,tech,to_node),np.array([row.iloc[3],row.iloc[4],row.iloc[5]]).mean().round(3))
    try:
        target_db.commit_session("Added tech conversion")
    except DBAPIError as e:
        print("commit tech conversion error")

def tech_production(target_db,sheet):
    
    for index, row in sheet.iterrows():
        tech = row.iloc[0]
        to_node = row.iloc[1]
        currency = row.iloc[12]
        add_parameter_value(target_db,"technology__to_commodity","capacity","Base",(tech,to_node),1.0)
        if pd.notna(currency):
            inflation_factor = math.prod([1+value_*1e-2 for value_ in inflation.loc[currency+1:,"HICP"].tolist()])
            print(tech, currency, inflation_factor)
        if pd.notna(row.iloc[2]):
            map_inv  = {"type":"map","index_type":"str","index_name":"period","data":{"y2030":1e6*inflation_factor*row.iloc[2],"y2040":1e6*inflation_factor*row.iloc[3],"y2050":1e6*inflation_factor*row.iloc[4]}}
            add_parameter_value(target_db,"technology__to_commodity","investment_cost","Base",(tech,to_node),map_inv)
        if pd.notna(row.iloc[5]):
            map_fom  = {"type":"map","index_type":"str","index_name":"period","data":{"y2030":inflation_factor*row.iloc[5],"y2040":inflation_factor*row.iloc[6],"y2050":inflation_factor*row.iloc[7]}}
            add_parameter_value(target_db,"technology__to_commodity","fixed_cost","Base",(tech,to_node),map_fom)
        if pd.notna(row.iloc[8]):
            map_vom  = {"type":"map","index_type":"str","index_name":"period","data":{"y2030":inflation_factor*row.iloc[8],"y2040":inflation_factor*row.iloc[9],"y2050":inflation_factor*row.iloc[10]}}
            add_parameter_value(target_db,"technology__to_commodity","operational_cost","Base",(tech,to_node),map_vom)
        if pd.notna(row.iloc[11]):
            add_parameter_value(target_db,"technology","lifetime","Base",(tech,),row.iloc[11])
    try:
        target_db.commit_session("tech production")
    except DBAPIError as e:
        print("commit tech production error")

def tech_storage(target_db,sheet):
    
    years = [f"y{year}" for year in ["2030","2040","2050"]]
    for tech in sheet.storage.unique():
        add_entity(target_db,"storage",(tech,))
        add_parameter_value(target_db,"storage","capacity","Base",(tech,),1.0)
        df = sheet[sheet.storage == tech]
        to_node = df.iloc[:,1].tolist()[0]
        add_entity(target_db,"storage_connection",(tech,to_node))
        add_parameter_value(target_db,"storage_connection","capacity_in","Base",(tech,to_node),1.0)
        add_parameter_value(target_db,"storage_connection","capacity_out","Base",(tech,to_node),1.0)

        energy_capex   = (1e6*df.iloc[:,3]).values
        energy_fom     = df.iloc[:,4].values
        power_vom      = df.iloc[:,5].values 
        efficiency_in  = df.iloc[:,6].values 
        efficiency_out = df.iloc[:,7].values
        lifetime       = df.iloc[:,8].tolist()[0]
        energy_invest  = (1e6*df.iloc[:,3]).values + df.iloc[:,4].values*lifetime
        hours_ratio    = df.iloc[:,9].tolist()[0]
        currency       = df.iloc[:,10].tolist()[0]

        if pd.notna(currency):
            inflation_factor = math.prod([1+value_*1e-2 for value_ in inflation.loc[currency+1:,"HICP"].tolist()])
            print(tech, currency, inflation_factor)
        if pd.notna(energy_invest[0]):
            map_inv  = {"type":"map","index_type":"str","index_name":"period","data":dict(zip(years,inflation_factor*energy_invest))}
            add_parameter_value(target_db,"storage","investment_cost","Base",(tech,),map_inv)
        if pd.notna(energy_fom[0]):
            map_fom  = {"type":"map","index_type":"str","index_name":"period","data":dict(zip(years,inflation_factor*energy_fom))}
            add_parameter_value(target_db,"storage","fixed_cost","Base",(tech,),map_fom)
        if pd.notna(power_vom[0]):
            map_vom  = {"type":"map","index_type":"str","index_name":"period","data":dict(zip(years,inflation_factor*power_vom/2.0))}
            add_parameter_value(target_db,"storage_connection","operational_cost_in","Base",(tech,to_node),map_vom)
            add_parameter_value(target_db,"storage_connection","operational_cost_out","Base",(tech,to_node),map_vom)
        if pd.notna(efficiency_in[0]):
            map_in  = {"type":"map","index_type":"str","index_name":"period","data":dict(zip(years,efficiency_in))}
            add_parameter_value(target_db,"storage_connection","efficiency_in","Base",(tech,to_node),efficiency_in.mean())
        if pd.notna(efficiency_out[0]):
            map_out  = {"type":"map","index_type":"str","index_name":"period","data":dict(zip(years,efficiency_out))}
            add_parameter_value(target_db,"storage_connection","efficiency_out","Base",(tech,to_node),efficiency_out.mean())
        if pd.notna(lifetime):
            add_parameter_value(target_db,"storage","lifetime","Base",(tech,),lifetime)
            '''add_parameter_value(target_db,"storage_connection","lifetime","Base",(tech+"-from",),lifetime)
            add_parameter_value(target_db,"storage_connection","lifetime","Base",(tech+"-to",),lifetime)'''
        '''if pd.notna(hours_ratio):
            add_parameter_value(target_db,"storage_connection","hours_ratio","Base",(tech,to_node),hours_ratio)'''
    try:
        target_db.commit_session("tech storage")
    except DBAPIError as e:
        print("commit tech storage error")

def ch4_production(target_db,sheet):

    for tech in sheet.technology.unique():
        if tech not in ["bio-diges-up","methanation"]:
            add_entity(target_db,"technology",(tech,))
            add_parameter_value(target_db,"technology","retirement_method","Base",(tech,),"not_retired")
            try:
                add_entity(target_db,"commodity",("fossil-CH4",))
            except:
                pass
            add_entity(target_db,"commodity__to_technology",("fossil-CH4",tech))
            add_entity(target_db,"technology__to_commodity",(tech,"CH4"))
            add_parameter_value(target_db,"technology__to_commodity","capacity","Base",(tech,"CH4"),1.0)
            add_entity(target_db,"commodity__to_technology__to_commodity",("fossil-CH4",tech,"CH4"))
            add_parameter_value(target_db,"commodity__to_technology__to_commodity","conversion_rate","Base",("fossil-CH4",tech,"CH4"),1.0)

    for country in sheet["To Country"].unique():
        add_entity(target_db,"region",(country,))   

    for index, row in sheet.iterrows():
        tech = row.iloc[0]
        country = row.iloc[1]
        capacity = row.iloc[3]
        cost = row.iloc[4]
        if tech in ["bio-diges-up","methanation"]:
            add_entity(target_db,"technology__region",(tech,country))
            map_cap = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round(capacity*1000/24,1) for year in ["2030"]}}
            add_parameter_value(target_db,"technology__region","units_existing","Base",(tech,country),map_cap)
        else:
            add_entity(target_db,"technology__region",(tech,country))
            add_entity(target_db,"technology__to_commodity__region",(tech,"CH4",country))
            map_cap = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round(capacity*1e3/24,1) for year in ["2030"]}}
            add_parameter_value(target_db,"technology__region","units_existing","Base",(tech,country),map_cap)
            if pd.notna(cost) and cost != 0.0 and isinstance(cost,float):
                add_parameter_value(target_db,"technology__to_commodity__region","operational_cost","Base",(tech,"CH4",country),round(cost,2))
    try:
        target_db.commit_session("Added CH4 production")
    except DBAPIError as e:
        print("commit CH4 production error")

def ch4_storage(target_db,sheet):

    com = "CH4"
    for tech in sheet.technology.unique():
        try:
            add_entity(target_db,"storage",(tech,))
            add_parameter_value(target_db,"storage","initial_state","Base",(tech,),0.9)
            add_parameter_value(target_db,"storage","storage_retirement_method","Base",(tech,),"not_retired")
            add_entity(target_db,"storage_connection",(tech,com))
            add_parameter_value(target_db,"storage_connection","retirement_method_in","Base",(tech,com),"not_retired")
            add_parameter_value(target_db,"storage_connection","retirement_method_out","Base",(tech,com),"not_retired")
            add_parameter_value(target_db,"storage_connection","efficiency_in","Base",(tech,com),1.0)
            add_parameter_value(target_db,"storage_connection","efficiency_out","Base",(tech,com),1.0)
        except:
            pass
    
    for country in sheet["Country"].unique():
        try:
            add_entity(target_db,"region",(country,)) 
        except:
            pass
    
    for index, row in sheet.iterrows():
        tech = row.iloc[0]
        country = row.iloc[1]
        capacity = row.iloc[3]
        capacity_in = row.iloc[4]
        capacity_out = row.iloc[5]
        inj_cost = row.iloc[6]
        with_cost = row.iloc[7]

        add_entity(target_db,"storage__region",(tech,country))
        map_cap = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round(capacity*1e6,1) for year in ["2030"]}}
        add_parameter_value(target_db,"storage__region","storages_existing","Base",(tech,country),map_cap)
        add_parameter_value(target_db,"storage__region","capacity","Base",(tech,country),1.0)
        
        add_entity(target_db,"storage_connection__region",(tech,com,country))
        map_cap_in = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round(capacity_in*1e3/24,1) for year in ["2030"]}}
        map_cap_out = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round(capacity_out*1e3/24,1) for year in ["2030"]}}
        add_parameter_value(target_db,"storage_connection__region","links_existing_in","Base",(tech,com,country),map_cap_in)
        add_parameter_value(target_db,"storage_connection__region","links_existing_out","Base",(tech,com,country),map_cap_out)
        add_parameter_value(target_db,"storage_connection__region","capacity_in","Base",(tech,com,country),1.0)
        add_parameter_value(target_db,"storage_connection__region","capacity_out","Base",(tech,com,country),1.0)
        add_parameter_value(target_db,"storage_connection__region","operational_cost_in","Base",(tech,com,country),round(inj_cost,2))
        add_parameter_value(target_db,"storage_connection__region","operational_cost_out","Base",(tech,com,country),round(with_cost,2))


    try:
        target_db.commit_session("Added CH4 storage")
    except DBAPIError as e:
        print("commit CH4 storage error")

def ch4_network(target_db,sheet):

    com = "CH4"

    for country in list(set(sheet["From Country"].unique().tolist() + sheet["To Country"].unique().tolist())):
        try:
            add_entity(target_db,"region",(country,)) 
        except:
            pass
    
    for index, row in sheet.iterrows():
        from_country = row.iloc[0]
        to_country = row.iloc[1]
        capacity = row.iloc[3]
        operational_cost = row.iloc[4]

        add_entity(target_db,"pipeline",(from_country,com,to_country))
        map_cap = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round(capacity*1e3/24,1) for year in ["2030"]}}
        add_parameter_value(target_db,"pipeline","links_existing","Base",(from_country,com,to_country),map_cap)
        add_parameter_value(target_db,"pipeline","capacity","Base",(from_country,com,to_country),1.0)
        add_parameter_value(target_db,"pipeline","retirement_method","Base",(from_country,com,to_country),"not_retired")
        map_cost = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round(operational_cost,2)  for year in ["2030","2040","2050"]}}
        add_parameter_value(target_db,"pipeline","operational_cost","Base",(from_country,com,to_country),round(operational_cost,2))
    try:
        target_db.commit_session("Added CH4 network")
    except DBAPIError as e:
        print("commit CH4 network error")

def h2_production(target_db,sheet):

    for tech in sheet.technology.unique():
        if tech not in ["SMR","SMR+CC"]:
            add_entity(target_db,"technology",(tech,))
            try:
                add_entity(target_db,"commodity",("global-H2",))
            except:
                pass
            add_entity(target_db,"commodity__to_technology",("global-H2",tech))
            add_entity(target_db,"technology__to_commodity",(tech,"H2"))
            add_entity(target_db,"commodity__to_technology__to_commodity",("global-H2",tech,"H2"))
            add_parameter_value(target_db,"technology","retirement_method","Base",(tech,),"not_retired")
            add_parameter_value(target_db,"technology__to_commodity","capacity","Base",(tech,"H2"),1.0)
            add_parameter_value(target_db,"commodity__to_technology__to_commodity","conversion_rate","Base",("global-H2",tech,"H2"),1.0)

    for country in sheet["Country"].unique():
        try:
            add_entity(target_db,"region",(country,))   
        except:
            pass

    for index, row in sheet.iterrows():
        tech = row.iloc[0]
        country = row.iloc[1]
        capacity = row.iloc[2]
        cost = row.iloc[3]
        if tech in ["SMR","SMR+CC"]:
            add_entity(target_db,"technology__region",(tech,country))
            map_cap = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round(capacity,1) for year in ["2030"]}}
            add_parameter_value(target_db,"technology__region","units_existing","Base",(tech,country),map_cap)
        else:
            add_entity(target_db,"technology__to_commodity__region",(tech,"H2",country))
            add_entity(target_db,"technology__region",(tech,country))
            if pd.notna(cost) and cost != 0.0:
                add_parameter_value(target_db,"technology__to_commodity__region","operational_cost","Base",(tech,"H2",country),round(cost,2))
            map_cap = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round(capacity,1) for year in ["2030"]}}
            add_parameter_value(target_db,"technology__region","units_existing","Base",(tech,country),map_cap)
    try:
        target_db.commit_session("Added H2 production")
    except DBAPIError as e:
        print("commit H2 production error")

def h2_storage(target_db,sheet):

    com = "H2"
    for tech in sheet.technology.unique():
        try:
            add_entity(target_db,"storage",(tech,))
            add_entity(target_db,"storage_connection",(tech,com))
        except:
            pass
    
    for country in sheet["Country"].unique():
        try:
            add_entity(target_db,"region",(country,)) 
        except:
            pass
    
    for index, row in sheet.iterrows():
        tech = row.iloc[0]
        country = row.iloc[1]
        capacity = row.iloc[2]
        capacity_2030 = row.iloc[3]
        capacity_2040 = row.iloc[4]
        capacity_2050 = row.iloc[5]
        power = row.iloc[6]
        power_2030 = row.iloc[7]
        power_2040 = row.iloc[8]
        power_2050 = row.iloc[9]
        load = row.iloc[10]
        load_2030 = row.iloc[11]
        load_2040 = row.iloc[12]
        load_2050 = row.iloc[13]

        add_entity(target_db,"storage__region",(tech,country))
        map_cap = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round(capacity*1e3,1)  for year in ["2030"]}}
        add_parameter_value(target_db,"storage__region","storages_existing","Base",(tech,country),map_cap)
        map_sto_pot = {"type":"map","index_type":"str","index_name":"period","data":{"y2030":round(capacity_2030*1e3,1),"y2040":round(capacity_2040*1e3,1),"y2050":round(capacity_2050*1e3,1)}}
        add_parameter_value(target_db,"storage__region","potentials","Base",(tech,country),map_sto_pot)
        
        add_entity(target_db,"storage_connection__region",(tech,com,country))
        map_cap = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round(load,1)  for year in ["2030"]}}
        add_parameter_value(target_db,"storage_connection__region","links_existing_in","Base",(tech,com,country),map_cap)
        map_load_pot = {"type":"map","index_type":"str","index_name":"period","data":{"y2030":round(load_2030,1),"y2040":round(load_2040,1),"y2050":round(load_2050,1)}}
        add_parameter_value(target_db,"storage_connection__region","potentials_in","Base",(tech,com,country),map_load_pot)
        
        map_cap = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round(power,1)  for year in ["2030"]}}
        add_parameter_value(target_db,"storage_connection__region","links_existing_out","Base",(tech,com,country),map_cap)
        map_power_pot = {"type":"map","index_type":"str","index_name":"period","data":{"y2030":round(power_2030,1),"y2040":round(power_2040,1),"y2050":round(power_2050,1)}}
        add_parameter_value(target_db,"storage_connection__region","potentials_out","Base",(tech,com,country),map_power_pot)
    try:
        target_db.commit_session("Added H2 storage")
    except DBAPIError as e:
        print("commit H2 storage error")

def h2_network(target_db,sheet):

    com = "H2"
    for country in list(set(sheet["From Country"].unique().tolist() + sheet["To Country"].unique().tolist())):
        try:
            add_entity(target_db,"region",(country,)) 
        except:
            pass
    
    for index, row in sheet.iterrows():
        from_country = row.iloc[0]
        to_country = row.iloc[1]
        capacity = row.iloc[2]
        operational_cost = row.iloc[3]
        potentials_30 = row.iloc[4]
        potentials_40 = row.iloc[5]
        investment_cost_30 = row.iloc[6]
        investment_cost_40= row.iloc[7]

        add_entity(target_db,"pipeline",(from_country,com,to_country))
        map_cap = {"type":"map","index_type":"str","index_name":"period","data":{"y2030":round(capacity*1e3/24,1)}}
        add_parameter_value(target_db,"pipeline","links_existing","Base",(from_country,com,to_country),map_cap)
        add_parameter_value(target_db,"pipeline","capacity","Base",(from_country,com,to_country),1.0)
        if pd.notna(operational_cost):
            map_cost = {"type":"map","index_type":"str","index_name":"period","data":{f"y{year}":round(operational_cost,1) for year in ["2030","2040","2050"]}}
            add_parameter_value(target_db,"pipeline","operational_cost","Base",(from_country,com,to_country),round(operational_cost,1))
        if (potentials_30 + potentials_40) > 0.0:
            map_pot = {"type":"map","index_type":"str","index_name":"period",
                       "data":{"y2030":round((capacity+potentials_30)*1e3/24,1),"y2040":round((capacity+potentials_30+potentials_40)*1e3/24,1),"y2050":round((capacity+potentials_30+potentials_40)*1e3/24,1)}}
            add_parameter_value(target_db,"pipeline","potentials","Base",(from_country,com,to_country),map_pot)
            inv_30 = round((investment_cost_30 if pd.notna(investment_cost_30) else 0.0)*24*1e3,1)
            inv_40 = round((investment_cost_40 if pd.notna(investment_cost_40) else investment_cost_30)*24*1e3,1)
            map_inv = {"type":"map","index_type":"str","index_name":"period",
                        "data":{"y2030":inv_30,"y2040":inv_40,"y2050":inv_40}}
            add_parameter_value(target_db,"pipeline","investment_cost","Base",(from_country,com,to_country),map_inv)
            add_parameter_value(target_db,"pipeline","lifetime","Base",(from_country,com,to_country),40.0)
    try:
        target_db.commit_session("Added H2 network")
    except DBAPIError as e:
        print("commit H2 network error")

def main():

    print("############### Filling the output DB ###############")
    with DatabaseMapping(url_db_out) as target_db:

        ## Empty the database
        target_db.purge_items('entity')
        target_db.purge_items('parameter_value')
        target_db.purge_items('alternative')
        target_db.purge_items('scenario')
        target_db.refresh_session()

        with open("gas_template_DB.json", 'r') as f:
            db_template = json.load(f)
        # Importing Map
        api.import_data(target_db,
                    entity_classes=db_template["entity_classes"],
                    parameter_definitions=db_template["parameter_definitions"],
                    )
        
        for alternative_name in ["Base"]:
            add_alternative(target_db,alternative_name)

        tech_conversion(target_db,sheets["Technology_Conversion"])
        print("added ->","conversion")
        tech_production(target_db,sheets["Technology_Costs"])
        print("added ->","tech costs")
        tech_storage(target_db,sheets["Storage_Costs"])
        print("added ->","tech storage")
        ch4_production(target_db,sheets["CH4_Production"])
        print("added ->","CH4_Production")
        ch4_storage(target_db,sheets["CH4_Storage"])
        print("added ->","CH4_Storage")
        ch4_network(target_db,sheets["CH4_Network"])
        print("added ->","CH4_Network")  
        h2_production(target_db,sheets["H2_Production"])
        print("added ->","H2_Production")
        h2_storage(target_db,sheets["H2_Storage"])
        print("added ->","H2_Storage")
        h2_network(target_db,sheets["H2_Network"])
        print("added ->","H2_Network")
    

if __name__ == "__main__":
    main()