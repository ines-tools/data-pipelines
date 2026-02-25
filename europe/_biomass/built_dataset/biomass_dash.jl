using Dash
using DataFrames, PlotlyJS, CSV, SQLite, DataStructures

include("mapping_html.jl")

#csv_data = download("https://raw.githubusercontent.com/plotly/datasets/master/gapminderDataFiveYear.csv")
#df = CSV.read(csv_data, DataFrame)

#years = unique(df[!, :year])

# Declare the dash board 
app = dash()

# Get the categories from the database and place them in a dictionnary
tablename = "biomass_mass" ;
db_b = SQLite.DB("biomass.db")

query = "SELECT DISTINCT scenario FROM $tablename" ;
data = SQLite.DBInterface.execute(db_b,query) ;
df_scenario = DataFrames.DataFrame(data);
scenariodic = [(label = i, value = i) for i in df_scenario.scenario]


resolution_option = [
    Dict("label" => "nuts0", "value" => "nuts0"),
    Dict("label" => "nuts1", "value" => "nuts1"),
    Dict("label" => "nuts2", "value" => "nuts2"),
    Dict("label" => "nuts3", "value" => "nuts3"),
    Dict("label" => "entsoe", "value" => "entsoe_bid"),
]

query = "SELECT DISTINCT maincat FROM $tablename" ;
data = SQLite.DBInterface.execute(db_b,query) ;
df_maincat = DataFrames.DataFrame(data);
maincatic = [] ;
maincatic = [(label = i, value = i) for i in df_maincat.maincat]

# Load the static shapefile in the memory

app.layout = html_div() do
    
    html_div(
        children = [html_label("Scenario"), 
                    dcc_dropdown(id="scenario", options = scenariodic, value = "high")],
        style = (width = "30%", display = "inline-block")),
    html_div(  
        children = [html_label("Category"),
                    dcc_dropdown(id="cat", options = maincatic, value = "agriculture")],
        style = (width = "30%", display = "inline-block")),
    html_div( 
        children = [html_label("Geographical resolution"),
                    dcc_dropdown(id="res", options = resolution_option, value = "nuts0")],
        style = (width = "30%", display = "inline-block")),
    dcc_graph(id = "graph")
end

callback!(
    app,
    Output("graph", "figure"),
    Input("scenario", "value"),
    Input("res", "value"),
    Input("cat", "value"),
    ) do scenario, res, cat
        plotgraph = mapping_html(res=res,scenario=scenario, maincat=cat, save=false)
        return plotgraph
    end
    

run_server(app, "0.0.0.0", debug=false)