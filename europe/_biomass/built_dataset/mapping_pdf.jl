using CSV
using DataFrames
using Dates
using Shapefile
using ZipFile
using Plots
using ColorSchemes
using GeoDataFrames
using GeoFormatTypes
using PlotlyJS
include("get_data.jl") ;
include("qgis_attributes.jl") ;
function mapping_pdf(;res=nothing, year=nothing, scenario=nothing, maincat=nothing)
    if isnothing(res) resdef = "nuts3" else resdef = res ; end
    # only 2020 available year
    if isnothing(year) yeardef = 2020  else yeardef = year ; end
    if isnothing(scenario) scenariodef = "high" else scenariodef = scenario ; end
    if isnothing(maincat) maincatdef = "agriculture" else maincatdef = maincat ; end
#filesep = Base.Filesystem.pathsep() ;
rootfolder =  @__DIR__ ;
cd(rootfolder) ;
subfolder = "NUTS_RG_20M_2013_3035.shp" ;
nutspath = joinpath(rootfolder,subfolder)

# Load the data from the database
mopo_data = qgis_attributes(scenario=scenariodef, maincat=maincatdef, res=resdef)

# Read shapefile
df = GeoDataFrames.read(joinpath(nutspath, "NUTS_RG_20M_2013_3035.shp"))
#df.geometry = GeoDataFrames.reproject(df.geometry, GeoFormatTypes.EPSG(3035), GeoFormatTypes.EPSG(2029))
# Create geodataframe

gdf = leftjoin(
    df, mopo_data, on = :NUTS_ID
)


select!(gdf, [:geometry, :quantity])
df1 = filter(row -> !ismissing(row.quantity), gdf)

# plot complete rows
plotid = Plots.plot(
    df1.geometry, 
    fill = cgrad(:matter,10, rev = false, categorical = true), 
    fill_z = reshape(df1.quantity, 1, nrow(df1)), 
    axis = false, 
    ticks = false,
    linewidth=.1,
    size = (600, 450),
    legend_fontsize=8, legend_title="Mopo $maincatdef $yeardef [PJ]",
)
#display(plotid)
Plots.savefig(plotid,"mopo_$maincatdef" * "_$scenariodef" * "_$resdef.pdf")
return mopo_data
end