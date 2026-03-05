using CSV
using DataFrames
using Dates
using Shapefile
using ZipFile
using Plots
using ColorSchemes
using GeoDataFrames
using GeoFormatTypes
using PlotlyJS, JSON3, JSON
include("get_data.jl") ;
include("qgis_attributes.jl") ;
function mapping_html(;res=nothing, year=nothing, scenario=nothing, maincat=nothing, save=nothing)
    function deletecontent(filename)
        abc = open(filename, "w")
        write(abc, "")    
        close(abc)
    end
    if isnothing(res) resdef = "nuts3" else resdef = res ; end
    # only 2020 available year
    if isnothing(year) yeardef = 2020  else yeardef = year ; end
    if isnothing(scenario) scenariodef = "high" else scenariodef = scenario ; end
    if isnothing(maincat) maincatdef = "agriculture" else maincatdef = maincat ; end
    if isnothing(save) savedef = true else savedef = false ; end
    #filesep = Base.Filesystem.pathsep() ;
    rootfolder =  @__DIR__ ;
    cd(rootfolder) ;
    if occursin("nuts", resdef)
        subfolder = "NUTS_RG_20M_2013_CRS84" ;
        nutspath = joinpath(rootfolder,subfolder)
        # Read shapefile
        df = GeoDataFrames.read(joinpath(nutspath, "NUTS_RG_20M_2013_CRS84.shp"))
    else
        subfolder = "entsoe_shp" ;
        nutspath = joinpath(rootfolder,subfolder)
        # Read shapefile
        df = GeoDataFrames.read(joinpath(nutspath, "entsoe_bid.shp"))
    end

    # Load the data from the database
    mopo_data = qgis_attributes(scenario=scenariodef, maincat=maincatdef, res=resdef, save=savedef)

    #df.geometry = GeoDataFrames.reproject(df.geometry, GeoFormatTypes.EPSG(3035), GeoFormatTypes.EPSG(4326))
    # Create geodataframe

    gdf = leftjoin(df, mopo_data, on = :NUTS_ID)

    if occursin("nuts", resdef)
        select!(gdf, [:geometry, :quantity, :FID, :roadsidecost])
    else
        select!(gdf, [:geometry, :quantity, :NUTS_ID, :roadsidecost])
    end

    df1 = filter(row -> !ismissing(row.quantity), gdf)
    df2 = filter(row -> !isnan(row.quantity), df1)
    df2 = filter(row -> !isnan(row.roadsidecost), df2)
    df2.id=1:nrow(df2) ;
    # If file exist, overwrite the geojson file
    #if isfile("temp_points5.geojson")
    #print(df2)
    # rm("temp_points5.geojson", force=true)
    #deletecontent("temp_points5.geojson")
    #deletecontent("temp_my2.json")
    if !isdir("./temp")
        mkdir("temp")
    end

    if isfile("./temp/temp_points0.geojson")
        filenumber = length(readdir("./temp")) / 2 
    else
        filenumber = 0
    end
    geoj_file = "./temp/temp_points" * string(filenumber) * ".geojson"
    json_file = "./temp/temp_my" * string(filenumber) * ".json"
    GeoDataFrames.write(geoj_file, df2)

    # Write geojson as json file
    open(json_file, "w") do f
        JSON3.pretty(f, JSON3.write(JSON3.read(geoj_file)))
    end

    jsondata= JSON.parsefile(json_file)
    jsondata["crs"] = Dict{String, Any}("properties"=>Dict{String, Any}("name"=>"urn:ogc:def:crs:EPSG::3035", "type"=>"name"))
    # Add id values to the geojson
    for (k, feat) in enumerate(jsondata["features"])
        feat["properties"]["id"]=k
    end

    # optional to save the file
    #stringdata = JSON.json(jsondata)
    #open("newjson.json", "w") do f
    #    JSON3.pretty(f, JSON3.write(jsondata))
    #end
    #newjdata= JSON.parsefile("newjson.json") ;
    df3 = select!(df2, Not(:geometry))

    if occursin("nuts", resdef)
        label = df3.FID
    else
        label = df3.NUTS_ID
    end

    plotid = Plot(choroplethmapbox(geojson = jsondata,
                        featureidkey = "properties.id",
                        locations = df3.id,
                        z=df3.quantity,
                        zmin=0, 
                        zmax=maximum(df3.quantity), #"Area: " * df3.FID[1]
                        text=label,
                        colorscale="pinkyl",
                        legend = "some title",
                        marker=attr(opacity=0.85, 
                                    line=attr(width=0.5, color="white"))),
                        Layout(mapbox = attr(center=attr(lon =15, lat=57),
                                            zoom=2.5, style="open-street-map"),
                                height = 750
                                            ));
    if save
        display(plotid)
        open("mopo_$maincatdef" * "_$scenariodef" * "_$resdef.html", "w") do io
            PlotlyBase.to_html(io, plotid)
        end
    end
    #savefig(plotid,"mopo_$maincatdef" * "_$scenariodef" * "_$resdef.pdf")
    return plotid
end