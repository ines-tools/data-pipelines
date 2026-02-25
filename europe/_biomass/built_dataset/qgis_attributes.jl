using DataFrames, Statistics, CSV
function qgis_attributes(;scenario=nothing, maincat=nothing, year=nothing, res=nothing, save=nothing)
    if isnothing(year) yearquery= 2020; else yearquery= year; end
    if isnothing(scenario) scenarioquery= "high"; else scenarioquery= scenario; end
    if isnothing(maincat) maincatquery= "agriculture"; else maincatquery= maincat; end
    if isnothing(res) resdef = "nuts3" else resdef = res ; end
    if isnothing(save) savedef = true else savedef = false ; end

    df = DataFrame(type_id = Int64[], nuts3 = String[], nuts2 = String[], 
    nuts1 = String[], nuts0 = String[], quantity = Float64[],
    cat_id= Int64[], subcat_id= Int64[], 
    scenario= String[], unit= String[], NCV=Float64[], entsoe_nuts=String[], maincat=String[], roadsidecost=Float64[])

    # Collect all nuts but CH
    df = get_data(scenario=scenarioquery,maincat=maincatquery) ;
    if !occursin("nuts", resdef)
        resdef = "entsoe_nuts"
    end
    nuts = unique(df[!,resdef]) ;
    nutsout = DataFrame(NUTS_ID = String[], quantity = Float64[], roadsidecost = Float64[]) ;
    sub_extract = [] ;
    for n in eachindex(nuts)
        sub_extract = [] ;
        if cmp(resdef, "nuts3")==0 
            sub_extract = filter(row -> row.nuts3==nuts[n], df) ;
        elseif cmp(resdef, "nuts2")==0 
            sub_extract = filter(row -> row.nuts2==nuts[n], df) ;
        elseif cmp(resdef, "nuts1")==0 
            sub_extract = filter(row -> row.nuts1==nuts[n], df) ;
        elseif cmp(resdef, "nuts0")==0 
            sub_extract = filter(row -> row.nuts0==nuts[n], df) ;
        elseif cmp(resdef, "entsoe_nuts")==0 
            sub_extract = filter(row -> row.entsoe_nuts==nuts[n], df) ;
        end
        ratio = [];
        norm_cost = [] ;
        try
            ratio  = sub_extract.quantity ./ sum(skipmissing(sub_extract.quantity)) ;
            norm_cost = sum(skipmissing(sub_extract.roadsidecost .* ratio))
        catch
            norm_cost = 0;
            if savedef
                println(n, nuts[n])
            end
        end
        push!(nutsout, [nuts[n]  sum(skipmissing(sub_extract.quantity)) norm_cost])
    end
    
    # Collect CH
    #dfCH = query_db2(scenario=scenarioquery,maincat=maincatquery,nuts="CH") ;
    #nuts = unique(dfCH[!,resdef]) ;



    #for n in eachindex(nuts)
        ##sub_extract = [] ;
        #if cmp(resdef, "nuts3")==0 
        #    sub_extract = filter(row -> row.nuts3==nuts[n], dfCH) ;
        #elseif cmp(resdef, "nuts2")==0 
        #    sub_extract = filter(row -> row.nuts2==nuts[n], dfCH) ;
        #elseif cmp(resdef, "nuts1")==0 
        #    sub_extract = filter(row -> row.nuts1==nuts[n], dfCH) ;
        #elseif cmp(resdef, "nuts0")==0 
        #    sub_extract = filter(row -> row.nuts0==nuts[n], dfCH) ;
        #end
        #try
        #    ratio  = sub_extract.quantity ./ sum(skipmissing(sub_extract.quantity)) ;
        #    norm_cost = sum(skipmissing(sub_extract.roadsidecost .* ratio))
        #catch
        #    norm_cost = 0;
        #    println(n, nuts[n])
        #end
        #push!(nutsout, [nuts[n]  sum(skipmissing(sub_extract.quantity)) norm_cost])
    #end


    if savedef
        CSV.write("mopo_$maincatquery _$scenarioquery _$year.csv", nutsout) ;
    end
    #quantile = quantile!(nutsout.quantity, [0.1, 0.2, 0.3, 0.4,  0.5, 0.6, 0.7, 0.8, 0.9])
    return nutsout
end