using CSV, DataFrames,XLSX, SQLite, DataStructures, Logging, LoggingExtras, Dates
include("downloads_file.jl")
include("query_db_enspresso_costs.jl")
#collect_biomass_data(update=true)
function collect_biomass_data(;update=nothing)
    # Declar variables
    filesep = Base.Filesystem.pathsep() ;
    rootfolder =  @__DIR__ ;
    cd(rootfolder) ;
    subfolder = "cost_supply_data" ;
    subfolderCH = "swiss database" ;
    enspresso  = "./enspreso/ENSPRESO_BIOMASS.xlsx" ;
    file = readdir(subfolder) ;
    fileCH = readdir(subfolderCH) ;
    
    # binary to have years or not in the database
    includeyear = false ;

    # Load all the definitions
    codedef = CSV.read(("biomass_code.csv"), DataFrame) ;
    conversion_s2biom = CSV.read("conversion_db.csv", DataFrame) ;
    entsoe_nuts = CSV.read("nuts_bidding_zone.csv", DataFrame);
    conversion_CH = CSV.read("conversion_CH.csv",delim =";", DataFrame);
    enspresso_mopo = CSV.read("enspresso_mopo_db.csv", DataFrame);
    
    # List all the countries that we need to get from enspresso
    enspressocountry = OrderedDict() ;
    merge!(enspressocountry,Dict("NO"=>"Norway"))

    # create new database or open existing database if it exists
    db_biomass_sqlite = SQLite.DB("biomass.db")
    # Initialise the database 
    SQLite.execute(db_biomass_sqlite, "DROP TABLE IF EXISTS biomass_mass")
    

    if includeyear
        SQLite.execute(db_biomass_sqlite, "CREATE TABLE biomass_mass
            (type_id INTEGER, nuts3 TEXT, nuts2 TEXT, nuts1 TEXT, 
            nuts0 TEXT, quantity REAL, cat_id INTEGER, subcat_id INTEGER, scenario TEXT, year INTEGER,
            unit TEXT, NCV REAL, entsoe_nuts TEXT, maincat TEXT, roadsidecost REAL)
            ")
    else
        SQLite.execute(db_biomass_sqlite, "CREATE TABLE biomass_mass
            (type_id INTEGER, nuts3 TEXT, nuts2 TEXT, nuts1 TEXT, 
            nuts0 TEXT, quantity REAL, cat_id INTEGER, subcat_id INTEGER, scenario TEXT,
            unit TEXT, NCV REAL, entsoe_nuts TEXT, maincat TEXT, roadsidecost REAL)
            ")
    end
    # Setup the logger to record info
    function logger_fun(msg)
        io = open("log.txt", "a")
        # Create a simple logger
        logger = SimpleLogger(io) ;
        
        with_logger(logger) do
            @info(Dates.format(now(), "yyyy/mm/dd HH:MM:SS") * " - " * msg)
        end
        close(io)
    end
    # declare subroutines to be used in the code
    function switchNCV(scenario, NCV)
        cmp(scenario, "high") == 0 && return rename!(select(NCV, [:type_id, :Maximum]),:Maximum => :quantity) ;
        cmp(scenario, "medium") == 0 && return rename!(select(NCV, [:type_id, :Typical]),:Typical => :quantity) 
        cmp(scenario, "low") == 0 && return rename!(select(NCV, [:type_id, :Minimum]),:Minimum => :quantity) 
        println("too big")  
    end

    function df_param()
        # Start the code
        db_biomass = DataFrame(type_id = Int64[], nuts3 = String[], nuts2 = String[], 
                                nuts1 = String[], nuts0 = String[], quantity = Float64[],
                                cat_id= Int64[], subcat_id= Int64[], 
                                scenario= String[], year=Int64[], unit= String[], NCV=Float64[], entsoe_nuts=String[], maincat=String[], roadsidecost=Float64[])
        return db_biomass
    end

    function df_param_noyear()
        # Start the code
        db_biomass = DataFrame(type_id = Int64[], nuts3 = String[], nuts2 = String[], 
                                nuts1 = String[], nuts0 = String[], quantity = Float64[],
                                cat_id= Int64[], subcat_id= Int64[], 
                                scenario= String[], unit= String[], NCV=Float64[], entsoe_nuts=String[], maincat=String[], roadsidecost=Float64[])
        return db_biomass
    end

    function parse2table(finaldf, data, rownames, codedef, md, entsoe_nuts, convert, conversion_s2biom, costs, includeyear)
        for i=1:size(data,1), j=1:size(data,2)
            type_id = parse(Int64,  names(data)[j]) ;
            if cmp(convert, "forestry") == 0
                if ~last(digits(type_id)) == 1
                    continue;
                end
            elseif cmp(convert, "agriculture") == 0
                if last(digits(type_id)) == 1
                    continue;
                end
            end
            # if type_id == 4212 || type_id == 2225
            #      continue;
            # end
            
            nuts3 = rownames[i] ;
            nuts2 = rownames[i][1:end-1] ;
            nuts1 = rownames[i][1:end-2] ;
            nuts0 = rownames[i][1:end-3] ;
            entsoe_zone = entsoe_nuts[entsoe_nuts.nuts3 .== nuts3, "bidding_zone"] ;
            quantity = data[!,names(data)[j]][i] ;
            #println(nuts3)
            #println(entsoe_zone)
            if ismissing(quantity)
                quantity = NaN ;
            end
            sub_pos         = findall(x->x==type_id, codedef[!,"type_id"]) ;
            cat_id          = codedef[!,"cat_id"][sub_pos] ;
            subcat_id       = codedef[!,"subcat_id"][sub_pos] ;
            #categorie       = codedef[!,"categorie"][sub_pos] ;
            #subcategorie    = codedef[!,"subcategorie"][sub_pos] ;
            #type_name       = codedef[!,"type_name"][sub_pos] ;
            #short_name      = codedef[!,"short_name"][sub_pos] ;
            scenario        = md[2] ;
            year            = parse(Int64,  md[3]) ; ;
            unit            = md[4] ;
            NCV             = 1 ;

            # get the costs associated to the type_id
            if hasproperty(costs,string(type_id))
                costsdb = costs[i,string(type_id)] ;
                if ismissing(costsdb)
                    costsdb = NaN ;
                end
            else
                costsdb = NaN ;
            end
            if cmp(unit,"euro")==0
                unit = "euro/kton";
            end
            maincat = filter(row -> row.type_id==type_id, conversion_s2biom) ;
            #println([type_id  nuts3   nuts2   nuts1   nuts0   quantity  cat_id  subcat_id scenario  year   unit entsoe_zone])
            #println(       [type_id  nuts3   nuts2   nuts1   nuts0   quantity  cat_id  subcat_id  categorie  subcategorie  type_name  short_name  scenario  year   unit])
            if includeyear
                push!(finaldf, [type_id  nuts3   nuts2   nuts1   nuts0   quantity  cat_id  subcat_id scenario  year   unit NCV entsoe_zone maincat.cat_aggreg[1] costsdb])
            else
                push!(finaldf, [type_id  nuts3   nuts2   nuts1   nuts0   quantity  cat_id  subcat_id scenario  unit NCV entsoe_zone maincat.cat_aggreg[1] costsdb])
            end
        end
        return finaldf
    end

    function parse_ener_2table(finaldf, db_biomass, conversion_s2biom, selected_type_id, entsoe_nuts, includeyear)
        extractdata = filter(row -> row.type_id==selected_type_id, db_biomass) ; #db_biomass[findall(in([selected_type_id]), db_biomass.type_id), :] ;
        conversion  = filter(row -> row.type_id==selected_type_id, conversion_s2biom) ;
        scenarioextract = unique(extractdata[!,"scenario"]) ;
        for nscenario in eachindex(scenarioextract)
            #println(nscenario)
            #println(selected_type_id) 
            nvc_value       = switchNCV(scenarioextract[nscenario], conversion) ;
            sub_extract     = filter(row -> row.scenario==scenarioextract[nscenario], extractdata) ;
            type_id         = sub_extract[!,"type_id"] ;
            roadsidecost    = sub_extract[!,"roadsidecost"] ;
            nuts3           = sub_extract[!,"nuts3"] ;
            nuts2           = sub_extract[!,"nuts2"] ;
            nuts1           = sub_extract[!,"nuts1"] ;
            nuts0           = sub_extract[!,"nuts0"] ;
            cat_id          = sub_extract[!,"cat_id"] ;
            subcat_id       = sub_extract[!,"subcat_id"] ;
            maincat         = sub_extract[!,"maincat"] ;
            #categorie       = extractdata[!,"categorie"] ;
            #subcategorie    = extractdata[!,"subcategorie"] ;
            #type_name       = extractdata[!,"type_name"] ;
            #short_name      = extractdata[!,"short_name"] ;
            scenario        = sub_extract[!,"scenario"] ;
            if includeyear
                year            = sub_extract[!,"year"] ;
            end
            unitoriginal    = sub_extract[!,"unit"] ; 
            unit            = replace(unitoriginal, "kton"=>"PJ") ; 
            bb              = findfirst.(isequal.(nuts3), (entsoe_nuts.nuts3,)) ;
            entsoe_zone     = entsoe_nuts.bidding_zone[bb] ;
            
            #NCV_level       = repeat("quantity", outer = length(nuts3)) ;
            NCV             = repeat(nvc_value.quantity, outer = length(nuts3)) ;
            quantity        = sub_extract.quantity .* nvc_value.quantity ./1000    ;

            # Recalculate the roadside cost by energy unit based on the NCV value
            roadsidecost    = roadsidecost ./ nvc_value.quantity ;

            append!(finaldf.type_id,type_id)
            append!(finaldf.nuts3,nuts3)
            append!(finaldf.nuts2,nuts2)
            append!(finaldf.nuts1,nuts1)
            append!(finaldf.nuts0,nuts0)
            append!(finaldf.entsoe_nuts,entsoe_zone)
            append!(finaldf.cat_id,cat_id)
            append!(finaldf.subcat_id,subcat_id)
            #append!(finaldf.categorie,categorie)
            #append!(finaldf.subcategorie,subcategorie)
            #append!(finaldf.type_name,type_name)
            #append!(finaldf.short_name,short_name)
            append!(finaldf.scenario,scenario)
            if includeyear
                append!(finaldf.year,year)
            end
            append!(finaldf.unit,unit)
            append!(finaldf.NCV,NCV)
            #append!(finaldf.NCV_level,NCV_level)
            append!(finaldf.quantity,quantity)
            append!(finaldf.maincat,maincat)
            append!(finaldf.roadsidecost,roadsidecost)
        end
        return finaldf
    end

    function complete_mass_db(db_biomass, file, subfolder,filesep, entsoe_nuts, codedef)
        # Start the code
        db_biomass = DataFrame(type_id = Int64[], nuts3 = String[], nuts2 = String[], 
                                nuts1 = String[], nuts0 = String[], quantity = Float64[],
                                cat_id= Int64[], subcat_id= Int64[], 
                                scenario= String[], year=Int64[], unit= String[], entsoe_nuts=String[])
        #db_biomass = copy(temp_db_biomass) ;
        b          = copy(db_biomass)    ; 
        for nfile in eachindex(file)
            xf = XLSX.readxlsx(subfolder * filesep * file[nfile]) 
            #println(file[nfile]);
            for n = 3:length(xf.workbook.sheets)
                md = split(xf[n].name, "_") ;
                sh = xf[n]   ;
                m  = sh[:]   ;
                df = DataFrame([something.(col, missing) for col in eachcol(m[2:end, 2:end])], Symbol.(m[1,2:end])) ;
                db_biomass = parse2table(b, df, m[2:end,1], codedef, md, entsoe_nuts)            
            end
        end    
        CSV.write("db_biomass_mass_complete.csv", db_biomass)
    end
    
    function get_cost_s2biom(xf)
        sheetcosts = "rs_cost_2020_euro" ;
        costs = [] ;
        for n in eachindex(xf.workbook.sheets)
            if cmp(xf[n].name, sheetcosts) == 0
                sh = xf[n]   ;
                m  = sh[:]   ;
                costs = DataFrame([something.(col, missing) for col in eachcol(m[2:end, 2:end])], Symbol.(m[1,2:end])) ;
            end
        end 
        return costs  
    end

    function refined_mass_db(file, subfolder,filesep, entsoe_nuts, codedef, conversion_s2biom, includeyear)
        # Start the code
       if includeyear
            db_biomass = df_param() ;
       else
            db_biomass = df_param_noyear() ;
       end
        #db_biomass = copy(temp_db_biomass) ;
        b          = copy(db_biomass)    ; 

        for nfile in eachindex(file)
            xf = XLSX.readxlsx(subfolder * filesep * file[nfile]) ;
            #println(file[nfile]);
            # collect the cost information
            costs = get_cost_s2biom(xf) ;
                  
            for n in eachindex(xf.workbook.sheets)
                md = split(xf[n].name, "_") ;
                # Only consider the sheet with mass data "dm"
                if cmp(md[1], "dm") == 0
                    if cmp(md[2], "TECH") == 0 # tech is used only for agrictulture and waste, HIGH for forestry
                        md[2] = "high" ;
                        convert = "agriculture" ;
                    elseif cmp(md[2], "HIGH") == 0
                        md[2] = "high" ;
                        convert = "forestry" ;
                    elseif cmp(md[2], "BASE") == 0 # used for all
                        md[2] = "medium" ;  
                        convert = "all" ;  
                    elseif cmp(md[2], "UD01") == 0# UD01 is used only for agrictulture and waste, UD06 for forestry
                        md[2] = "low" ;
                        convert = "agriculture" ;
                    elseif cmp(md[2], "UD06") == 0
                        md[2] = "low" ;
                        convert = "forestry" ;
                    else
                        continue ;
                    end
                    if !includeyear
                        if cmp(md[3],"2020") != 0
                            continue ;
                        end
                    end
                    sh = xf[n]   ;
                    m  = sh[:]   ;
                    df = DataFrame([something.(col, missing) for col in eachcol(m[2:end, 2:end])], Symbol.(m[1,2:end])) ;
                    db_biomass = parse2table(b, df, m[2:end,1], codedef, md, entsoe_nuts, convert, conversion_s2biom, costs, includeyear)  
                end          
            end
        end   
        return  db_biomass
    end

    function refined_ener_db(db_biomass, conversion_s2biom,entsoe_nuts, includeyear)
        if includeyear
            db_biomass_ener = df_param() ;
        else
            db_biomass_ener = df_param_noyear() ;
        end
        b2 = copy(db_biomass_ener) ;
        alltypeid = unique(db_biomass[!,"type_id"]) ;
        for irow in eachindex(alltypeid)
            #conversion = allval .* conversion_s2biom[findall(in([row]), conversion_s2biom.type_id), :].type_id
            #println(alltypeid[irow]);
            db_biomass_ener = parse_ener_2table(b2, db_biomass, conversion_s2biom, alltypeid[irow], entsoe_nuts, includeyear)
            #println(size(db_biomass_ener,1))
        end
        return db_biomass_ener
    end

    function parse2tableCH(finaldf, data, rownames, codedef, md, entsoe_nuts, convert, conversion_s2biom,conversion_CH,includeyear)
        # Exchange rate can be modified or use an api if needed
        exchange_rate = 0.8661 ;
        for i=1:size(data,1), j=1:size(data,2)
            type_id_temp  = names(data)[j] ;
            type_id_temp2 = filter(row -> row.categorie==type_id_temp, conversion_CH) ;
            type_id       = type_id_temp2.type_id[1]
            if cmp(convert, "forestry") == 0
                if ~last(digits(type_id)) == 1
                    continue;
                end
            elseif cmp(convert, "agriculture") == 0
                if last(digits(type_id)) == 1
                    continue;
                end
            end
            # if type_id == 4212 || type_id == 2225
            #      continue;
            # end
            
            nuts3 = rownames[i] ;
            nuts2 = rownames[i][1:end-1] ;
            nuts1 = rownames[i][1:end-2] ;
            nuts0 = rownames[i][1:end-3] ;
            #println(nuts3)
            entsoe_zone = entsoe_nuts[entsoe_nuts.nuts3 .== nuts3, "bidding_zone"][1] ;
            quantity = data[!,names(data)[j]][i] / 1e6;
            conversion  = filter(row -> row.type_id==type_id, conversion_s2biom) ;

            nvc_value       = switchNCV(md[1], conversion) ;

            
            #println(nuts3)
            #println(entsoe_zone)
            if ismissing(quantity)
                quantity = NaN ;
            end
            sub_pos         = filter(row -> row.type_id==type_id, codedef) ;
            cat_id          = sub_pos.cat_id[1] ;
            subcat_id       = sub_pos.subcat_id[1] ;
            #categorie       = codedef[!,"categorie"][sub_pos] ;
            #subcategorie    = codedef[!,"subcategorie"][sub_pos] ;
            #type_name       = codedef[!,"type_name"][sub_pos] ;
            #short_name      = codedef[!,"short_name"][sub_pos] ;
            scenario        = md[1] ;
            if includeyear
                year        = 2018 ; 
            end
            unit            = "PJ" ;   
            NCV             = nvc_value.quantity[1] ;     
            if cmp(unit,"euro")==0
                unit = "euro/GJ";
            end
            if cmp(scenario,"high") == 0
                querysce = "ENS_High" ;
            elseif cmp(scenario,"medium") == 0
                querysce = "ENS_Med" ;
            elseif cmp(scenario,"low") == 0
                querysce = "ENS_Low" ;
            else
                querysce = "ENS_High" ;
            end
            if ismissing(type_id_temp2.enspreso_code[1])
                # The enspresso category was not defined for this variable or scenario. Take the default value
                costsdb = type_id_temp2.roadside_costs[1] / exchange_rate ;
            else
                en_cost_nuts2 = query_db_enspresso_costs(tablenamein="cost_nuts2",year=2020, nuts=nuts2, scenario=querysce,E_Comm=type_id_temp2.enspreso_code[1],B_Com=type_id_temp2.enspreso_name[1]) ;
                en_cost_nuts0 = query_db_enspresso_costs(tablenamein="cost_nuts0",year=2020, nuts=nuts0, scenario=querysce,E_Comm=type_id_temp2.enspreso_code[1]) ;
                if isempty(en_cost_nuts2)
                    # If no values are available in the nuts2 file, llok into the nuts0 file
                    if isempty(en_cost_nuts0)
                        logger_fun("No value in the nuts2 and nuts 0 file. Allocating NaN. Commodity: " * type_id_temp2.enspreso_code[1] * " ; " * type_id_temp2.enspreso_name[1] * " ; " * "NUTS0="  * nuts0 * " ; " * "NUTS2=" * nuts2 )
                        costsdb = NaN ;
                    else
                        # Check that the costs exist for the nuts0 defined
                        costsdb = en_cost_nuts0.potential[1] ;
                        if costsdb == 0
                            costsdb = NaN ;
                            logger_fun("Found in nuts0 file but value was 0, allocation NaN. Commodity: " * type_id_temp2.enspreso_code[1] * " ; " * type_id_temp2.enspreso_name[1] * " ; " * "NUTS0="  * nuts0)    
                        else
                            #logger_fun("Found in nuts0 file. Allocating nuts0 value. Commodity: " * type_id_temp2.enspreso_code[1] * " ; " * type_id_temp2.enspreso_name[1] * " ; " * "NUTS0="  * nuts0)                                                
                        end
                    end
                else
                    # Check that the costs exist for the nuts2 defined
                    costsdb = en_cost_nuts2.potential[1] ;
                    #logger_fun("Found in nuts2 file. Allocating nuts2 value. Commodity: " * type_id_temp2.enspreso_code[1] * " ; " * type_id_temp2.enspreso_name[1] * " ; " * "NUTS0="  * nuts0)     
                    # In case the costs input in the nuts2 file is 0, look for the values in the nuts0 database
                    if costsdb == 0
                        if isempty(en_cost_nuts0)
                            #logger_fun("Found in nuts2 file but value was 0, not found in nuts0 file. Allocating NaN. Commodity: " * type_id_temp2.enspreso_code[1] * " ; " * type_id_temp2.enspreso_name[1] * " ; " * "NUTS0="  * nuts0 * " ; " * "NUTS2=" * nuts2 )     
                            costsdb = NaN ;
                        else
                            # Check that the costs exist for the nuts2 defined
                            costsdb = en_cost_nuts0.potential[1] ;
                            if costsdb == 0
                                costsdb = NaN ;
                                logger_fun("Found in nuts2 file but value was 0. Found in nuts0 file but value was 0, allocation NaN. Commodity: " * type_id_temp2.enspreso_code[1] * " ; " * type_id_temp2.enspreso_name[1] * " ; " * "NUTS0="  * nuts0)    
                            else
                                #logger_fun("Found in nuts2 file but value was 0, found in nuts0 file. Allocating nuts0 value. Commodity: " * type_id_temp2.enspreso_code[1] * " ; " * type_id_temp2.enspreso_name[1] * " ; " * "NUTS0="  * nuts0)                                                
                            end
                        end
                    end
                end
            end
            #
            #println([type_id  nuts3   nuts2   nuts1   nuts0   quantity  cat_id  subcat_id scenario  year   unit entsoe_zone])
            #println(       [type_id  nuts3   nuts2   nuts1   nuts0   quantity  cat_id  subcat_id  categorie  subcategorie  type_name  short_name  scenario  year   unit])
            if includeyear
                push!(finaldf, [type_id  nuts3   nuts2   nuts1   nuts0   quantity  cat_id  subcat_id scenario  year   unit NCV entsoe_zone conversion.cat_aggreg[1] costsdb])
            else
                push!(finaldf, [type_id  nuts3   nuts2   nuts1   nuts0   quantity  cat_id  subcat_id scenario  unit NCV entsoe_zone conversion.cat_aggreg[1] costsdb])
            end
        end
        return finaldf
    end
    # Add Switzerland into the database
    function refined_ener_switzerland_db(file, subfolder,filesep, entsoe_nuts, codedef, conversion_s2biom, conversion_CH,includeyear)
        # Start the code
        println("   ... creating the table in the database")
        if includeyear
            db_biomass = df_param() ;
        else
            db_biomass = df_param_noyear() ;
        end
        #db_biomass = copy(temp_db_biomass) ;
        b          = copy(db_biomass)    ; 
        # Load the equivalence table
        println("   ... Load the equivalence table")
        CH_nuts = CSV.read("CH_nuts.csv", DataFrame; delim = ';')
        for nfile in eachindex(file)
            if cmp(file[nfile], "biomasspotentials_cantonal-level.xlsx") == 0
                println("   ... Load the excel table")
                xf = XLSX.readxlsx(subfolder * filesep * file[nfile]) ;
                #println(file[nfile]);
                for n in eachindex(xf.workbook.sheets)
                    md = split(xf[n].name, "_") ;
                    # Only consider the sheet with mass data "dm"
                    if length(md) > 1
                        if cmp(md[2], "GJ") == 0
                            if cmp(md[1], "TheoreticalPotential") == 0 # tech is used only for agrictulture and waste, HIGH for forestry
                                println("   ... importing the high scenario")
                                md[1] = "high" ;
                                convert = "all" ;
                            elseif cmp(md[1], "SustainablePotential") == 0
                                println("   ... importing the medium scenario")
                                md[1] = "medium" ;
                                convert = "all" ;
                                # In this case, duplicate the values to create the low scenario
                            else
                                continue ;
                            end
                            sh = xf[n]   ;
                            m  = sh[:]   ;
                            println("   ... loading the table in a dataframe")
                            df = DataFrame([something.(col, missing) for col in eachcol(m[7:32, 2:end])], Symbol.(m[6,2:end]), makeunique=true) ;
                            df1  = DataFrame() ;
                            foreach(
                                        x->all(ismissing, df[!, x]) ? nothing : df1[!, x] = df[!, x],
                                        propertynames(df)
                                    )
                            #println(df1)
                            if cmp(md[1],"medium") == 0
                                # create a loop to do it twice, once for the medium and once for the low profile
                                println("   ... importing the medium scenario into the database")
                                db_biomass = parse2tableCH(b, df1, CH_nuts.nuts, codedef, md, entsoe_nuts, convert, conversion_s2biom,conversion_CH, includeyear)  
                                md[1] = "low" ;
                                println("   ... importing the low scenario into the database")
                                db_biomass = parse2tableCH(b, df1, CH_nuts.nuts, codedef, md, entsoe_nuts, convert, conversion_s2biom,conversion_CH, includeyear)  
                            else
                                println("   ... importing the scenario into the database")
                                db_biomass = parse2tableCH(b, df1, CH_nuts.nuts, codedef, md, entsoe_nuts, convert, conversion_s2biom,conversion_CH, includeyear)  
                            end
                        end        
                    end  
                end
            end
        end   
        return  db_biomass
    end
    # Load Enspresso data
    function enspresso_costs(enspresso_db)
        xf = XLSX.readxlsx(enspresso_db) ;
        sheetcost = "COST - NUTS2 BioCom" ;
        df_enspresso_cost_nuts2=[] ;
        for n in eachindex(xf.workbook.sheets)            
            if cmp(xf[n].name, sheetcost) == 0
                m = xf[n][:,1:8] ;
                df_enspresso_cost_nuts2 = DataFrame(m[2:end,:],m[1,:]) ;
            end
        end
        # Collect the cost details for NUTS0
        sheetcost = "COST - NUTS0 EnergyCom" ;
        df_enspresso_cost_nuts0=[] ;
        for n in eachindex(xf.workbook.sheets)            
            if cmp(xf[n].name, sheetcost) == 0
                m = xf[n][:,1:6] ;
                df_enspresso_cost_nuts0 = DataFrame(m[2:end,:],m[1,:]) ;
            end
        end
        cost_nuts2_db = copy(df_enspresso_cost_nuts2) ;
        cost_nuts0_db = copy(df_enspresso_cost_nuts0) ;
        rename!(cost_nuts2_db,[:year,:scenario,:nuts0,:nuts2,:E_Comm,:B_Com,   :potential, :unit] ) #[:"B-Com", :NUTS2, :Year, :Scenario, :NUTS0, :"E-Comm", :"NUTS2 Potential available by Bio Commodity", :Units] .=> )
        rename!(cost_nuts0_db,[:year,:scenario,:nuts0,:E_Comm,:potential, :unit])
        # create new database or open existing database if it exists
        db_b = SQLite.DB("enspresso_costs.db")
        # Initialise the database 
        SQLite.execute(db_b, "DROP TABLE IF EXISTS cost_nuts2")
        SQLite.execute(db_b, "DROP TABLE IF EXISTS cost_nuts0")
        SQLite.execute(db_b, "CREATE TABLE cost_nuts2
                            (year INTEGER, scenario TEXT, nuts0 TEXT, nuts2 TEXT, 
                            E_Comm TEXT, B_Com TEXT, potential REAL, unit TEXT)
                            ")
        SQLite.execute(db_b, "CREATE TABLE cost_nuts0
                            (year INTEGER, scenario TEXT, nuts0 TEXT, 
                            E_Comm TEXT, potential REAL, unit TEXT)
                            ")
        SQLite.load!(cost_nuts2_db, db_b, "cost_nuts2") ;
        SQLite.load!(cost_nuts0_db, db_b, "cost_nuts0") ;
    end
    # Enspresso conversion
    function enspresso_conversion(enspresso_db, codedef, countryalpha2, sheetin, enspresso_conv, entsoe_nuts,includeyear,conversion_s2biom)
        if includeyear
            df = df_param() ;
        else
            df = df_param_noyear() ;
        end
        # Collect the data from selected country
        xf = XLSX.readxlsx(enspresso_db) ;
        validscenario = ["ENS_High", "ENS_Low", "ENS_Med"] ;

        # Collect the cost details for NUTS2
        df_enspresso_cost_nuts2 = query_db_enspresso_costs(tablenamein="cost_nuts2") ;
        df_enspresso_cost_nuts0 = query_db_enspresso_costs(tablenamein="cost_nuts0") ;

        for n in eachindex(xf.workbook.sheets)
            if cmp(xf[n].name, sheetin) == 0
                m = xf[n][:,1:8] ;
                df_enspresso = DataFrame(m[2:end,:],m[1,:]) ;
                # Loop through the years
                allyears = unique(df_enspresso.Year) ;
                # Loop through the scenario
                allscenario = unique(df_enspresso.Scenario) ;
                # Loop through each alphacountry
                for key in countryalpha2, scenarion in eachindex(allscenario), yearn in eachindex(allyears)
                    countryalpha = key.first ;
                    if !includeyear
                        if allyears[yearn] != 2020
                            #println(allyears[yearn])
                            continue ;
                        end
                    end
                    if allscenario[scenarion] in validscenario
                        #Go through each row and add it to the DataFrame
                        subdf = subset(df_enspresso, :NUTS0 => ByRow(==(countryalpha)), :Scenario => ByRow(==(allscenario[scenarion])), :Year => ByRow(==(allyears[yearn]))) ;
                        subdf_cost_nuts2 = subset(df_enspresso_cost_nuts2, :nuts0 => ByRow(==(countryalpha)), :scenario => ByRow(==(allscenario[scenarion])), :year => ByRow(==(allyears[yearn]))) ;
                        #subdf_cost_nuts2 = query_db_enspresso_costs(tablenamein="cost_nuts2",year=allyears[yearn], nuts=countryalpha, scenario=allscenario[scenarion])
                        subdf_cost_nuts0 = subset(df_enspresso_cost_nuts0, :nuts0 => ByRow(==(countryalpha)), :scenario => ByRow(==(allscenario[scenarion])), :year => ByRow(==(allyears[yearn]))) ;
                        #subdf_cost_nuts0 = query_db_enspresso_costs(tablenamein="cost_nuts0",year=allyears[yearn], nuts=countryalpha, scenario=allscenario[scenarion])
                        if ~isempty(subdf)
                            # This means the dataset exists
                            for rowin in eachrow( subdf )
                                # do something with row which is of type DataFrameRow

                                subd_conv           = subset(enspresso_conv, :enspreso_code => ByRow(==(rowin."E-Comm")), :enspreso_name => ByRow(==(rowin."B-Com")))
                                subd_conv_costNUTS2 = subset(subdf_cost_nuts2, :"E_Comm" => ByRow(==(rowin."E-Comm")), :"B_Com" => ByRow(==(rowin."B-Com"))) ;
                                subd_conv_costNUTS0 = subset(subdf_cost_nuts0, :"E_Comm" => ByRow(==(rowin."E-Comm"))) ;
                                if isempty(subd_conv)
                                    println(subd_conv) ;println(rowin);
                                end
                                cat = subd_conv.MOPO_db_cat_id[1] ;
                                nuts3 = "-" ;
                                nuts2 = rowin.NUTS2 ;
                                if length(nuts2) == 2
                                    # This means that there is a mistake in the enspreso database and needs to be fixed country by country
                                    if cmp(nuts2, "ME") == 0 || cmp(nuts2, "RS") == 0 || cmp(nuts2, "UA") == 0 || cmp(nuts2, "AL") == 0 || cmp(nuts2, "BA") == 0
                                        nuts2 = nuts2*"11"
                                    elseif cmp(nuts2, "MK") == 0 || cmp(nuts2, "XK") == 0
                                        nuts2 = nuts2*"00"
                                    end
                                end
                                nuts0 = rowin.NUTS0 ;
                                if cmp(rowin.NUTS2,"-") == 0
                                    nuts1 = rowin.NUTS2 ;
                                    entsoe_zone = rowin.NUTS0 ;
                                else
                                    nuts1 = rowin.NUTS2[1:end-1] ;
                                    #println(nuts1)
                                    a = filter(row -> row.nuts2==nuts2, entsoe_nuts) ;
                                    #println(a)
                                    entsoe_zone = a.bidding_zone[1] ;
                                end

                                # Check that the costs exist
                                if isempty(subd_conv_costNUTS2)
                                    # If no values are available in the nuts2 file, llok into the nuts0 file
                                    if isempty(subd_conv_costNUTS0)
                                        logger_fun("No value in the nuts2 and nuts 0 file. Allocating NaN. Commodity: " * rowin."E-Comm" * " ; " * rowin."B-Com" * " ; " * countryalpha * " ; " * "NUTS0="  * nuts0 * " ; " * "NUTS2=" * nuts2 )
                                        costsdb = NaN ;
                                    else
                                        # Check that the costs exist for the nuts2 defined
                                        aaa = subset(subd_conv_costNUTS0,:nuts0 =>ByRow(==(rowin.NUTS0))) ;
                                        if isempty(aaa)
                                            costsdb = subd_conv_costNUTS0[1,:]."potential" ;
                                        else
                                            costsdb = aaa[1,:]."potential" ;
                                        end
                                        if costsdb == 0
                                            costsdb = NaN ;
                                            logger_fun("Found in nuts0 file but value was 0, allocation NaN. Commodity: " * rowin."E-Comm" * " ; " * rowin."B-Com" * " ; " * countryalpha * " ; " * "NUTS0="  * nuts0)    
                                        else
                                            #logger_fun("Found in nuts0 file. Allocating nuts0 value. Commodity: " * rowin."E-Comm" * " ; " * rowin."B-Com" * " ; " * countryalpha * " ; " * "NUTS0="  * nuts0)                                                
                                        end
                                    end
                                else
                                    # Check that the costs exist for the nuts2 defined
                                    aaa = subset(subd_conv_costNUTS2,:nuts2 =>ByRow(==(rowin.NUTS2))) ;
                                    if isempty(aaa)
                                        costsdb = subd_conv_costNUTS2[1,:]."potential" ;
                                    else
                                        costsdb = aaa[1,:]."potential" ;
                                    end
                                    #logger_fun("Found in nuts2 file. Allocating nuts2 value. Commodity: " * rowin."E-Comm" * " ; " * rowin."B-Com" * " ; " * countryalpha * " ; " * "NUTS0="  * nuts0)     
                                    # In case the costs input in the nuts2 file is 0, look for the values in the nuts0 database
                                    if costsdb == 0
                                        if isempty(subd_conv_costNUTS0)
                                            logger_fun("Found in nuts2 file but value was 0, not found in nuts0 file. Allocating NaN. Commodity: " * rowin."E-Comm" * " ; " * rowin."B-Com" * " ; " * countryalpha * " ; " * "NUTS0="  * nuts0 * " ; " * "NUTS2=" * nuts2 )     
                                            costsdb = NaN ;
                                        else
                                            # Check that the costs exist for the nuts2 defined
                                            aaa = subset(subd_conv_costNUTS0,:nuts0 =>ByRow(==(rowin.NUTS0))) ;
                                            if isempty(aaa)
                                                costsdb = subd_conv_costNUTS0[1,:]."potential" ;
                                            else
                                                costsdb = aaa[1,:]."potential" ;
                                            end  
                                            if costsdb == 0
                                                costsdb = NaN ;
                                                logger_fun("Found in nuts2 file but value was 0. Found in nuts0 file but value was 0, allocation NaN. Commodity: " * rowin."E-Comm" * " ; " * rowin."B-Com" * " ; " * countryalpha * " ; " * "NUTS0="  * nuts0)    
                                            else
                                                #logger_fun("Found in nuts2 file but value was 0, found in nuts0 file. Allocating nuts0 value. Commodity: " * rowin."E-Comm" * " ; " * rowin."B-Com" * " ; " * countryalpha * " ; " * "NUTS0="  * nuts0)                                                
                                            end
                                        end
                                    end
                                end

                                
                                if includeyear
                                    year  = allyears[yearn] ;
                                end
                                if cmp(allscenario[scenarion], "ENS_High")==0 scenario = "high" ;
                                elseif cmp(allscenario[scenarion], "ENS_Low")==0 scenario = "low" ;
                                elseif cmp(allscenario[scenarion], "ENS_Med")==0 scenario = "medium" ;
                                else logger_fun("their is a problem"); return ; end
                                unit = subdf.Unit[1] ;
                                quantity = rowin."NUTS2 Potential available by Bio Commodity" ;
                                NCV = NaN ;
                                # Depending on the size of the code, we look in different database
                                if cat < 100
                                    subs = filter(row -> row.cat_id==cat, codedef) ;
                                    # then use the cat_id
                                    type_id = cat * 100 ;
                                    subcat_id = cat * 10 ;
                                    cat_id = cat ;  
                                    maincat = subs.cat_aggreg[1] ;
                                    conversion = conversion_s2biom[findmin(abs.(conversion_s2biom.type_id.-type_id))[2],:]
                                    try
                                        nvc_value        = switchNCV(scenario, DataFrame(conversion)) ;
                                        NCV = nvc_value.quantity[1] ;
                                    catch
                                        
                                        println(conversion)
                                        println(scenario)
                                        NCV        = NaN ;
                                    end
                                elseif  cat < 1000
                                    subs = filter(row -> row.subcat_id==cat, codedef) ;
                                    # use the suprintln("cat < 100")bcat_id
                                    type_id = cat * 10  ;
                                    subcat_id = cat ;
                                    cat_id = subs.cat_id[1] ;  
                                    maincat = subs.cat_aggreg[1] ;
                                    conversion = conversion_s2biom[findmin(abs.(conversion_s2biom.type_id.-type_id))[2],:]
                                    try
                                        nvc_value        = switchNCV(scenario, DataFrame(conversion)) ;
                                        NCV = nvc_value.quantity[1] ;
                                    catch
                                        println("cat < 1000")
                                        println(conversion)
                                        println(scenario)
                                        NCV        = NaN ;
                                    end
                                else 
                                    subs = filter(row -> row.type_id==cat, codedef) ;
                                    # use the type_id
                                    type_id = cat ;
                                    subcat_id = subs.subcat_id[1] ;
                                    cat_id = subs.cat_id[1] ;
                                    maincat = subs.cat_aggreg[1] ;
                                    conversion = filter(row -> row.type_id==type_id, conversion_s2biom)
                                    try
                                        nvc_value        = switchNCV(scenario, conversion) ;
                                        NCV = nvc_value.quantity[1] ;
                                    catch
                                        println("normal")
                                        println(conversion)
                                        println(scenario)
                                        NCV        = NaN ;
                                    end
                                end
                                #println( [type_id  nuts3   nuts2   nuts1   nuts0   quantity  cat_id  subcat_id scenario  year   unit  NCV entsoe_zone maincat])
                                if includeyear
                                    push!(df, [type_id  nuts3   nuts2   nuts1   nuts0   quantity  cat_id  subcat_id scenario  year   unit NCV entsoe_zone maincat costsdb])
                                else
                                    push!(df, [type_id  nuts3   nuts2   nuts1   nuts0   quantity  cat_id  subcat_id scenario  unit NCV entsoe_zone maincat costsdb])
                                end
                           end
                        end
                    end
                end
            end
        end
        return df
    end
    if ~isnothing(update)
        # Initialise the enspresso cost database
        enspresso_costs(enspresso) ;
    end
    biomass_mass_mopo = refined_mass_db(file, subfolder,filesep, entsoe_nuts, codedef, conversion_s2biom,includeyear) ;
    biomass_ener_s2biom = refined_ener_db(biomass_mass_mopo, conversion_s2biom,entsoe_nuts,includeyear) ;
    println("... s2biom database uploaded to the database")
    biomass_ener_CH   = refined_ener_switzerland_db(fileCH, subfolderCH,filesep, entsoe_nuts, codedef, conversion_s2biom, conversion_CH,includeyear)
    println("... switzerland database uploaded to the database")
    biomass_ener_mopo = vcat(biomass_ener_s2biom, biomass_ener_CH, cols=:union) ;

    # Method for getting the data out from enspresso database to s2biom compatible
    sheetname = "ENER - NUTS2 BioCom E" ;
    biomassenspresso = enspresso_conversion(enspresso, codedef, enspressocountry, sheetname, enspresso_mopo, entsoe_nuts,includeyear,conversion_s2biom)
    println("... enspreso database uploaded to the database")
    biomass_ener_mopo = vcat(biomass_ener_mopo, biomassenspresso, cols=:union) ;  

    # Collect data for manure and sludge from enspresso for countries
    d = Dict("AL"=>"Albania", "AT"=>"Austria", "BA"=>"Bosnia", "BE"=>"Belgium", "BG"=>"Bulgaria", "CY"=>"Cyprus", "CZ"=>"Czech", "DE"=>"Germany", "DK"=>"Denmark", "EE"=>"Estonia", "EL"=>"Greece", "ES"=>"Spain", "FI"=>"Finland", "FR"=>"France", "HR"=>"HR", "HU"=>"Hungary", "IE"=>"Ireland", "IS"=>"Iceland", "IT"=>"Italy", "LT"=>"Lithuania", "LU"=>"Luxemburg", "LV"=>"Latvia", "ME"=>"ME", "MK"=>"Macedonia", "MT"=>"Montenegro", "NL"=>"Netherlands", "PL"=>"Poland", "PT"=>"Portugal", "RO"=>"Romania", "RS"=>"Serbia", "SE"=>"Sweden", "SI"=>"Slovenia", "SK"=>"Slovakia", "UK"=>"United Kindgom")
    biomassenspresso = enspresso_conversion(enspresso, codedef, d, sheetname, enspresso_mopo, entsoe_nuts,includeyear, conversion_s2biom) ;
    biogas_subset = filter(row -> row.cat_id==61,biomassenspresso) ;
    biomass_ener_mopo = vcat(biomass_ener_mopo, biogas_subset, cols=:union) ; 

    CSV.write("db_biomass_mass_mopo.csv", biomass_mass_mopo) ;
    ## Convert to energy data 
    CSV.write("db_biomass_ener_mopo.csv", biomass_ener_mopo) ;

    dbcsv = CSV.read("db_biomass_ener_mopo.csv", DataFrame);
    print(typeof(db_biomass_sqlite))
    SQLite.load!(dbcsv, db_biomass_sqlite, "biomass_mass") ;
    return biomass_ener_mopo    
end