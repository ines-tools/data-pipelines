using SQLite, DataStructures, DataFrames
    function get_data(;type_id=nothing, nuts=nothing, maincat=nothing, entsoe_nuts=nothing, scenario=nothing, year=nothing)
        dictquery = OrderedDict() ;
        tablename = "biomass_mass" ;
        db_b = SQLite.DB("biomass.db")

        function buildquery(tablename, category, colname, db_b, dictquery)
            if ~isnothing(category)
                if cmp(colname, "nuts") == 0
                    if length(category) == 5
                        colname = "nuts3" ;
                    elseif length(category) == 4
                        colname = "nuts2" ;
                    elseif length(category) == 3
                        colname = "nuts1" ;
                    elseif length(category) == 2
                        colname = "nuts0" ;
                    else
                        println("wrong number of nuts") ;
                        return
                    end
                end
                # Check if the id existin the database
                querytemp = "SELECT DISTINCT $colname FROM $tablename;" ;
                data = DataFrames.DataFrame(SQLite.DBInterface.execute(db_b,querytemp)) ;
                dftid = data[data[!,colname] .== category,:]
                if isempty(dftid)
                    #println("the $colname $category does not exist in the database") ;
                    #println(("valid $colname are: ", data[!,colname])) ;
                    return dictquery
                end
                # rune the query if it exists
                id_query = string(category) ;
                merge!(dictquery,Dict(colname =>id_query))
            end
            return dictquery
        end

        dictquery = buildquery(tablename, type_id, "type_id", db_b, dictquery) ;
        dictquery = buildquery(tablename, maincat, "maincat", db_b, dictquery) ;
        dictquery = buildquery(tablename, nuts, "nuts", db_b, dictquery) ;
        dictquery = buildquery(tablename, entsoe_nuts, "entsoe_nuts", db_b, dictquery) ;
        dictquery = buildquery(tablename, scenario, "scenario", db_b, dictquery) ;
        dictquery = buildquery(tablename, year, "year", db_b, dictquery) ;

        query = "SELECT * FROM $tablename" ;
        if ~isempty(dictquery)
            query = query * " WHERE " ;
            for (k,v) in dictquery
                query = query * k * "='" * v * "' and " ;  
            end
            query = chop(query, head = 0, tail = 5)
            query = query * ";" ;
        end

        data = SQLite.DBInterface.execute(db_b,query) ;
        detailed_df = DataFrames.DataFrame(data);
        return detailed_df
    end

