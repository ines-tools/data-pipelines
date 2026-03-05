using HTTP, Downloads, CSV, DataFrames

# Check if the folders exist, if it does, asks if re-downloading the results if necessary. 
# If it does not exists, create the folder and download the list of excels from the online repo. If the repo is not available, then return an error and redirect the user to some long term repo
# where to get the filesep

function Fetch_files(url::String, path::String, filename::String)
    println("\n url source: " * url * " --> file saved in: " * path * filename)
    project_file = Downloads.download(url, path * filename)
end

function load_txt(Pathfile::String, list::String, case::String, url_in::String)
    for word in eachline(list)
        filename = word
        if !isfile(joinpath(Pathfile, filename))
            print("         ... $case: Downlading the missing file --> $filename")
            url = url_in * filename
            Fetch_files(url, pwd() * "\\" * Pathfile * "\\", filename)
        end
    end
end

function load_csv(file::String, Pathfile::String, case::String)
    data = CSV.read(file, DataFrame; delim = ';')
    for (i, row) in enumerate( eachrow( data ) ) 
        filename = data.file[i]
        if !isfile(joinpath(Pathfile, filename))
            print("         ... $case: Downlading the missing file --> $filename")
            url = data.url[i] * data.file[i]
            Fetch_files(url, pwd() * "\\" * Pathfile * "\\", data.file[i])
        end
    end
end

function checkFiles(Pathfile::String, list::String, url_in::String, case::String)
    a = splitext(list)
    if !isdir(Pathfile)
        mkdir(Pathfile)
        if cmp(a[2], ".txt") == 0
            load_txt(Pathfile, list, case, url_in)
        elseif cmp(a[2], ".csv") == 0
            load_csv(list, Pathfile, case)
        else
            @warn  "The file is not defined, so far accept only .txt or .csv files"
            return
        end
    else
        println("$case: folder already exists")
        println("     ... $case: Checking that all files are there")
        if cmp(a[2], ".txt") == 0
            load_txt(Pathfile, list, case, url_in)
        elseif cmp(a[2], ".csv") == 0
            load_csv(list, Pathfile, case)
        else
            @warn  "The file is not defined, so far accept only .txt or .csv files"
            return
        end
        println("     ... $case: All files are present")
    end
end
    # S2Biom
s2biom_Url = "https://s2biom.wenr.wur.nl/doc/data/"
s2biom_PathFile = "cost_supply_data"
xlsx_list = "s2biom_files.txt"

# Swiss database
CH_PathFile = "swiss database"
csv_list = "CH_files.csv"

# Get the Enspreso file
enspreso_Url = "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ENSPRESO/"
enspreso_PathFile = "enspreso"
enspreso_list = "enspreso_files.txt"

# Check S2Biom files
checkFiles(s2biom_PathFile, xlsx_list, s2biom_Url, "s2biom database")    

# Check for the swiss database
checkFiles(CH_PathFile, csv_list, "", "Switzerland data")      
    
# Check for the enspreso database
checkFiles(enspreso_PathFile, enspreso_list, enspreso_Url, "ENSPRESO database")  




