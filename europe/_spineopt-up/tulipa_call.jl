using TulipaClustering
using TulipaIO
using DataFrames
using DuckDB
using Distances
using CSV
using Debugger

function get_data(input_folder,year)
  dir = joinpath(input_folder, "profiles")
  con = DBInterface.connect(DuckDB.DB)
  TulipaIO.create_tbl(con, joinpath(dir, string("profiles","_",year,".csv")); name = "profiles")
  return DBInterface.execute(con, "SELECT * FROM profiles") |> DataFrame
end

input_folder = joinpath(@__DIR__, "")
rp_df = DataFrame()
for alternative in ["wy2009"]
    println("--------",alternative,"--------")

    println("Importing Data")
    clustering_data = get_data(input_folder,string(alternative))
    println("Splitting Periods")
    split_into_periods!(clustering_data; period_duration = 24)

    println("Finding Representative Periods")
    clustering_result = find_representative_periods(
        clustering_data,
        12;
        drop_incomplete_last_period = false,
        method = :convex_hull, # k_means, k_medoids, convex_hull, convex_hull_with_null, conical_hull
        distance = Euclidean(), #Any distance from Distances.jl e.g., SqEuclidean(), or CosineDist()
        # init = :kmcen,
    )

    println("Saving Representative Periods ",alternative, clustering_result.auxiliary_data.medoids)
    #medoids = clustering_result.auxiliary_data.medoids
    rp_df[!,alternative] = clustering_result.auxiliary_data.medoids

    
    println("Calculating Weights")
    TulipaClustering.fit_rep_period_weights!(
      clustering_result;
      weight_type = :convex, # :dirac, :convex, :conical_bounded
      niters = 1000,
      learning_rate = 1e-3
    )

    println("Writting Results")
    weights = TulipaClustering.weight_matrix_to_df(clustering_result.weight_matrix)
    CSV.write(string("results/weights","_",alternative,".csv"),weights)
    
end
CSV.write(string("results/representative_periods.csv"),rp_df);