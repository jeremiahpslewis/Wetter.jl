module Wetter

greet() = print("Hello World!")

end # module

using Statistics
using RemoteFiles
using HTTP
using CSV
using DataFrames
using TimeZones
using Dates
using Gumbo
using CodecZlib


# Steps:
# Import file as data table
# Document functions
# Split up Into functions
# Subset columns and rename
# Concat all files
# Export file as zipped CSV

# Compile date format for faster parsing
const date_format = Dates.DateFormat("yyyymmddHHMM:00:000")


function write_df_to_gzip_csv(df, filename)
    open(GzipCompressorStream, filename, "w") do stream
        CSV.write(stream, df)
    end
end


function get_station_id(file_name)
    m = match(r"10minutenwerte_TU_([0-9]+)_.*", file_name)
    return String(m.captures[1])
end

# Fetch list of available data files
function fetch_data_file_list()
    url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/historical/"
    r = HTTP.request("GET", url)


    weather_file_dir = String(r.body)

    weather_file_html  = parsehtml(weather_file_dir)
    link_elem = weather_file_html.root[2][3].children

    file_list = String[]

    for elem = filter(x->isa(x, HTMLElement), link_elem)
        filename = elem.attributes["href"]
        if endswith(filename, "hist.zip")
            push!(file_list, filename)
        end
    end

    return file_list
end

# Given filename, download file
function download_data_file(file_name, data_dir_path)
    url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/historical/"
    r = RemoteFile(url * file_name, file = file_name, dir = data_dir_path)
    download(r)
    file_path = joinpath(data_dir_path, file_name)
    run(`unzip -o $file_path -d $data_dir_path`) # Unzip file using command line

    rm(file_path)
end

function covert_csv_to_dataframe(file_name)
    df = CSV.File(file_name, delim = ";", missingstrings = ["-999"], types = Dict(2 => String), normalizenames = true, select = [:STATIONS_ID, :QN, :TT_10, :MESS_DATUM]) |> DataFrame!

    function todate(date_ex)
        date = DateTime(String(date_ex), date_format)

        # Post-2000 timestamps are in UTC, so all is well starting Y2K, otherwise need to tz convert
        if year(date) < 2000
            date = try
                DateTime(ZonedDateTime(date, tz"Europe/Berlin"), UTC)
            catch e
                if isa(e, AmbiguousTimeError) || isa(e, NonExistentTimeError)
                    missing
                else
                    e
                end
            end
        end

        # Return timestamp or "NA"-string
        if ismissing(date)
            return "NA"
        else
            return Dates.format(date, "yyyy-mm-dd HH:MM")
        end
    end

    df[!, :obs_date_utc] = todate.(df[!, :MESS_DATUM])
    rename!(df, Dict(:STATIONS_ID => "station_id", :QN => "qn", :TT_10 => "tt_10", :MESS_DATUM => "raw_date_string")) # TODO: Figure out how to convert ambiguous and nonexistent timestamps...
    df = df[!, [:station_id, :qn, :tt_10, :obs_date_utc, :raw_date_string]]
    return df
end


function download_and_export_data(station_id, file_list)
    tmp_dir_path = mktempdir(tempdir(); prefix = "jl_", cleanup = true)
    # remove raw data directory (zip dir...)
    file_list = filter(x->occursin("_TU_" * station_id, x), file_list)

    download_data_file.(file_list, (tmp_dir_path,))
    # Import File as Data Table
    file_list = readdir(tmp_dir_path, join = true)
    df_list = covert_csv_to_dataframe.(file_list)
    df_full = vcat(df_list...)

    # Test that the number of NA vals is less than 0.1%
    @assert mean(df_full.obs_date_utc .== "NA") < 0.001
    mkpath("data/csv")
    write_df_to_gzip_csv(df_full, "data/csv/station_id_" * String(station_id) * ".csv.gz")
    rm(tmp_dir_path, recursive = true, force = true)
end


function download_and_export_data()

    # Download Data
    file_list = fetch_data_file_list()

    file_df = DataFrame(file_name = file_list, station_id = get_station_id.(file_list))

    station_ids = unique(file_df.station_id)
    # station_ids = station_ids[1:3]
    for station_id in station_ids
        download_and_export_data(station_id, file_df.file_name)
    end
end


download_and_export_data()
