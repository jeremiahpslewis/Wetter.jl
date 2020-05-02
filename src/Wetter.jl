module Wetter

greet() = print("Hello World!")

end # module

using Statistics
using RemoteFiles
using HTTP
using CSV
using ZipFile
using DataFrames
using TimeZones
using Dates
using Gumbo

const date_format = Dates.DateFormat("yyyymmddHHMM:00:000")

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
function download_data_file(file_name)
    url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/historical/"
    r = RemoteFile(url * file_name, file = file_name, dir = "data")
    download(r)
end

function covert_csv_to_dataframe(file_name)
    file_name = "data/" * file_name
    global z = ZipFile.Reader(file_name) # Declare as global variable to avoid bug: https://github.com/fhs/ZipFile.jl/issues/14
    txt_file = filter(x->endswith(x.name, ".txt"), z.files)[1]

    df = CSV.File(txt_file, delim = ";", missingstrings = ["-999"], types = Dict(2 => String), normalizenames = true) |> DataFrame!
    close(z)


    function todate(date_ex)
        date = DateTime(String(date_ex), date_format)

        # Post-2000 timestamps are in UTC, so all is well in Y2K
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
    rename!(df, Dict(:STATIONS_ID => "station_id", :QN => "qn", :TT_10 => "tt_10", :MESS_DATUM => "raw_date_string"))
    df = df[!, [:station_id, :qn, :tt_10, :obs_date_utc, :raw_date_string]]
    return df
end


# Download Data
# file_list = fetch_data_file_list()
# file_list = file_list[1:100]
# download_data_file.(file_list)


# Import File as Data Table
file_list = readdir("data")
file_list = file_list[1:100]

df_list = covert_csv_to_dataframe.(file_list)
# df_list[3]
df_full = vcat(df_list...)

# Test that the number of NA vals is less than 0.1%
@assert mean(df_full.obs_date_utc .== "NA") < 0.001


# df_list[3]
# reduce(df_list)

# df[!, [:STATIONS_ID]]


# Next steps:
# Import file as data table
# Document functions
# Split up Into functions
# Subset columns and rename
# Concat all files
# Export file as zipped CSV


# for item = df_full[!, :obs_date_utc]
#     try
#         Dates.format(item, "yyyy-mm-dd HH:MM")
#     catch
#         println(item)
#     end
# end


# df_full[df_full.obs_date_utc .== nothing, :raw_date_string]
# df_full
