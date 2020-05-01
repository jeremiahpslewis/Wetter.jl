module Wetter

greet() = print("Hello World!")

end # module

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
    z = ZipFile.Reader(file_name)
    txt_file = filter(x->endswith(x.name, ".txt"), z.files)[1]

    df = CSV.File(txt_file, delim = ";", skipto = 5, missingstrings = ["-999"], types = Dict(2 => String)) |> DataFrame!



    function todate(date_ex)
        date = DateTime(String(date_ex), date_format)
        if year(date) < 2000
            time_zone = tz"Europe/Berlin"
        else
            time_zone = tz"UTC"
        end
        return ZonedDateTime(date, time_zone)
    end


    df[:obs_date_utc] = todate.(df[!, :MESS_DATUM])

    return df
end


# Download Data
file_list = fetch_data_file_list()
file_list = file_list[1:10]
download_data_file.(file_list)


# Import File as Data Table
file_list = readdir("data")
file_list = file_list[1:10]
df_list = covert_csv_to_dataframe.(file_list)


# Next steps:
# Import file as data table
# Document functions
# Split up Into functions
# Subset columns and rename
# Concat all files
# Export file as zipped CSV
