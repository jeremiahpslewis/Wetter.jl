module Wetter

greet() = print("Hello World!")

end # module

using RemoteFiles
using HTTP


url = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/historical/"
r = HTTP.request("GET", url)
r.body

using Gumbo


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

file_list = file_list[1:10]

# Download Data
for filename = file_list
    r = RemoteFile(url * filename, file = filename, dir = "data")
    download(r)
end

# Import File as Data Table

using CSV
using ZipFile
using DataFrames
using TimeZones
using Dates

file_list = readdir("data")

file_name = "data/" * file_list[1]

z = ZipFile.Reader(file_name)
txt_file = filter(x->endswith(x.name, ".txt"), z.files)[1]

df = CSV.File(txt_file, delim = ";", skipto = 5, missingstrings = ["-999"], types = Dict(2 => String)) |> DataFrame!

# Next steps:
# Import file as data table
# Document functions
# Split up Into functions
# Subset columns and rename
# Concat all files
# Export file as zipped CSV


# identify the right file in zip
date_ex = df[1, :MESS_DATUM]


const date_format = Dates.DateFormat("yyyymmddHHMM:00:000")




function todate(num)
    date = DateTime(String(date_ex), date_format)
    if year(date) < 2000
        time_zone = tz"Europe/Berlin"
    else
        time_zone = tz"UTC"
    end
    return ZonedDateTime(date, time_zone)
end


df[:obs_date_utc] = todate.(df[!, :MESS_DATUM])
