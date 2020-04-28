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
for elem = filter(x->isa(x, HTMLElement), link_elem)
    filename = elem.attributes["href"]
    if endswith(filename, "hist.zip")
        r = RemoteFile(url * filename, file = filename, dir = "data")
        download(r)
    end
end

# Next steps:
# Import file as data table
# Export file as parquet
