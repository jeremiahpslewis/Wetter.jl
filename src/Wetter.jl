module Wetter

greet() = print("Hello World!")

end # module

# using Pkg

# Pkg.activate(".")

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



function read_df_from_gzip_csv(filename)
    return CSV.read(GzipDecompressorStream(open(filename)))
end

function get_station_id(file_name)
    m = match(r"10minutenwerte_TU_([0-9]+)_.*", file_name)
    return String(m.captures[1])
end

# Fetch list of available data files
function fetch_data_file_df()
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

    file_df = DataFrame(file_name = file_list, station_id = get_station_id.(file_list))

    return file_df
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

        return date
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
    # TODO: Sort rows by timestamp
    # Test that the number of NA vals is less than 0.1%
    @assert mean(ismissing(df_full.obs_date_utc)) < 0.001
    mkpath("data/csv")
    write_df_to_gzip_csv(df_full, "data/csv/station_id_" * String(station_id) * ".csv.gz")
    rm(tmp_dir_path, recursive = true, force = true)
end


function download_and_export_data()

    # Download Data
    file_df = fetch_data_file_df()

    station_ids = unique(file_df.station_id)

    # Filter Berlin Tegel...
    # station_ids = filter(x->x == "00430", station_ids)
    # station_ids = station_ids[1:3]
    map(x->download_and_export_data(x, file_df.file_name), station_ids)
end


# download_and_export_data()


using VegaLite
using FilePaths
using DataFramesMeta


file_list = readdir("data/csv", join = true)
file_list = filter(x->occursin("_00430", x), file_list)
df = read_df_from_gzip_csv(file_list[1])



# TODO: inspect number of data points per cell, decide on how to handle missing data...
df_annual_trends = @linq df |>
    where(.!ismissing.(:tt_10) .& .!ismissing.(:obs_date_utc)) |>
    transform(obs_time = Time.(:obs_date_utc), obs_year = year.(:obs_date_utc), obs_day_of_year = dayofyear.(:obs_date_utc), obs_month = month.(:obs_date_utc)) |>
    transform(obs_5_year = fld.(:obs_year, 5) * 5) |>
    where((:obs_time .> Time("20:00"))) |>
    by([:obs_day_of_year, :station_id, :obs_month], median_tt_10 = median(:tt_10), q_25_tt_10 = quantile(:tt_10, 0.25), q_75_tt_10 = quantile(:tt_10, 0.75), pct_days_over_20_c = mean(:tt_10 .> 20)) |>
    orderby(:obs_day_of_year) |>
    select(:station_id, :obs_day_of_year, :obs_month, :median_tt_10, :q_25_tt_10, :q_75_tt_10, :pct_days_over_20_c)

df_annual_trends |>
    @vlplot(
        title = {text = "Berlin Climate Trends: Nightime Temperatures"},
        mark = {:line},
        x = {"obs_day_of_year", title = "Day of Year"},
        y = {"median_tt_10:q"},
        column = "obs_month:O")


# TODO: inspect number of data points per cell, decide on how to handle missing data...
df_time_trends = @linq df |>
    where(.!ismissing.(:obs_date_utc)) |>
    # NOTE: To ensure CET time is displayed independent of browser timezone, time data is specified as UTC, but preconverted to CET
    # Warining: this hack probably fails for dates near daylight savings clock change.
    transform(obs_time_cet = Time.(Dates.format.(astimezone.(ZonedDateTime.(:obs_date_utc, FixedTimeZone("UTC")), tz"Europe/Berlin"), "HH:MM")),
        obs_year = year.(:obs_date_utc),
        obs_month = month.(:obs_date_utc)) |>
    transform(obs_5_year = fld.(:obs_year, 5) * 5) |>
    where(:obs_time_cet .>= Time("20:00"), :obs_month .>= 6, :obs_month .< 9, .!ismissing.(:tt_10)) |>
    by([:obs_time_cet, :obs_5_year, :station_id, :obs_year],
        median_tt_10 = median(:tt_10),
        q_25_tt_10 = quantile(:tt_10, 0.25),
        q_75_tt_10 = quantile(:tt_10, 0.75),
        pct_days_over_20_c = mean(:tt_10 .> 20),
        pct_days_over_25_c = mean(:tt_10 .> 25),
        pct_days_over_30_c = mean(:tt_10 .> 30)) |>
    orderby(:obs_time_cet, :obs_5_year) |>
    select(:station_id,
        obs_time_cet_as_utc_str = Dates.format.(:obs_time_cet, "2000-01-01THH:MM:00") .* "Z",
        :median_tt_10,
        :obs_year,
        :q_25_tt_10,
        :q_75_tt_10,
        obs_5_year = string.(:obs_5_year, "—", :obs_5_year .+ 4),
        :pct_days_over_20_c,
        :pct_days_over_25_c,
        :pct_days_over_30_c)

df_time_trends = DataFrames.stack(df_time_trends,
        [:pct_days_over_20_c,
        :pct_days_over_25_c,
        :pct_days_over_30_c])

p = @where(df_time_trends, :variable .== Symbol("pct_days_over_20_c")) |>
    @vlplot(
        width = 1000,
        height = 1000,
        title = {text = "Berlin Summer Nightime Temperatures", subtitle = "(June — August)"},
        layer = [
        {
            mark = {:area, opacity = 0.15},
            x = {"obs_time_cet_as_utc_str:T", timeUnit = "utchoursminutes", title = "Time of Day (CET)", axis = {tickCount = 4}},
            y = {"q1(value):q", scale = {domain = [0, 1]}, axis = {format = "%", tickCount = 20, labelExpr = "(datum.value * 100) % 10 ? null : datum.label"}, title = "Days over 20°C"},
            y2 = {"q3(value):q", scale = {domain = [0, 1]}, axis = {format = "%", tickCount = 20, labelExpr = "(datum.value * 100) % 10 ? null : datum.label"}, title = "Days over 20°C"},
            # tooltip = {"obs_5_year"},
            color = {"obs_5_year:N", title = "", scale = {scheme = "magma"}}
        },
        {
            mark = {:line, opacity = 1},
            x = {"obs_time_cet_as_utc_str:T", timeUnit = "utchoursminutes", title = "Time of Day (CET)", axis = {tickCount = 4}},
            y = {"median(value):q", scale = {domain = [0, 1]}, axis = {format = "%", tickCount = 20, labelExpr = "(datum.value * 100) % 10 ? null : datum.label"}, title = "Days over 20°C"},
            color = {"obs_5_year:N", title = "", scale = {scheme = "magma"}}
        },
        {
            mark = {:point, opacity = 0.2},
            x = {"obs_time_cet_as_utc_str:T", timeUnit = "utchoursminutes", title = "Time of Day (CET)", axis = {tickCount = 4}},
            y = {"value:q", scale = {domain = [0, 1]}, axis = {format = "%", tickCount = 20, labelExpr = "(datum.value * 100) % 10 ? null : datum.label"}, title = "Days over 20°C"},
            tooltip = {"obs_year"},
            color = {"obs_5_year:N", title = "", scale = {scheme = "magma"}}
        }
        ]
)
# show in absolute terms, difference in number of days per quarter in 1995 vs 2015
# jsave("figure.png", p)


# https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/historical/zehn_min_tu_Beschreibung_Stationen.txt
