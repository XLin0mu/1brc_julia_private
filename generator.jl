


using Dates

function formatByte(bytes::Number)
    if bytes < 0
        return "negative bytes"
    end

    units = ["Bytes", "KiB", "MiB", "GiB", "TiB", "PiB"]
    result_unit = 1

    while bytes >= 1024
        bytes /= 1024
        result_unit += 1
    end

    bytes

    if result_unit <= 6
        return "$(round(bytes, digits=3)) $(units[result_unit])"
    else
        return "$(round(bytes, digits=3)) $(units[6])"
    end
end

function estimate_file_size(weather_station_names, num_rows_to_create)
    """
    Tries to estimate how large a file the test data will be
    """
    total_name_bytes = sum(length.(weather_station_names))
    avg_name_bytes = total_name_bytes / length(weather_station_names)

    # avg_temp_bytes = sum(len(str(n / 10.0)) for n in range(-999, 1000)) / 1999
    avg_temp_bytes = 4.400200100050025

    # add 2 for separator and newline
    avg_line_length = avg_name_bytes + avg_temp_bytes + 2

    human_file_size = formatByte(num_rows_to_create * avg_line_length)

    println("Estimated max file size is: $human_file_size.")
end

function build_weather_station_name_list()
    file_name = "./weather_stations.csv"

    file_io = open(file_name, "r")

    lines_vec = readlines(file_io)

    station_names = String[]

    for i in 3:length(lines_vec)
        push!(station_names, split(lines_vec[i], ';')[1])
    end

    return [i for i in Set(station_names)]
end

function build_test_data(weather_station_names, num_rows_to_create)
    start_time = now()
    coldest_temp = -99.9
    hottest_temp = 99.9
    station_names_10k_max = rand(weather_station_names, 10_000)
    batch_size = 10000 # instead of writing line by line to file, process a batch of stations and put it to disk
    chunks = ceil(Int, num_rows_to_create / batch_size)
    println("Building test data...")

    try
        rm(FILE_NAME)
    catch
    end

    try
        touch(FILE_NAME)
        open(FILE_NAME, "w") do file_io
            progress = 0
            for chunk in 1:chunks

                batch = rand(station_names_10k_max, batch_size)
                prepped_deviated_batch = join([
                    "$station;$(ceil(rand()*(hottest_temp - coldest_temp) + coldest_temp, digits=1))\n" for station in batch
                ])
                write(file_io, prepped_deviated_batch)

                # Update progress bar every 1%
                if ceil(Int, chunk / chunks * 100) != progress
                    progress = ceil(Int, chunk / chunks * 100)
                    bars = ('='^(progress÷2)*' '^50)[1:50]
                    print("\r[$bars] $progress%")
                end
            end
        end
        println("")
    catch e
        println("Something went wrong. Printing error info and exiting...")
        @show e
    end

    end_time = now()
    elapsed_time = end_time - start_time
    file_size = stat(FILE_NAME).size
    human_file_size = formatByte(file_size)

    println("Test data successfully written to 1brc/data/measurements.txt")
    println("Actual file size:  $human_file_size")
    println("Elapsed time: $(elapsed_time.value/1000) Seconds")
end

function generate_data(amount::Int)
    num_rows_to_create = amount
    weather_station_names = build_weather_station_name_list()
    estimate_file_size(weather_station_names, num_rows_to_create)
    build_test_data(weather_station_names, num_rows_to_create)
    println("Test data build complete.")
end

#global FILE_NAME = "./measurements.txt"
#generate_data(1_000_000_000)

#global FILE_NAME = "./measurements_10k.txt"
#generate_data(10_000)

#global FILE_NAME = "./measurements_10m.txt"
#generate_data(10_000_000)

global FILE_NAME = "./measurements_100m.txt"
generate_data(100_000_000)
