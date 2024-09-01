
using Base.Threads
using Dates

function format_row(p)
    r(num) = round(num; digits=1)
    data = round.((
            p[2][1],
            p[2][2],
            p[2][3] / p[2][4]
        ); digits=1)

    return p[1] * '=' * "$(data[1])/$(data[3]))/$(data[2])"
end

function parse_char2num(str::Base.CodeUnits{UInt8,String}, pointer::Int)
    return str[pointer] - UInt8('0')
end

function parse_temp(ustr::Base.CodeUnits{UInt8,String}, pointer::Int)
    #pointer at ';' + 1
    temp = one(Float64)
    sign = one(Float64)

    if ustr[pointer] == UInt8('-')
        sign = -sign
        pointer += 1
    end

    temp *= parse_char2num(ustr, pointer)
    pointer += 1

    if ustr[pointer] != UInt8('.')
        return (sign * (10 * temp + parse_char2num(ustr, pointer) + 0.1 * parse_char2num(ustr, 2 + pointer)),
            4 + pointer)
    else
        return (sign * (temp + 0.1 * parse_char2num(ustr, 1 + pointer)),
            3 + pointer
        )
    end
end

function parse_name(str::AbstractString, pointer::Int)
    semi = findnext(';', str, pointer)
    return (String(str[pointer:prevind(str, semi)]), 1 + semi)
end

function update_data!(data::Tuple{Float64,Float64,Float64,Int64}, tmp)
    if data[1] < tmp && data[2] > tmp
        return (
            data[1],
            data[2],
            data[3] + tmp,
            1 + data[4]
        )
    elseif tmp < data[1]
        if data[2] > tmp
            return (
                tmp,
                data[2],
                data[3] + tmp,
                1 + data[4]
            )
        else
            return (
                data[1],
                data[2],
                data[3] + tmp,
                1 + data[4]
            )
        end
    else
        return (
            data[1],
            tmp,
            data[3] + tmp,
            1 + data[4]
        )
    end
end

function combine_into_dict!(com_dict::Dict{String,Tuple{Float64,Float64,Float64,Int64}}, sub_text, ustr, len)
    pointer = 1
    while pointer < len
        name, pointer = parse_name(sub_text, pointer)
        tmp, pointer = parse_temp(ustr, pointer)

        data = get!(com_dict, name, (tmp, tmp, tmp, 1))

        data != (tmp, tmp, tmp, 1) && setindex!(com_dict, update_data!(data, tmp), name)
    end
end

function task_assign(sub_text::SubString)

    len = sizeof(sub_text)
    ustr = transcode(UInt8, String(sub_text))

    com_dict = Dict{String,Tuple{Float64,Float64,Float64,Int64}}()
    combine_into_dict!(com_dict, sub_text, ustr, len)

    return com_dict
end

function merge_data(data1::T, data2::T) where {T}
    if data1[1] < data2[1] && data1[2] > data2[2]
        return (
            data1[1],
            data1[2],
            data1[3] + data2[3],
            data1[4] + data2[4]
        )
    elseif data2[1] < data1[1]
        if data1[2] > data2[2]
            return (
                data2[1],
                data1[2],
                data1[3] + data2[3],
                data1[4] + data2[4]
            )
        else
            return (
                data2[1],
                data2[2],
                data1[3] + data2[3],
                data1[4] + data2[4]
            )
        end
    else
        return (
            data1[1],
            data2[2],
            data1[3] + data2[3],
            data1[4] + data2[4]
        )
    end
end

function split_task(text_part, fold)
    text_length = sizeof(text_part)
    block_size = text_length ÷ fold

    split_refs = findnext.('\n', text_part, prevind.(text_part, block_size .* [i for i in 1:fold]))
    split_refs[end] = text_length

    return SubString.(text_part, [1, split_refs[1:end-1] .+ 1...], split_refs)
end

function form_com_dict(com_dict)
    return '{' * join(format_row.(sort!([pairs(com_dict)...]; by=p -> p[1])), ", ") * '}'
end

function process_data(text; thread_number=nthreads())

    sub_strs = split_task(text, thread_number);

    com_dicts = fetch.([Threads.@spawn task_assign(sub_strs[i]) for i in 1:thread_number])

    return form_com_dict(mergewith(merge_data, com_dicts...))
end

function get_time(f::Function; pre_run=false)
    pre_run && f()
    Base.GC.gc()
    start_time = now()
    f()
    end_time = now()
    elapsed_time = end_time - start_time

    println("Elapsed time: $(elapsed_time.value/1000) Seconds")
end

function init_data(file_name="./measurements.txt"; fold=1)

    text = ""
    text = open(file_name, "r") do fio
        return read(fio, String)
    end

    #reduce pressure
    if fold != 1
        text = String(split_task(text, fold)[1])
    end
    Base.GC.gc()

    return text
end

function __main__(file_name)
    text = init_data("./measurements.txt")
    #text = init_data("./measurements.txt")
    #text = init_data("./measurements_100m.txt");
    #mask_text = SubString(text, 1, sizeof(text))
    f = () -> process_data(text)
    #performance as 232.887 Seconds with gc
    get_time(f; pre_run=true)
end

#using Chairmarks
#using BenchmarkTools
#using ProfileCanvas