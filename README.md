# 1brc_julia_private
1brc generator writen by julia, with a baseline provided by my self

best result till now as bellow:

```julia
julia> versioninfo()
Julia Version 1.12.0-DEV.1126
Commit 0622123121 (2024-08-31 11:37 UTC)
Build Info:
  Official https://julialang.org release
Platform Info:
  OS: Windows (x86_64-w64-mingw32)
  CPU: 24 × 13th Gen Intel(R) Core(TM) i7-13700F
  WORD_SIZE: 64
  LLVM: libLLVM-18.1.7 (ORCJIT, alderlake)
Threads: 24 default, 0 interactive, 24 GC (on 24 virtual cores)

julia> text = init_data("./measurements.txt")

julia> using Chairmarks

julia> process_data(text);

julia> Base.GC.gc()

julia> @be process_data(text)
Benchmark: 1 sample with 1 evaluation
       14.019 s (1000170878 allocs: 45.933 GiB, 32.24% gc time, without a warmup)
```
