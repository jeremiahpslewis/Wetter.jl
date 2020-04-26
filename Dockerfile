FROM julia:1.4-buster

COPY Project.toml Manifest.toml ./

RUN julia -e 'using Pkg; Pkg.activate("."); Pkg.instantiate(); Pkg.precompile();'

COPY src/startup.jl .julia/config/
