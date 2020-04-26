using Documenter, Wetter

makedocs(;
    modules=[Wetter],
    format=Documenter.HTML(),
    pages=[
        "Home" => "index.md",
    ],
    repo="https://github.com/jlewis91/Wetter.jl/blob/{commit}{path}#L{line}",
    sitename="Wetter.jl",
    authors="Jeremiah Lewis",
    assets=String[],
)
