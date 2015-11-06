defmodule Cafex.Mixfile do
  use Mix.Project

  def project do
    [app: :cafex,
     version: "0.0.1",
     elixir: "~> 1.0",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps,
     name: "Cafex",
     source_url: "https://github.com/upyun/cafex",
     homepage_url: "http://cafex.github.com/",
     docs: [extras: ["README.md"]],
     dialyzer: [flags: ["-Werror_handling", "-Wrace_conditions", "-Wunderspecs"]]]
  end

  # Configuration for the OTP application
  #
  # Type `mix help compile.app` for more information
  def application do
    [applications: [:logger, :erlzk],
     mod: {Cafex.Application, []}]
  end

  # Dependencies can be Hex packages:
  #
  #   {:mydep, "~> 0.3.0"}
  #
  # Or git/path repositories:
  #
  #   {:mydep, git: "https://github.com/elixir-lang/mydep.git", tag: "0.1.0"}
  #
  # Type `mix help deps` for more examples and options
  defp deps do
    [{:earmark, "~> 0.1.17", only: :dev},
     {:ex_doc,  "~> 0.10.0", only: :dev},
     {:erlzk,   git: "ssh://gitlab@gitlab.widget-inc.com:65422/huaban-core/erlzk.git", branch: "develop"}]
  end
end
