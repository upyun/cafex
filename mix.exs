defmodule Cafex.Mixfile do
  use Mix.Project

  def project do
    [app: :cafex,
     version: "0.0.1",
     elixir: "~> 1.0",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps,
     dialyzer: [flags: ["-Werror_handling", "-Wrace_conditions", "-Wunderspecs"]]]
  end

  # Configuration for the OTP application
  #
  # Type `mix help compile.app` for more information
  def application do
    [applications: [:logger],
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
    [{:kafka_ex, "~> 0.1.0"}]
  end
end
