defmodule Exqlite.MixProject do
  use Mix.Project

  def project do
    [
      app: :exqlite,
      version: "0.1.0",
      elixir: "~> 1.6",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:esqlite, "~> 0.4.0"},
      {:db_connection, "~> 2.2.1"},
      {:ex_doc, "~> 0.21", only: :dev, runtime: false},
      {:temp, "~> 0.4", only: [:dev, :test]},
    ]
  end
end
