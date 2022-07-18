defmodule Ronex.MixProject do
  use Mix.Project

  def project do
    [
      app: :ronex,
      version: "0.2.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ex_parameterized, "~> 1.3.7"}
    ]
  end
end
