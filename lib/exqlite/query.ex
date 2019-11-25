defmodule Exqlite.Query do
  defstruct [
    query: nil,
    statement: nil,
  ]

  def from(s), do: %__MODULE__{query: [s]}

  defimpl DBConnection.Query do
    def parse(query, _opts), do: query

    def describe(query, _opts), do: query

    def encode(_query, params, _opts), do: params

    def decode(query, result, opts) do
      result
    end
  end
end
