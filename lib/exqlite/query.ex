defmodule Exqlite.Query do
  defstruct [
    statement: nil,
    prepared_statement: nil,

    column_names: nil,
    column_types: nil,
  ]

  defimpl DBConnection.Query do
    def parse(query, _opts), do: query

    def describe(query, _opts), do: query

    def encode(_query, params, _opts), do: params

    def decode(query, result, _opts) do
      columns = Tuple.to_list(query.column_names)

      rows = Enum.map(result.raw_rows, fn row -> Enum.zip(columns, Tuple.to_list(row)) end)
      %{ result | rows: rows }
    end
  end
end
