defmodule Exqlite.Query do
  defstruct [
    statement: nil,
    prepared_statement: nil,
  ]

  defimpl DBConnection.Query do
    def parse(query, _opts), do: query

    def describe(query, _opts), do: query

    def encode(_query, params, _opts), do: params

    def decode(query, result, _opts) do
      columns = Tuple.to_list(:esqlite3.column_names(query.prepared_statement))

      Enum.map(result, fn row ->
        Enum.zip(columns, Tuple.to_list(row))
      end)

    end
  end
end
