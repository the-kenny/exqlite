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

      rows = result.raw_rows
      |> Stream.map(&Tuple.to_list/1)
      |> Stream.map(fn row -> Enum.map(row, &undefined_to_nil/1) end)
      |> Enum.map(fn row -> Enum.zip(columns, row) end)

      %{ result | rows: rows }
    end

    defp undefined_to_nil(:undefined), do: nil
    defp undefined_to_nil(value), do: value
  end

end
