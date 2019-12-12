defmodule Exqlite.Result do
  defstruct [
    raw_rows: [],
    rows: [],
  ]

  @type t :: %__MODULE__{
    raw_rows: [raw_row()],
    rows: [row()],
  }

  @type raw_row :: tuple()
  @type row :: Keyword.t()
end
