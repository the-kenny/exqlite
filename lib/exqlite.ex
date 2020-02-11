defmodule Exqlite do
  @moduledoc """
  Sqlite3 driver for Elixir.
  """

  @type start_option() ::
    {:database, binary()}
    | DBConnection.start_option()

  @type option() :: DBConnection.option()

  @doc """
  Opens a connection to a SQLite database.

  ## Parameters

  ## Examples

  Start a in-memory database connection

      iex> {:ok, db} = Exqlite.start_link([database: ':memory:'])
      iex> is_pid(db)
      true

  Start a database connection to a file

      iex> file = Path.absname("test.db"); File.mkdir_p!(Path.dirname(file))
      iex> {:ok, db} = Exqlite.start_link([database: file])
      iex> is_pid(db)
      true

  """
  @spec start_link([start_option()]) :: GenServer.on_start
  def start_link(opts) do
    DBConnection.start_link(Exqlite.Connection, opts)
  end

  @doc """
  Run queries against a given database connection.

  ## Options

  Options are passed to `DBConnection.execute/4` for text protocol, and
  `DBConnection.prepare_execute/4` for binary protocol. See their documentation
  for all available options.

  ## Examples

  Run queries

      iex> {:ok, db} = Exqlite.start_link([database: ':memory:'])
      iex> {:ok, _} = Exqlite.query(db, "create table test (number integer)")
      iex> {:ok, _} = Exqlite.query(db, "insert into test (number) values (42)")
      iex> {:ok, result} = Exqlite.query(db, "select * from test")
      iex> result.rows
      [[number: 42]]

  """
  def query(conn, statement, params \\ [], options \\ []) do
    query = %Exqlite.Query{statement: statement}
    DBConnection.prepare_execute(conn, query, params, options)
    |> query_result()
  end

  defp query_result({:ok, _query, result}), do: {:ok, result}
  defp query_result({:error, _} = error), do: error

  def prepare(conn, statement, options \\ []) do
    query = %Exqlite.Query{statement: statement}
    DBConnection.prepare(conn, query, options)
  end

  def prepare_execute(conn, statement, options \\ []) do
    query = %Exqlite.Query{statement: statement}
    DBConnection.prepare_execute(conn, query, options)
  end


  def execute(conn, %Exqlite.Query{} = query, params, opts) do
    with {:ok, _query, result} <- DBConnection.execute(conn, query, params, opts) do
      {:ok, result}
    end
  end

  def execute(_conn, _query, _params, _opts) do
    {:error, "Unprepared query. Did you run `Exqlite.prepare`?"}
  end

  def execute(conn, query, params), do: execute(conn, query, params, [])


  def execute_raw(conn, sql, opts \\ []) do
    query = %Exqlite.RawQuery{sql: sql}
    with {:ok, _query, result} <- DBConnection.execute(conn, query, [], opts) do
      {:ok, result}
    end
  end

  def close(conn, %Exqlite.Query{} = query, opts \\ []) do
    case DBConnection.close(conn, query, opts) do
      {:ok, _} ->
        :ok

      {:error, _} = error ->
        error
    end
  end

  defdelegate transaction(conn, fun, opts \\ []), to: DBConnection

  defdelegate rollback(conn, reason), to: DBConnection

  def stream(%DBConnection{} = conn, statement, params, opts) when is_binary(statement) do
    query = %Exqlite.Query{statement: statement}

    opts = Keyword.put_new(opts, :max_rows, 500)
    DBConnection.prepare_stream(conn, query, params, opts)
  end

  def stream(%DBConnection{} = conn, %Exqlite.Query{} = query, params, opts) do
    opts = Keyword.put_new(opts, :max_rows, 500)
    DBConnection.stream(conn, query, params, opts)
  end

  def child_spec(opts) do
    DBConnection.child_spec(Exqlite.Connection, opts)
  end

end
