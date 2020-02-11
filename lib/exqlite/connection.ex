
require Exqlite.Query
alias Exqlite.Result

# TODO: Timeout Handling from `opts`

defmodule Exqlite.Connection do
  use DBConnection

  defmodule State do
    defstruct [
      connection: nil,
      status: :idle,
    ]
  end

  @impl true
  def connect(opts) do
    database = Keyword.fetch!(opts, :database) || raise "No :database specified"
    with {:ok, conn} <- :esqlite3.open(to_charlist(database)) do
      {:ok, %State{connection: conn, status: :idle}}
    end
  end

  @impl true
  def checkout(state), do: {:ok, state}

  @impl true
  def checkin(state), do: {:ok, state}

  @impl true
  def ping(state), do: {:ok, state}

  defp handle_tx(%{status: status} = state, expected_status, _after_status, _sql, _opts) when status != expected_status do
    {status, state}
  end

  defp handle_tx(state , _expected_status, after_status, sql, _opts) do
    with :ok <- :esqlite3.exec(sql, state.connection) do
      {:ok, :ok, %{state | status: after_status}}
    else
      {:error, _error} -> {:error, state}
    end
  end

  @impl true
  def handle_begin(opts, state), do: handle_tx(state, :idle, :transaction, "begin", opts)

  @impl true
  def handle_commit(opts, state), do: handle_tx(state, :transaction, :idle, "commit", opts)

  @impl true
  def handle_rollback(opts, state), do: handle_tx(state, :transaction, :idle, "rollback", opts)

  @impl true
  def handle_status(_opts, state), do: {state.status, state}

  @impl true
  def handle_prepare(query, _opts, state) do
    case maybe_prepare_query(query, state.connection) do
      {:ok, query} -> {:ok, query, state}
      {:error, error} -> {:error, error_description(error), state}
    end
  end

  @impl true
  def handle_execute(query, params, opts, state) do
    try do
      with {:ok, query} <- maybe_prepare_query(query, state.connection),
           :ok <- :esqlite3.bind(query.prepared_statement, params),
           {:ok, result} <- fetch_all(query.prepared_statement, opts)
      do
        {:ok, query, result, state}
      else
        {:error, error} -> throw {:error, error}
      end
    catch
      {:error, e} -> {:error, error_description(e), state}
    end
  end

  @spec error_description(any()) :: String.t()
  defp error_description(:args_wrong_length), do: "Invalid number of arguments"
  defp error_description({:constraint, b}), do: "Constraint error: " <> to_string(b)
  defp error_description({:sqlite_error, b}), do: to_string(b)
  defp error_description(error) when is_binary(error), do: to_string(error)

  @impl true
  def handle_close(_query, _opts, state), do: {:ok, [], state}

  @impl true
  def disconnect(_error, state) do
    :esqlite3.close(state.connection)
  end

  @impl true
  def handle_declare(query, params, _opts, state) do
    with {:ok, query} <- maybe_prepare_query(query, state.connection),
         :ok <- :esqlite3.bind(query.prepared_statement, params)
    do
      {:ok, query, query.prepared_statement, state}
    end
  end

  @impl true
  def handle_fetch(_query, cursor, opts, state) do
    # TODO: Support `:max_rows` in `opts`
    case fetch_one(cursor, opts) do
      {:ok, %Result{raw_rows: []} = result} -> {:halt, result, state}
      {:ok, result} -> {:cont, result, state}
      {:error, error} -> {:error, error_description(error), state}
    end
  end

  @impl true
  def handle_deallocate(_query, _cursor, _opts, state) do
    # :esqlite3.finalize() # TODO
    {:ok, [], state}
  end

  # Helpers

  @spec maybe_prepare_query(Exqlite.Query.t(), State.t()) :: {:ok, Exqlite.Query.t()} | {:error, String.t()}
  defp maybe_prepare_query(%Exqlite.Query{prepared_statement: nil} = query, connection) do
    with {:ok, stmt} <- :esqlite3.prepare(query.statement, connection) do
      query = %{query |
        prepared_statement: stmt,
        column_names: :esqlite3.column_names(stmt),
        column_types: :esqlite3.column_types(stmt)
      }
      {:ok, query}
    else
      {:error, error} -> {:error, error_description(error)}
    end
  end

  defp maybe_prepare_query(query, _connection) do
    {:ok, query}
  end

  @spec fetch_one(Exqlite.Query.t(), Keyword.t()) :: {:ok, Exqlite.Result.t()} | {:error, any()}
  defp fetch_one(statement, opts) do
    case :esqlite3.step(statement) do
      {:row, row} -> {:ok, %Result{raw_rows: [row]}}
      {:error, error} -> {:error, error}
      :"$done" -> {:ok, %Result{raw_rows: []}}
      :"$busy" ->
        Process.sleep(10)
        fetch_one(statement, opts)
    end
  end


  @spec fetch_all(Exqlite.Query.t(), Keyword.t()) :: {:ok, Exqlite.Result.t()} | {:error, any()}
  defp fetch_all(statement, opts) do
    fetch_all_loop(statement, opts, [])
  end

  @spec fetch_all_loop(Exqlite.Query.t(), Keyword.t(), [tuple()]) :: {:ok, Exqlite.Result.t()} | {:error, any()}
  defp fetch_all_loop(statement, opts, rows) do
    case fetch_one(statement, opts) do
      {:ok, %Result{raw_rows: []}} -> {:ok, %Result{raw_rows: rows}}
      {:ok, %Result{raw_rows: new_rows}} -> fetch_all_loop(statement, opts, rows ++ new_rows)
      {:error, error} -> {:error, error}
    end
  end
end
