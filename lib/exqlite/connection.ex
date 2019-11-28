
require Exqlite.Query

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
    database = Keyword.fetch!(opts, :database) || raise "No :database specifiec"
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
  @spec handle_status(any, atom | %{status: any}) :: {any, atom | %{status: any}}
  def handle_status(_opts, state), do: {state.status, state}

  @spec maybe_prepare_query(Exqlite.Query.t(), State.t()) :: {:ok, Exqlite.Query.t()} | {:error, String.t()}
  defp maybe_prepare_query(%Exqlite.Query{prepared_statement: nil} = query, connection) do
    with {:ok, stmt} <- :esqlite3.prepare(query.statement, connection) do
      {:ok, %{query | prepared_statement: stmt}}
    end
  end

  defp maybe_prepare_query(query, _connection) do
    {:ok, query}
  end

  @impl true
  def handle_prepare(query, _opts, state) do
    case maybe_prepare_query(query, state.connection) do
      {:ok, query} -> {:ok, query, state}
      {:error, error} -> {:error, error, state}
    end
  end

  @impl true
  def handle_execute(query, params, _opts, state) do
    try do
      with {:ok, query} <- maybe_prepare_query(query, state.connection),
           :ok <- :esqlite3.bind(query.prepared_statement, params),
           rows when is_list(rows) <- :esqlite3.fetchall(query.prepared_statement)
      do
        {:ok, query, rows, state}
      else
        {:error, error} -> throw {:error, error}
      end
    catch
      {:error, {:sqlite_error, e}} -> {:error, to_string(e), state}
    end
  end

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
  def handle_fetch(_query, cursor, _opts, state) do
    case :esqlite3.fetchone(cursor) do
      :ok -> {:halt, [], state}
      {:error, error} -> {:error, error, state}
      row -> {:cont, [row], state}
    end
  end

  @impl true
  def handle_deallocate(_query, _cursor, _opts, state) do
    # :esqlite3.finalize() # TODO
    {:ok, [], state}
  end
end
