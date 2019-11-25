
require Exqlite.Query

defmodule Exqlite.Connection do
  use DBConnection

  @impl true
  def connect(opts) do
    database = Keyword.fetch!(opts, :database) || raise "No :database specifiec"
    with {:ok, conn} <- :esqlite3.open(to_charlist(database)) do
      {:ok, {conn, :idle}}
    end
  end

  @impl true
  def checkout(state), do: {:ok, state}

  @impl true
  def checkin(state), do: {:ok, state}

  @impl true
  def ping(state), do: {:ok, state}

  defp handle_tx({_conn, status} = state, expected_status, _after_status, _sql, _opts) when status != expected_status do
    {status, state}
  end

  defp handle_tx({conn, _status} , _expected_status, after_status, sql, _opts) do
    with :ok <- :esqlite3.exec(sql, conn) do
      {:ok, :ok, {conn, after_status}}
    else
      {:error, _error} -> {:error, conn, :error}
    end
  end

  @impl true
  def handle_begin(opts, state), do: handle_tx(state, :idle, :transaction, "begin", opts)

  @impl true
  def handle_commit(opts, state), do: handle_tx(state, :transaction, :idle, "commit", opts)

  @impl true
  def handle_rollback(opts, state), do: handle_tx(state, :transaction, :idle, "rollback", opts)

  @impl true
  def handle_status(_opts, {_conn, status} = state), do: {status, state}

  @impl true
  def handle_prepare(query, _opts, state) do
    # We skip preparing for now and just return the query as-is
    {:ok, query, state}
  end

  @impl true
  def handle_execute(query, params, _opts, {conn, _status} = state) do
    try do
      case :esqlite3.q(query.query, params, conn) do
        {:error, term} -> {:error, term}
        rows -> {:ok, query, rows, state}
      end
    catch
      {:error, {:sqlite_error, e}} -> {:error, to_string(e), state}
    end
  end

  @impl true
  def handle_close(_query, _opts, state), do: {:ok, [], state}

  @impl true
  def disconnect(_error, {conn, _status}) do
    :esqlite3.close(conn)
  end

  @impl true
  def handle_declare(query, params, _opts, {conn, _status} = state) do
    with {:ok, stmt} <- :esqlite3.prepare(query.query, conn),
      :ok <- :esqlite3.bind(stmt, params)
    do
      {:ok, query, stmt, state}
    end
  end

  @impl true
  def handle_fetch(_query, cursor, _opts, state) do
    case :esqlite3.fetchone(cursor) do
      :ok -> {:halt, [], state}
      {:error, error} -> {:error, error, state}
      row -> {:cont, [row]}
    end
  end

  @impl true
  def handle_deallocate(_query, _cursor, _opts, state) do
    # :esqlite3.finalize() # TODO
    {:ok, [], state}
  end
end
