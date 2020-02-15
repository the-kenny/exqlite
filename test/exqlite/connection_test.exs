defmodule Exqlite.ConnectionTest do
  use ExUnit.Case, async: true
  doctest Exqlite.Connection

  alias Exqlite.Result
  alias Exqlite.Query
  require Temp

  @opts [
    database: ":memory:",
    timeout: 100
  ]

  test "status" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)
    assert :idle == DBConnection.status(db)
  end

  test "execute without prepare" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)
    {:ok, _, result} = DBConnection.execute(db, %Query{statement: "select 1+1"}, [])
    assert [[{:"1+1", 2}]] = result.rows
  end

  test "prepare_execute" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)
    {:ok, _, result} = DBConnection.prepare_execute(db, %Query{statement: "select 1+1"}, [])
    assert [[{:"1+1", 2}]] = result.rows
  end

  test "prepare_execute with args" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)
    {:ok, _, result} = DBConnection.prepare_execute(db, %Query{statement: "select ?+? as result"}, [42, 23])
    assert [[{:result, 65}]] = result.rows
  end

  test "transaction + prepare_execute" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)

    DBConnection.transaction(db, fn db ->
      {:ok, _, result} = DBConnection.prepare_execute(db, %Query{statement: "select 10"}, [])
      assert [[{:"10", 10}]]  = result.rows
    end)
  end

  test "various sqltie functions" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)
    {:ok, _, result} = DBConnection.prepare_execute(db, %Query{statement: "select sqlite_version()"}, [])
    assert [[{:"sqlite_version()", _}]] = result.rows
  end

  test "invalid sql syntax" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)
    assert {:error, _} = DBConnection.prepare(db, %Query{statement: "select asdf"}, [])
    assert {:error, _} = DBConnection.execute(db, %Query{statement: "select asdf"}, [])
    assert {:error, _} = DBConnection.prepare_execute(db, %Query{statement: "select asdf"}, [])
  end

  test "unique violations" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)
    assert {:ok, _, _result} = DBConnection.prepare_execute(db, %Query{statement: "create table test (number integer unique)"}, [])

    {:ok, query} = DBConnection.prepare(db, %Query{statement: "insert into test (number) values(?)"})
    assert {:ok, _, _result} = DBConnection.execute(db, query, [42], [])
    assert {:ok, _, _result} = DBConnection.execute(db, query, [23], [])
    assert {:error, "Constraint error: UNIQUE constraint failed: test.number"} = DBConnection.execute(db, query, [42])
  end

  test "NULL<->nil conversion" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)
    DBConnection.prepare_execute(db, %Query{statement: "create table test (number integer)"}, [])
    assert {:ok, _, _} = DBConnection.prepare_execute(db, %Query{statement: "insert into test(number) values(?)"}, [nil])
    assert {:ok, _, _} = DBConnection.prepare_execute(db, %Query{statement: "insert into test(number) values(?)"}, [42])

    assert {:ok, _, result} = DBConnection.prepare_execute(db, %Query{statement: "select number from test order by number asc"}, [])
    assert result.rows == [[{:number, nil}], [{:number, 42}]]
  end

  test "streaming" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)

    query = %Query{statement: "WITH RECURSIVE cnt(x) AS (SELECT 0 UNION ALL SELECT x+1 FROM cnt LIMIT ?) SELECT x FROM cnt"}
    query = DBConnection.prepare!(db, query)

    {:ok, _} = DBConnection.transaction(db, fn db ->
      try do
        range = Range.new(0, 100) |> Enum.map(fn x -> [{:x, x}] end)

        stream = DBConnection.stream(db, query, [length(range)])
        list = stream |> Enum.flat_map(fn x -> x.rows end) |> Enum.to_list()
        assert range == list
      after
        DBConnection.close(db, query)
      end
    end)
  end

  defp tmp_db!() do
    Temp.track!()
    {:ok, path} = Temp.path("exqlite-test")
    path
  end

  test "blocking operations on the same database file" do
    file = tmp_db!()
    try do
      {:ok, blocking_db} = DBConnection.start_link(Exqlite.Connection, [ database: file ])
      create_table_query = %Query{statement: "create table test (id integer)"}
      {:ok, _, _} = DBConnection.prepare_execute(blocking_db, create_table_query, [])

      insert_query = %Query{statement: "insert into test (id) values (42)"}

      {:ok, db} = DBConnection.start_link(Exqlite.Connection, [ database: file ])

      parent = self()
      {:ok, _task} = Task.start_link(fn ->
        assert {:ok, _} = DBConnection.transaction(blocking_db, fn blocking_db ->
          assert {:ok, _, _} = DBConnection.prepare_execute(blocking_db, insert_query, [])
          Process.send(parent, :continue, [])
          Process.sleep(1000)
        end)
      end)

      receive do
        :continue -> :ok
      end

      assert {:ok, _, _} = DBConnection.prepare_execute(db, insert_query, [])
    after
      File.rm!(file)
    end
  end

  test "error when opening a file in a non existing director" do
    file = Path.join("non/existing/path/", tmp_db!())
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, [database: file])
    assert {:error, _error} = DBConnection.execute(db, %Exqlite.Query{statement: "select true"}, [])
  end
end
