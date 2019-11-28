defmodule Exqlite.ConnectionTest do
  use ExUnit.Case, async: true
  doctest Exqlite.Connection

  alias Exqlite.Query

  @opts [
    database: ":memory:"
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

  test "blocking operations on the same database file" do
    file = "db_#{:erlang.phash2(make_ref())}"
    try do
      {:ok, blocking_db} = DBConnection.start_link(Exqlite.Connection, [ database: file ])
      create_table_query = %Query{statement: "create table test (id integer)"}
      {:ok, _, _} = DBConnection.prepare_execute(blocking_db, create_table_query, [])

      insert_query = %Query{statement: "insert into test (id) values (42)"}

      {:ok, db} = DBConnection.start_link(Exqlite.Connection, [ database: file ])

      parent = self()
      {:ok, task} = Task.start_link(fn ->
        DBConnection.transaction(blocking_db, fn blocking_db ->
          assert {:ok, _, _} = DBConnection.prepare_execute(blocking_db, insert_query, [])
          Process.send(parent, :continue, [])
          Process.sleep(20_000)
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
end
