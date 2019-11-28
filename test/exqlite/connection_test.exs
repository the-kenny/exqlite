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
    assert {:ok, _query, [["1+1": 2]]} = DBConnection.execute(db, %Query{statement: "select 1+1"}, [])
  end

  test "prepare_execute" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)
    assert {:ok, _query, [["1+1": 2]]} = DBConnection.prepare_execute(db, %Query{statement: "select 1+1"}, [])
  end

  test "prepare_execute with args" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)
    assert {:ok, _query, [[result: 65]]} = DBConnection.prepare_execute(db, %Query{statement: "select ?+? as result"}, [42, 23])
  end

  test "transaction + prepare_execute" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)

    DBConnection.transaction(db, fn db ->
      assert {:ok, _query, [["10": 10]]} = DBConnection.prepare_execute(db, %Query{statement: "select 10"}, [])
    end)
  end

  test "various sqltie functions" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)
    assert {:ok, _query, [["sqlite_version()": _]]} = DBConnection.prepare_execute(db, %Query{statement: "select sqlite_version()"}, [])
  end

  test "invalid sql syntax" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)
    assert {:error, _} = DBConnection.prepare(db, %Query{statement: "select asdf"}, [])
    assert {:error, _} = DBConnection.execute(db, %Query{statement: "select asdf"}, [])
    assert {:error, _} = DBConnection.prepare_execute(db, %Query{statement: "select asdf"}, [])
  end
    assert {:error, _} = DBConnection.prepare(db, Query.from("select asdf"), [])
    assert {:error, _} = DBConnection.execute(db, Query.from("select asdf"), [])
    assert {:error, _} = DBConnection.prepare_execute(db, Query.from("select asdf"), [])
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
