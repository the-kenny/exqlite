defmodule Exqlite.ConnectionTest do
  use ExUnit.Case
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
    assert {:ok, _query, [["1+1": 2]]} = DBConnection.execute(db, Query.from("select 1+1"), [])
  end

  test "prepare_execute" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)
    assert {:ok, _query, [["1+1": 2]]} = DBConnection.prepare_execute(db, Query.from("select 1+1"), [])
  end

  test "prepare_execute with args" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)
    assert {:ok, _query, [[result: 65]]} = DBConnection.prepare_execute(db, Query.from("select ?+? as result"), [42, 23])
  end

  test "transaction + prepare_execute" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)

    DBConnection.transaction(db, fn db ->
      assert {:ok, _query, [["10": 10]]} = DBConnection.prepare_execute(db, Query.from("select 10"), [])
    end)
  end

  test "invalid sql syntax" do
    {:ok, db} = DBConnection.start_link(Exqlite.Connection, @opts)
    assert {:error, _} = DBConnection.prepare(db, Query.from("select asdf"), [])
    assert {:error, _} = DBConnection.execute(db, Query.from("select asdf"), [])
    assert {:error, _} = DBConnection.prepare_execute(db, Query.from("select asdf"), [])
  end
end
