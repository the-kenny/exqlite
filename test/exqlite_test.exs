defmodule ExqliteTest do
  use ExUnit.Case
  doctest Exqlite

  alias Exqlite.Result

  defp db!() do
    {:ok, db} = Exqlite.start_link(database: ':memory:')
    db
  end

  test "query" do
    db = db!()
    assert {:ok, %Result{}} = Exqlite.query(db, "create table test (number integer)")
    assert {:ok, %Result{rows: []}} = Exqlite.query(db, "select * from test")
    assert {:ok, %Result{}} = Exqlite.query(db, "insert into test (number) values(?)", [42])
    assert {:ok, %Result{rows: [[{:number, 42}]]}} = Exqlite.query(db, "select * from test")
  end

  test "unique violations" do
    db = db!()
    assert {:ok, %Result{}} = Exqlite.query(db, "create table test (number integer unique)")
    assert {:ok, %Result{}} = Exqlite.query(db, "insert into test (number) values(?)", [42])
    assert {:error, "UNIQUE constraint failed: test.number"} = Exqlite.query(db, "insert into test (number) values(?)", [42])
  end
end
