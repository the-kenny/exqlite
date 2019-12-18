defmodule ExqliteTest do
  use ExUnit.Case
  doctest Exqlite

  require Temp

  alias Exqlite.Result

  test "start_link" do
    assert {:ok, db} = Exqlite.start_link(database: ':memory:')
    assert is_pid(db)

    assert {:ok, db} = Exqlite.start_link(database: ":memory:")
    assert is_pid(db)

    assert {:ok, db} = Exqlite.start_link(database: Temp.path!())
    assert is_pid(db)
  end

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

  @tag :skip
  test "prepare" do

  end

  @tag :skip
  test "prepare_execute" do

  end

  @tag :skip
  test "execute" do

  end

  @tag :skip
  test "close" do

  end

  @tag :skip
  test "transaction" do

  end

  @tag :skip
  test "rollback" do

  end

  @tag :skip
  test "stream" do

  end

end
