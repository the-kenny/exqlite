defmodule ExqliteTest do
  use ExUnit.Case
  doctest Exqlite

  require Temp

  alias Exqlite.Query
  alias Exqlite.Result

  test "start_link" do
    assert {:ok, db} = Exqlite.start_link(database: ':memory:')
    assert is_pid(db)

    assert {:ok, db} = Exqlite.start_link(database: ":memory:")
    assert is_pid(db)

    assert {:ok, db} = Exqlite.start_link(database: Temp.path!())
    assert is_pid(db)

    assert {:ok, db} = Exqlite.start_link(database: ':memory:', name: Foo.Bar)
    assert {:ok, _} = Exqlite.query(Foo.Bar, "select 42")
  end

  defp db!() do
    {:ok, db} = Exqlite.start_link(database: ':memory:')
    db
  end

  test "query" do
    db = db!()
    assert {:ok, %Result{rows: [[{:x, "foo"}]]}} = Exqlite.query(db, "select ? as x", ["foo"])

    assert {:ok, %Result{}} = Exqlite.query(db, "create table test (number integer)")
    assert {:ok, %Result{rows: []}} = Exqlite.query(db, "select * from test")
    assert {:ok, %Result{}} = Exqlite.query(db, "insert into test (number) values(?)", [42])
    assert {:ok, %Result{rows: [[{:number, 42}]]}} = Exqlite.query(db, "select * from test")

    assert {:ok, %Result{}} = Exqlite.query(db, "insert into test (number) values(?)", ["foo"])
  end

  test "prepare" do
    db = db!()

    # Simple case
    assert {:ok, %Query{}} = Exqlite.prepare(db, "select 42")

    # `prepare` With parameters
    assert {:ok, %Query{}} = Exqlite.prepare(db, "select ? + ?")

    # Invalid Syntax
    assert {:error, error_description} = Exqlite.prepare(db, "asdf 42")
    assert is_binary(error_description)
  end

  test "prepare_execute" do
    db = db!()

    # Simple case
    assert {:ok, %Query{}, result} = Exqlite.prepare_execute(db, "select 42 as result")
    assert result.rows == [[{:result, 42}]]

    # `prepare_execute` with bound parameters
    assert {:ok, %Query{}, result} = Exqlite.prepare_execute(db, "select ? + ? as result", [1, 2])
    assert result.rows == [[{:result, 3}]]

    # Invalid Syntax
    assert {:error, error_description} = Exqlite.prepare_execute(db, "asdf 42")
    assert is_binary(error_description)

    # Missing parameters
    assert {:error, error_description} = Exqlite.prepare_execute(db, "select ? + ? as result")
    assert is_binary(error_description)
  end

  test "execute" do
    db = db!()
    {:ok, query} = Exqlite.prepare(db, "select 42 as result")
    assert {:ok, result} = Exqlite.execute(db, query, [])
    assert result.rows == [[{:result, 42}]]

    assert {:error, _error} = Exqlite.execute(db, "select 42", [])
  end

  test "execute_raw" do
    db = db!()
    :ok = Exqlite.execute_raw(db, "begin; select 42 as result; commit;")
    {:error, "near \"bejgin\": syntax error"} = Exqlite.execute_raw(db, "bejgin; select 42 as result; commit;")
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
