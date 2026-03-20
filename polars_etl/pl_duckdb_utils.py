import duckdb


def load_to_duckdb(df):
    con = duckdb.connect()
    con.register("articles_df", df.to_arrow())
    con.execute("CREATE TABLE articles AS SELECT * FROM articles_df")
    return con


def query_duckdb(con, query):
    return con.execute(query).fetch_df()