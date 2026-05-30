use duckdb::duckdb_entrypoint_c_api;
use duckdb::Connection;

#[duckdb_entrypoint_c_api(ext_name = "db_driver", min_duckdb_version = "v0.0.1")]
pub fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "postgres")]
    con.register_table_function::<crate::vtab::RemoteVTab<crate::vtab::PostgresConnector>>(
        "postgres_query",
    )?;

    #[cfg(feature = "mysql")]
    con.register_table_function::<crate::vtab::RemoteVTab<crate::vtab::MySqlConnector>>(
        "mysql_query",
    )?;

    #[cfg(feature = "mssql")]
    con.register_table_function::<crate::vtab::RemoteVTab<crate::vtab::MssqlConnector>>(
        "mssql_query",
    )?;

    Ok(())
}
