# utils/db.R

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
  library(glue)
})

### connect_postgres_FUNCTION
connect_postgres <- function ( 
    host     = Sys.getenv("PG_HOST", "postgres"),
    port     = as.integer(Sys.getenv("PG_PORT", "5432")),
    dbname   = Sys.getenv("PG_DB", "retailops"),
    user     = Sys.getenv("PG_USER", "retailops"),
    password = Sys.getenv("PG_PASS", "retailops123")
){  
    con <- tryCatch(
        DBI::dbConnect(
        RPostgres::Postgres(),
        host     = host,
        port     = port,
        dbname   = dbname,
        user     = user,
        password = password
        ),

        error = function(e) {
            stop(glue( "Failed to connect to Postgres:\n",
                        "  host={host}, db={dbname}, user={user}\n",
                        "  error={e$message}"
            ),call. = False)
        } 
    )        
    return(con)
}



q_ident <- function(x) DBI::dbQuoteIdentifier(con,x)



### PG_read_TABLE_FUNCTION
pg_read_table <- function(con,schema,table){
    DBI::dbGetQuery(con,glue(
        "select * from {q_ident(schema)}.{q_ident(table)}"
        )
    )
}

### PG_WRITE_TABLE_FUNCTION
pg_write_table <- function(con,schema,table,df){
    DBI::dbWriteTable(con,
    DBI::Id(schema = schema, table = table),
    df,overwrite = TRUE
    )
    invisible(TRUE)
}


### ROW_COUNT_FUNCTION
get_row_count <- function(schema, table){
    DBI::dbGetQuery(
        con,
        glue("select count(*)::bigint as n from {q_ident(schema)}.{q_ident(table)}")
    )$n[1]
}


con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host    =Sys.getenv("PG_HOST","postgres"),
    port    =as.integer(Sys.getenv("PG_PORT","5432")),
    dbname  =Sys.getenv("PG_DB","retailops"),
    user    =Sys.getenv("PG_USER","retailops"),
    password=Sys.getenv("PG_PASS","retailops123")
    )
on.exit( try( DBI::dbDisconnect(con),silent=TRUE), add=TRUE)