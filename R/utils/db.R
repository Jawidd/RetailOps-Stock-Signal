# utils/db.R

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
  library(glue)
})


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







con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host    =Sys.getenv("PG_HOST","postgres"),
    port    =as.integer(Sys.getenv("PG_PORT","5432")),
    dbname  =Sys.getenv("PG_DB","retailops"),
    user    =Sys.getenv("PG_USER","retailops"),
    password=Sys.getenv("PG_PASS","retailops123")
    )
on.exit( try( DBI::dbDisconnect(con),silent=TRUE), add=TRUE)