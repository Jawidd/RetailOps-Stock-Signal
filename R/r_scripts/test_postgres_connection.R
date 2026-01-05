library(DBI)
library(RPostgres)

cat("Testing Postgres connection...\n")

con <- tryCatch(
  {
    dbConnect(
      RPostgres::Postgres(),
      host = "postgres",
      dbname = "retailops",
      user = "retailops",
      password = "retailops123",
      port = 5432
    )
  },
  error = function(e) {
    stop("Connection failed: ", e$message)
  }
)

cat("Connected successfully\n")

res <- dbGetQuery(con, "select current_database() as db, current_user as user, now() as time;")
print(res)

dbDisconnect(con)
cat("Connection closed\n")
