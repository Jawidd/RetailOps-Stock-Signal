source("../utils/db.R")
library(DBI)
library(RPostgres)
library(logger)

log_info("{line}", line = paste(rep("=", 60), collapse = ""))
log_info("data availability check (postgres)")

con <- connect_postgres()
on.exit( try( DBI::dbDisconnect(con),silent=TRUE), add=TRUE)


cat("Connected successfully\n")

res <- dbGetQuery(con, "select current_database() as db, current_user as user, now() as time;")
print(,res)

dbDisconnect(con)
cat("Connection closed\n")
