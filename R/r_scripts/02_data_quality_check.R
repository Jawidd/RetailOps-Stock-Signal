# scripts/02_data_quality_check
source("../utils/db.R")
suppressPackageStartupMessages({
  library(tidyverse)
  library(data.table)
  library(skimr)
  library(DataExplorer)
  library(logger)
  library(glue)
  library(janitor)
})



log_info("{line}", line = paste(rep("=", 60), collapse = ""))
log_info("02_data_quality_check")


SCHEMA <- Sys.getenv("PG_SCHEMA", "raw")

Tables <- list(
    stores =        list(pk= c("store_nbr"),date_col = NA),
    items =         list(pk=c("item_nbr"),date_col = NA),
    transactions=   list(pk=c("date","store_nbr"),date_col = "date"),
    oil=            list(pk=c("date"), date_col= "date"),
    holidays_events=list(pk=c("date", "locale", "description"),date_col= "date"),
    train=          list(pk=c("date","store_nbr","item_nbr"), date_col= "date"),
    test=          list(pk=c("date","store_nbr","item_nbr"), date_col= "date")
    )


con <- connect_postgres()
on.exit( try( DBI::dbDisconnect(con),silent=TRUE), add=TRUE)

###### FUNCTION LIST ######
### DB QOUTE IDENTIFIER FUNCTION
q_ident <- function(x) DBI::dbQuoteIdentifier(con,x)

### ROW COUNT FUNCTION
get_row_count <- function(schema, table){
    DBI::dbGetQuery(
        con,
        glue("select count(*)::bigint as n from {q_ident(schema)}.{q_ident(table)}")
    )$n[1]
}



###### RUN ######
summary_rows <- tibble()
column_profile <- tibble()
issues <- tibble()

for (tbl in names(Tables)){
    pk <- Tables[[tbl]]$pk
    date_col <- Tables[[tbl]]$date_col
    log_info("=== Checking {SCHEMA}.{tbl} ")

    # summary_rows <- bind_rows(summary_rows, tibble(
    #     schema = SCHEMA,
    #     table = tbl,
    #     row_count= NA_real_,
    #     pk_duplicate_groups= NA_real_,
    #     date_min = NA_character_,
    #     date_max = NA_character_
    # ))
    row_count <- as.numeric(get_row_count(SCHEMA, tbl)) 

    summary_rows <-bind_rows(summary_rows, tibble(
    schema= SCHEMA,
    table= tbl,
    row_count = row_count

    ))
    out_dir <- normalizePath(file.path(getwd(), "..", "output"), mustWork = FALSE)
    dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

    summary_path <- file.path(out_dir, "02_data_quality_check.csv")

    write.csv(summary_rows, summary_path, row.names = FALSE)

    log_info("summary_rows   saved to {summary_path}")


}

