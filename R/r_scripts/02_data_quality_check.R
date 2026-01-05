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

######      FUNCTION LIST       ######
### DB_QOUTE_IDENTIFIER_FUNCTION
q_ident <- function(x) DBI::dbQuoteIdentifier(con,x)

### ROW_COUNT_FUNCTION
get_row_count <- function(schema, table){
    DBI::dbGetQuery(
        con,
        glue("select count(*)::bigint as n from {q_ident(schema)}.{q_ident(table)}")
    )$n[1]
}

### GET_COLUMN_METADATA_FUNCTION
get_columns_meta <- function(schema,table){
    DBI::dbGetQuery(
        con,
        "select
            column_name,
            data_type,
            is_nullable
        from information_schema.columns
        where table_schema = $1 and table_name = $2
        order by ordinal_position",
        params = list(schema,table)
    )
}

### GET_PG_STAT_FUNCTION
get_pg_stat <- function(schema, table) {
  DBI::dbGetQuery(
    con,
    "select
       schemaname,
       tablename,
       attname as column_name,
       null_frac,
       avg_width,
       n_distinct,
       most_common_vals::text as most_common_vals,
       most_common_freqs::text as most_common_freqs,
       histogram_bounds::text as histogram_bounds,
       correlation
     from pg_stats
     where schemaname = $1 and tablename = $2",
    params = list(schema, table)
  )
}


######      RUN      ######
summary_rows <- tibble()
column_profile <- tibble()
issues <- tibble()

for (tbl in names(Tables)){
    # pk <- Tables[[tbl]]$pk
    # date_col <- Tables[[tbl]]$date_col
    log_info("=== Checking {SCHEMA}.{tbl} ")



#  1.Table summary
    row_count <- as.numeric(get_row_count(SCHEMA, tbl)) 
    summary_rows <-bind_rows(summary_rows, tibble(
    schema= SCHEMA,
    table= tbl,
    row_count = row_count,
    ))

#  2. column_profile
    column_metadata <- get_columns_meta(SCHEMA,tbl) %>% as_tibble()
    column_stats    <- get_pg_stat(SCHEMA, tbl) %>% 
        as_tibble() %>%
             mutate(schema = SCHEMA, table = tbl)
    column_profile <- bind_rows(column_profile, column_stats)
    
}

out_dir <- normalizePath(file.path(getwd(), "..", "output"), mustWork = FALSE)
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)


summary_path <- file.path(out_dir, "02_data_quality_summary.csv")
cols_path    <- file.path(out_dir, "02_data_dictionary_pg_stats.csv")

write.csv(summary_rows, summary_path, row.names = FALSE)
write.csv(column_profile, cols_path, row.names = FALSE)

log_info("summary_rows   saved to {summary_path}")
