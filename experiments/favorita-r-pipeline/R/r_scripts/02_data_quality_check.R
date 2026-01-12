# scripts/02_data_quality_check

source("../utils/db.R")

suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(tibble)
  library(logger)
  library(glue)
  library(readr)

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

### GET_PK_DUPLICATE_COUNT_FUNCTION
get_pk_duplicate_count <- function(schema, table, pk_cols) {
  cols <- paste(sapply(pk_cols, q_ident), collapse = ", ")
  DBI::dbGetQuery(
    con,
    glue("
      select count(*)::bigint as n
      from (
        select {cols}
        from {q_ident(schema)}.{q_ident(table)}
        group by {cols}
        having count(*) > 1
      ) d
    ")
  )$n[1]
}


######      RUN      ######
summary_rows <- tibble()
column_profile <- tibble()
issues <- tibble()

for (tbl in names(Tables)){
  pk <- Tables[[tbl]]$pk
  # date_col <- Tables[[tbl]]$date_col
  log_info("=== Checking {SCHEMA}.{tbl} ")


  # 1.Table summary
  n_rows <- as.numeric(get_row_count(SCHEMA, tbl)) 
  summary_rows <-bind_rows(summary_rows, tibble(
  schema= SCHEMA,
  table= tbl,
  row_count = n_rows
  ))


  # 2. column_profile
  column_profile_tbl <- get_columns_meta(SCHEMA, tbl) %>%
    as_tibble() %>%
    left_join(
      get_pg_stat(SCHEMA, tbl) %>%
        as_tibble() %>%
        select(-schemaname, -tablename),  
      by = "column_name"
    ) %>%
    mutate(schema = SCHEMA, table = tbl) %>%  
    select(schema, table, everything())       
  column_profile <- bind_rows(column_profile, column_profile_tbl)

  
  # 3. check issues
  # 3a) PK columns nullable?
  pk_nullable <- column_profile_tbl %>%
    filter(column_name %in% pk, toupper(is_nullable) == "YES")

  if (nrow(pk_nullable) > 0) {
    issues <- bind_rows(
      issues,
      tibble(
        severity = "FAIL",
        table = tbl,
        check = "pk_nullable",
        detail = glue("PK column(s) nullable: {paste(pk_nullable$column_name, collapse = ', ')}")
      )
    )
  }


  # 3. check issues
  # 3b) PK values duplicated?
  row_limit <- 100000

  if (n_rows <= row_limit) {
    pk_duplicates <- as.numeric(get_pk_duplicate_count(SCHEMA, tbl, pk))

    if (pk_duplicates > 0) {
      issues <- bind_rows(
        issues,
        tibble(
          severity = "FAIL",
          table = tbl,
          check = "duplicate_pk",
          detail = glue("{pk_duplicates} duplicated PK group(s) on [{paste(pk, collapse = ', ')}]")
        ))}

  } else {
    issues <- bind_rows(
      issues,
      tibble(
        severity = "INFO",
        table = tbl,
        check = "duplicate_pk",
        detail = glue("Skipped (row_count={n_rows} > row_limit={row_limit})")
      ))}

}



out_dir <- normalizePath(file.path(getwd(), "..", "output"), mustWork = FALSE)
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)


summary_path <- file.path(out_dir, "02_data_quality_summary.csv")
cols_path    <- file.path(out_dir, "02_data_dictionary_pg_stats.csv")
issues_path <- file.path(out_dir, "02_quality_issues.csv")

write.csv(summary_rows, summary_path, row.names = FALSE)
write.csv(column_profile, cols_path, row.names = FALSE)
write.csv(issues, issues_path, row.names = FALSE)
# readr::write_csv(issues, issues_path)

log_info("summary_rows   saved to {summary_path}")
log_info("column_profile   saved to {cols_path}")
log_info("issues   saved to {issues_path}")
