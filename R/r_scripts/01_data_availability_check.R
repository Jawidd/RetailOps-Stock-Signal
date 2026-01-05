# scripts/01_extract_data.R
# CSV extraction + loading handled by: scripts/extract_favorita_7z.sh and load_csv_to_postgres.py

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
  library(dplyr)
  library(logger)
  library(glue)
  library(tibble)
})




log_info("{line}", line = paste(rep("=", 60), collapse = ""))
log_info("data availability check (postgres)")

SCHEMA <- Sys.getenv("PG_SCHEMA", "raw")
EXPECTED_TABLES <- c("stores", "items", "transactions", "oil", "holidays_events", "test", "train")

con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host    =Sys.getenv("PG_HOST","postgres"),
    port    =as.integer(Sys.getenv("PG_PORT","5432")),
    dbname  =Sys.getenv("PG_DB","retailops"),
    user    =Sys.getenv("PG_USER","retailops"),
    password=Sys.getenv("PG_PASS","retailops123")
    )
on.exit( try( DBI::dbDisconnect(con),silent=TRUE), add=TRUE)

existing <- dbGetQuery(
    con,
    "select table_name
    from information_schema.tables
    where table_schema= $1",
    params = list(SCHEMA)
)$table_name

missing_tables <- setdiff(EXPECTED_TABLES, existing)

table_checks <- tibble(
  table = EXPECTED_TABLES,
  exists = table %in% existing,
  row_count = NA_real_,
  status = NA_character_,
  timestamp = as.character(Sys.time())
) %>%
  rowwise() %>%

mutate(
  row_count = if (exists) {
    dbGetQuery(
      con,
      glue("select count(*)::bigint as n from {DBI::dbQuoteIdentifier(con, SCHEMA)}.{DBI::dbQuoteIdentifier(con, table)}")
      )$n[1]
      } else NA_real_,
      status = case_when(
      !exists ~ "missing",
      exists & row_count > 0 ~ "ok",
      exists & row_count == 0 ~ "empty",
      TRUE ~ "unknown"
    )
  )  %>%
  ungroup()

  out_dir <- normalizePath(file.path(getwd(), "..", "output"), mustWork = FALSE)
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

  results_path <- file.path(out_dir, "01_postgres_raw_table_check.csv")
  write.csv(table_checks, results_path, row.names = FALSE)
  log_info("Postgres raw table check saved to {results_path}")

  empty_tables <- table_checks %>% filter(status == "empty") %>% pull(table)

if (length(missing_tables) == 0 && length(empty_tables) == 0) {
  log_info("All expected raw tables exist and have rows in schema '{SCHEMA}'.")
  quit(status = 0)
  } else {
  log_warn("Raw tables not fully ready in Postgres.")
  if (length(missing_tables) > 0) log_warn("Missing tables: {paste(missing_tables, collapse = ', ')}")
  if (length(empty_tables) > 0) log_warn("Empty tables: {paste(empty_tables, collapse = ', ')}")
}
