# scripts/04_test_data.R
source("../utils/db.R")

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
  library(dplyr)
  library(rlang)
  library(glue)
  library(logger)
})






log_info("{line}", line = paste(rep("=", 60), collapse = ""))
log_info("04_test_data")


SCHEMA_CLEAN_DATA <- Sys.getenv("PG_SCHEMA", "r_stage")

con <- connect_postgres()
on.exit( try( DBI::dbDisconnect(con),silent=TRUE), add=TRUE)

test_results <- tibble::tibble(
  timestamp   = character(),
  category    = character(),
  schema      = character(),
  table_name = character(),
  test_name   = character(),
  status      = character()
)


uniqueness_columns <- list(
  stage_holidays_events = c("holiday_date", "holiday_type", "locale", "locale_name", "description"),
  stage_items          = c("item_nbr"),
  stage_oil            = c("date"),
  stage_stores         = c("store_nbr"),
#   stage_test           = c("id"),
#   stage_train          = c("date", "store_nbr", "item_nbr"),
  stage_transactions   = c("date", "store_nbr")
)

not_null_columns <- list(
  stage_holidays_events = c("holiday_date", "holiday_type", "locale", "locale_name", "description"),
  stage_items          = c("item_nbr","family","class"),
  stage_oil            = c("date","oil_price"),
  stage_stores         = c("store_nbr","city","state"),
#   stage_test           = c("date", "store_nbr", "item_nbr","id"),
#   stage_train          = c("date", "store_nbr", "item_nbr","unit_sales"),
  stage_transactions   = c("date", "store_nbr", "transactions")
)





######      FUNCTION LIST       ######
### UNIQUENESS TEST
test_uniqueness <- function(schema,table_name,cols ) {

    data <- pg_read_table(con, schema, table_name)
    cols <- rlang::syms(cols)
    test_name <- paste0("Unique (", paste(cols,collapse = ","),   ")")
    
    n_row <- nrow(data)
    unique_rows <- data %>% dplyr::distinct(!!!cols) %>% nrow()
    status <- if (n_row == unique_rows) "PASS" else "FAIL"



    test_results <<- dplyr::add_row(
        test_results,
        timestamp   = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        category    = "UNIQUENESS",
        schema      = schema,
        table_name = table_name,
        test_name   = test_name,
        status      = status
    )

    out_dir <- normalizePath(file.path(getwd(), "..", "output"), mustWork = FALSE)
    dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
    readr::write_csv(test_results,
    file.path(out_dir, "04_test_data_quality.csv"))
    
}


### NOT NULL TEST
test_not_null <- function(schema,table_name,cols ) {
    data <- pg_read_table(con, schema, table_name)
    # cols <- rlang::syms(cols)
    test_name <- paste0("NOT_NULL (", paste(cols,collapse = ","),   ")")

    nulls <- colSums(is.na(data[cols]))

    status <- paste(
        paste0(cols, " -> ", ifelse(nulls == 0, "PASS", "FAIL")),
        collapse = ", "
    )

    test_results <<- dplyr::add_row(
        test_results,
        timestamp   = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        category    = "NOT_NULL",
        schema      = schema,
        table_name = table_name,
        test_name   = test_name,
        status      = status
    )
    out_dir <- normalizePath(file.path(getwd(), "..", "output"), mustWork = FALSE)
    dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
    readr::write_csv(test_results,
    file.path(out_dir, "04_test_data_quality.csv"))

}


######               ######
######      RUN      ######




# # stage tables row count
# for(tbl in names(uniqueness_columns)) {
#     n_rows <- get_row_count("r_stage",tbl )
# logger::log_info(" {tbl} n_rows: {n_rows}" , tbl = tbl, n_rows = n_rows  )
# }



#UNIQUENESS TESTS 
for(tbl in names(uniqueness_columns)) {
test_uniqueness("r_stage", tbl, uniqueness_columns[[tbl]])
}
logger::log_info("UNIQUENESS TESTS COMPLETED and saved to /outputs" )


#NOT NULL TESTS 
for(tbl in names(not_null_columns)) {
test_not_null("r_stage", tbl, not_null_columns[[tbl]])
}
logger::log_info("not_null TESTS COMPLETED and saved to /outputs" )

