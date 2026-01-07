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


# for 1-UNIQUENESS TEST Function
uniqueness_columns <- list(
  stage_holidays_events = c("holiday_date", "holiday_type", "locale", "locale_name", "description"),
  stage_items          = c("item_nbr"),
  stage_oil            = c("date"),
  stage_stores         = c("store_nbr"),
#   stage_train          = c("date", "store_nbr", "item_nbr"),
  stage_transactions   = c("date", "store_nbr")
)

# # for 2-test_not_null  Function
not_null_columns <- list(
  stage_holidays_events = c("holiday_date", "holiday_type", "locale", "locale_name", "description"),
  stage_items          = c("item_nbr","family","class"),
  stage_oil            = c("date","oil_price"),
  stage_stores         = c("store_nbr","city","state"),
#   stage_train          = c("date", "store_nbr", "item_nbr","unit_sales"),
  stage_transactions   = c("date", "store_nbr", "transactions")
)

# for 3-test_referential Function
referential_columns <- list(
  list(child = "stage_transactions", child_col = "store_nbr", parent = "stage_stores", parent_col = "store_nbr")
#   ,list(child = "stage_train",        child_col = "store_nbr", parent = "stage_stores", parent_col = "store_nbr"),
#   list(child = "stage_train",        child_col = "item_nbr",  parent = "stage_items",  parent_col = "item_nbr")
)

# for 4-test_referential Function
range_columns <- list(
  list(table = "stage_transactions", col = "transactions", min = 0, max = 10000),
  list(table = "stage_oil",          col = "oil_price",   min = 20, max = 150),
  list(table = "stage_items",        col = "class",       min = 1,   max = 10000)
)







######      FUNCTION LIST       ######
### 1-UNIQUENESS TEST Function
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


### 2-NOT NULL TEST
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

### 3-Test_Referntial_column_function
test_referential <- function(schema, child_table, child_col, parent_table, parent_col) {

    child <- pg_read_table(con, schema, child_table) 
    parent <- pg_read_table(con, schema, parent_table)

    bad_referential <- child %>%
    dplyr::anti_join(
      parent %>% dplyr::select(dplyr::all_of(parent_col)) %>% dplyr::distinct(),
      by = setNames(parent_col, child_col)
    )
    n_bad_referential <- nrow(bad_referential)

    test_name <-   test_name <- paste0(
    "FK (", child_table, ".", child_col,
    " -> ", parent_table, ".", parent_col, ")"
  )
    status <- if (n_bad_referential == 0) "PASS" else paste0("FAIL (", n_bad_referential, " missing keys)")
    test_results <<- dplyr::add_row(
    test_results,
    timestamp   = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    category    = "REFERENTIAL",
    schema      = schema,
    table_name  = child_table,
    test_name   = test_name,
    status      = status
  )
  out_dir <- normalizePath(file.path(getwd(), "..", "output"), mustWork = FALSE)
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  readr::write_csv(test_results, file.path(out_dir, "04_test_data_quality.csv"))


}



### 4-Test_Range_Function
test_range <- function(schema, table_name, col, min_value, max_value) {

  data <- pg_read_table(con, schema, table_name)

  # ignore NULLs (NULLs handled by NOT_NULL test)
  out_of_range <- data %>%
    dplyr::filter(.data[[col]] < min_value | .data[[col]] > max_value)

  n_out_of_range <- nrow(out_of_range)

  test_name <- paste0(
    "RANGE (", col, " between ", min_value, " and ", max_value, ")"
  )

  status <- if (n_out_of_range == 0) "PASS" else paste0("FAIL (", n_out_of_range, " out of range)")

  test_results <<- dplyr::add_row(
    test_results,
    timestamp   = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    category    = "RANGE",
    schema      = schema,
    table_name = table_name,
    test_name   = test_name,
    status      = status
  )

  out_dir <- normalizePath(file.path(getwd(), "..", "output"), mustWork = FALSE)
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

  readr::write_csv( test_results,
    file.path(out_dir, "04_test_data_quality.csv")
  )
}







######               ######
######      RUN      ######




# # stage tables row count
# for(tbl in names(uniqueness_columns)) {
#     n_rows <- get_row_count("r_stage",tbl )
# logger::log_info(" {tbl} n_rows: {n_rows}" , tbl = tbl, n_rows = n_rows  )
# }



# 1 run UNIQUENESS TESTS 
for(tbl in names(uniqueness_columns)) {
test_uniqueness("r_stage", tbl, uniqueness_columns[[tbl]])
}
logger::log_info("UNIQUENESS TESTS COMPLETED and saved to /outputs" )


# 2 run NOT NULL TESTS 
for(tbl in names(not_null_columns)) {
test_not_null("r_stage", tbl, not_null_columns[[tbl]])
}
logger::log_info("not_null TESTS COMPLETED and saved to /outputs" )


# 3 run referential tests
for (record in referential_columns) {
  test_referential(  "r_stage",
    record$child, record$child_col,
    record$parent, record$parent_col
  )
}
logger::log_info("REFERENTIAL TESTS COMPLETED and saved to /outputs")


# 4 run range tests
for (record in range_columns) {
  test_range("r_stage", record$table, record$col, record$min, record$max)
}

logger::log_info("RANGE TESTS COMPLETED and saved to /outputs")
