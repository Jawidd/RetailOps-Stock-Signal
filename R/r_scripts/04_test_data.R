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


uniqueness_rules <- list(
  stage_holidays_events = c("holiday_date", "holiday_type", "locale", "locale_name", "description"),
  stage_items          = c("item_nbr"),
  stage_oil            = c("date"),
  stage_stores         = c("store_nbr"),
#   stage_test           = c("id"),
#   stage_train          = c("date", "store_nbr", "item_nbr"),
  stage_transactions   = c("date", "store_nbr")
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



######               ######
######      RUN      ######




#UNIQUENESS TESTS 
for(tbl in names(uniqueness_rules)) {
test_uniqueness("r_stage", tbl, uniqueness_rules[[tbl]])
}
logger::log_info("UNIQUENESS TESTS COMPLETED and saved to /outputs" )


