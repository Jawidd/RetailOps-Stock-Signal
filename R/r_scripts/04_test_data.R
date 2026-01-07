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

uniqueness_rules <- list(
  stage_holidays_events = c("holiday_date","holiday_type", "locale", "locale_name"),
  stage_items          = c("item_nbr"),
  stage_oil            = c("date"),
  stage_stores         = c("store_nbr"),
  stage_test           = c("id"),
  stage_train          = c("date", "store_nbr", "item_nbr"),
  stage_transactions   = c("date", "store_nbr")
)





######      FUNCTION LIST       ######
### UNIQUENESS TEST
#res
test_uniqueness <- function(schema,table_name,cols ) {

    data <- pg_read_table(con, schema, table_name)
    test_name <- paste0("Unique (", paste(cols,collapse = ","),   ")")
    cols <- rlang::syms(cols)

    n_row <- nrow(data)
    unique_rows <- data %>% dplyr::distinct(!!!cols) %>% nrow()
    status <- if (n_row == unique_rows) "PASS" else "FAIL"

    log_info("UNIQUENESS | {schema} | {table_name} | {test_name} -> {status} ", schema = schema ,table_name = table_name, test_name = test_name, status = status)


}



######               ######
######      RUN      ######





for(tbl in names(uniqueness_rules)) {
test_uniqueness("r_stage", tbl, uniqueness_rules[[tbl]])

}
