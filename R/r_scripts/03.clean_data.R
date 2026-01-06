# scripts/03_clean_data

source("../utils/db.R")

suppressPackageStartupMessages({
  library(tidyverse)
  library(data.table)
  library(lubridate)
  library(janitor)
  library(logger)
  library(glue)

})


log_info("{line}", line = paste(rep("=", 60), collapse = ""))
log_info("03_clean_data")

SCHEMA_RAW <- Sys.getenv("PG_SCHEMA", "raw")
SCHEMA_CLEAN_DATA <- Sys.getenv("PG_SCHEMA", "r_stage")


con <- connect_postgres()
on.exit( try( DBI::dbDisconnect(con),silent=TRUE), add=TRUE)

DBI::dbExecute(con,glue("create schema if not exists {DBI::dbQuoteIdentifier(con, SCHEMA_CLEAN_DATA)}"))



######      FUNCTION LIST       ######
### PG_read_TABLE_FUNCTION
pg_read_table <- function(con,schema,table){
    DBI::dbGetQuery(con,glue(
        "select * from {DBI::dbQuoteIdentifier(con,schema)}.{DBI::dbQuoteIdentifier(con,table)}"
        )
    )
}

### PG_WRITE_TABLE_FUNCTION
pg_write_table <- function(con,schema,table,df){
    DBI::dbWriteTable(con,
    DBI::Id(schema = schema, table = table),
    df,overwrite = TRUE
    )
    invisible(TRUE)
}


### CLEAN_HOLIDAY_EVENTS_FUNCTION
clean_holiday_events <- function(con, raw_schema = SCHEMA_RAW){
    log_info("Cleaing HOLIDAY_EVENTS...")

    holidays_events <- pg_read_table(con,raw_schema,"holidays_events") %>% 
        clean_names() %>%
            filter(!is.na(date)) %>%
                mutate(
                    holiday_date   = as.Date(date),
                    holiday_type   = type,
                    locale      = locale,
                    locale_name = locale_name,
                    holiday_description = description,
                    holiday_transferred = coalesce(as.logical(transferred == "True"), FALSE),
                    is_actual_holiday   = (
                        type %in% c("Holiday", "Additional", "Bridge") & transferred == "False"
                        )
                    ) %>%
                        select(
                            holiday_date, holiday_type, locale, locale_name,
                            holiday_description, holiday_transferred, is_actual_holiday
                        )
    log_info("cleaned {nrow(holidays_events)} holidays_events")
    holidays_events 
}

### CLEAN_ITEMs_FUNCTION
clean_items <- function(con, raw_schema = SCHEMA_RAW){
    log_info("Cleaing items....")

    items <- pg_read_table(con,raw_schema,"items") %>% 
        clean_names() %>%
            filter(!is.na(item_nbr)) %>%
                mutate(
                    item_nbr = as.integer(item_nbr),
                    family= family,
                    class= as.integer(class),
                    perishable = as.logical(as.integer(perishable)),
                    )
    log_info("cleaned {nrow(items)} items")
    items 
}

### CLEAN_STORES_FUNCTION
clean_stores <- function(con, raw_schema = SCHEMA_RAW){
    log_info("Cleaing stores....")

    stores <- pg_read_table(con,raw_schema,"stores") %>% 
        clean_names() %>%
            fileter(!is.na(store_nbr)) %>%
                mutate(
                    store_nbr = as.integer(store_nbr),
                    city = city,
                    state = state,
                    store_type = as.character(type),
                    cluster = as.integer(cluster)
                    )%>%
                        select(
                            store_nbr, city, state, store_type,cluster
                        )
    log_info("cleaned {nrow(stores)} stores")
    stores 
}


