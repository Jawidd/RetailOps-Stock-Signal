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
earthquake_start <- as.Date(Sys.getenv("earthquake_start", "2016-04-16"))
earthquake_end <- as.Date(Sys.getenv("earthquake_end", "2016-04-30"))

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
                    date   = as.Date(date),
                    type   = type,
                    locale      = locale,
                    locale_name = locale_name,
                    holiday_description = description,
                    holiday_transferred = coalesce(as.logical(transferred == "True"), FALSE),
                    is_actual_holiday   = (
                        type %in% c("Holiday", "Additional", "Bridge") & transferred == "False"
                        )
                    ) %>%
                        rename(holiday_date = date , holiday_type = type)

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
                    perishable = as.logical(as.integer(perishable))
                    )
    log_info("cleaned {nrow(items)} items")
    items 
}


### CLEAN_OIL_FUNCTION
clean_oil <- function(con, raw_schema = SCHEMA_RAW){
    log_info("Cleaing oil table....")

    oil_price_records <- pg_read_table(con,raw_schema,"oil") %>% 
        clean_names() %>%
            filter(!is.na(dcoilwtico) ) %>%
                mutate(
                    date   = as.Date(date),
                    dcoilwtico = as.double(dcoilwtico)
                    )%>%
                        arrange(date) %>%
                                        mutate(dcoilwtico = dcoilwtico - lag(dcoilwtico, default = NA))  %>%
                    rename(oil_price = dcoilwtico ,  )

    log_info("cleaned {nrow(oil_price_records)} oil_price_records")
    oil_price_records 
}


### CLEAN_STORES_FUNCTION
clean_stores <- function(con, raw_schema = SCHEMA_RAW){
    log_info("Cleaing stores....")

    stores <- pg_read_table(con,raw_schema,"stores") %>% 
        clean_names() %>%
            filter(!is.na(store_nbr)) %>%
                mutate(
                    store_nbr = as.integer(store_nbr),
                    city = city,
                    state = state,
                    type = as.character(type),
                    cluster = as.integer(cluster)
                    )%>%
                        rename(store_type = type )


    log_info("cleaned {nrow(stores)} stores")
    stores 
}




### CLEAN_Transaction_FUNCTION
clean_transactions <- function(con, raw_schema = SCHEMA_RAW) {
  log_info("Cleaning transactions...")

  transactions <- pg_read_table(con, raw_schema, "transactions") %>%
    clean_names() %>%
    filter(!is.na(date) & !is.na(store_nbr) & !is.na(transactions)) %>%
    mutate(
      date         = as.Date(date),
      store_nbr    = as.integer(store_nbr),
      transactions = as.integer(transactions)
    )

  log_info("  Cleaned {nrow(transactions)} transaction records")
  transactions
}



### CLEAN_Train_FUNCTION
clean_train <- function(con, raw_schema = SCHEMA_RAW) {
  log_info("Cleaning Train ")

  train <- pg_read_table(con, raw_schema, "train") %>%
    clean_names() %>%
    filter(!is.na(date) & !is.na(store_nbr) & !is.na(item_nbr) & !is.na(unit_sales)) %>%
    mutate(
      id        = as.integer(id),
      date      = as.Date(date),
      store_nbr = as.integer(store_nbr),
      item_nbr  = as.integer(item_nbr),
      unit_sales = as.numeric(unit_sales),
      onpromotion = coalesce(as.logical(onpromotion == "True"), FALSE),

      is_return  = unit_sales < 0,
      year       = year(date),
      month      = month(date),
      day        = day(date),
      day_of_week = wday(date) ,  # 0=Monday, 6=Sunday
      is_earthquake_period = date >= earthquake_start &
        date <= earthquake_end,
      is_wage_day = (day == 15 | day == days_in_month(date))
    )
    

  log_info("  Cleaned {nrow(train)} train records")
  train
}

### CLEAN_TestFUNCTION
clean_test <- function(con, raw_schema = SCHEMA_RAW) {
  log_info("Cleaning test ")

  test <- pg_read_table(con, raw_schema, "test") %>%
    clean_names() %>%
    filter(!is.na(date) & !is.na(store_nbr) & !is.na(item_nbr)) %>%
    mutate(
      id        = as.integer(id),
      date      = as.Date(date),
      store_nbr = as.integer(store_nbr),
      item_nbr  = as.integer(item_nbr),
     
      onpromotion = coalesce(as.logical(onpromotion == "True"), FALSE),

   
      year       = year(date),
      month      = month(date),
      day        = day(date),
      day_of_week = wday(date) ,  # 0=Monday, 6=Sunday
      is_earthquake_period = date >= earthquake_start &
        date <= earthquake_end,
      is_wage_day = (day == 15 | day == days_in_month(date))
    )
    

  log_info("  Cleaned {nrow(test)} test records")
  test
}





######               ######
######      RUN      ######

cleaned_data <- list()

cleaned_data$holidays_events <- clean_holiday_events(con)
cleaned_data$items <- clean_items(con)
cleaned_data$oil <- clean_oil(con)
cleaned_data$stores <- clean_stores(con)
cleaned_data$transactions <- clean_transactions(con)
# cleaned_data$train <- clean_train(con)
cleaned_data$test  <- clean_test(con)

log_info("Writing cleaned tables to Postgres schema {SCHEMA_CLEAN_DATA}...")

for(name in names(cleaned_data)){

    out_table <- paste0("stage_", name)
    pg_write_table(con,SCHEMA_CLEAN_DATA,out_table,cleaned_data[[name]])
    log_info("wrote {SCHEMA_CLEAN_DATA}.{out_table} ({nrow(cleaned_data[[name]])} rows)")
}