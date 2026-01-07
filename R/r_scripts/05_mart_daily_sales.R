# scripts/05_build_marts.R
source("../utils/db.R")

suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(lubridate)
  library(glue)
  library(logger)
  library(data.table)
})

log_info("{line}", line = paste(rep("=", 60), collapse = ""))
log_info("05_build_marts")

SCHEMA_STAGE <- Sys.getenv("PG_SCHEMA", "r_stage")

con <- connect_postgres()
on.exit( try( DBI::dbDisconnect(con),silent=TRUE), add=TRUE)

out_dir <- normalizePath(file.path(getwd(), "..", "output"), mustWork = FALSE)
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)



log_info("Loading stage tables...")

train        <- pg_read_table(con, SCHEMA_STAGE, "stage_train")
stores       <- pg_read_table(con, SCHEMA_STAGE, "stage_stores")
transactions <- pg_read_table(con, SCHEMA_STAGE, "stage_transactions")
holidays     <- pg_read_table(con, SCHEMA_STAGE, "stage_holidays_events")



log_info("Creating daily sales mart...")

store_transactions <- transactions %>%
  mutate(date = as.Date(date)) %>%
  group_by(date, store_nbr) %>%
  summarise(
    transactions_count = sum(transactions, na.rm = TRUE),
    .groups = "drop"
  )

holiday_flags <- holidays %>%
  mutate(holiday_date = as.Date(holiday_date)) %>%
  filter(
    is_actual_holiday == TRUE,
    locale %in% c("National", "Regional")
  ) %>%
  group_by(holiday_date) %>%
  summarise(
    is_actual_holiday = any(is_actual_holiday),
    .groups = "drop"
  )

daily_sales <- train %>%
  mutate(date = as.Date(date)) %>%
  group_by(date, store_nbr) %>%
  summarise(
    unique_item_types_sold = n_distinct(item_nbr),
    line_items = n(),
    total_units_sold = sum(unit_sales, na.rm = TRUE),

    returned_units = sum(ifelse(is_return, abs(unit_sales), 0), na.rm = TRUE),
    sold_units = sum(ifelse(is_return, 0, unit_sales), na.rm = TRUE),

    promo_units = sum(ifelse(onpromotion, unit_sales, 0), na.rm = TRUE),
    nonpromo_units = sum(ifelse(onpromotion, 0, unit_sales), na.rm = TRUE),

    items_on_promo = sum(as.integer(onpromotion), na.rm = TRUE),
    distinct_items_on_promo = n_distinct(item_nbr[onpromotion]),

    year = max(year, na.rm = TRUE),
    month = max(month, na.rm = TRUE),
    day = max(day, na.rm = TRUE),
    day_of_week = max(day_of_week, na.rm = TRUE),

    is_wage_day = any(is_wage_day),
    is_earthquake_period = any(is_earthquake_period),

    .groups = "drop"
  ) %>%
  left_join(
    stores %>%
      transmute(
        store_nbr,
        city,
        state,
        store_type,
        store_cluster = cluster
      ),
    by = "store_nbr"
  ) %>%
  left_join(store_transactions, by = c("date", "store_nbr")) %>%
  left_join(holiday_flags, by = c("date" = "holiday_date")) %>%
  mutate(
    transactions_count = coalesce(transactions_count, 0L),
    is_holiday = coalesce(is_actual_holiday, FALSE),
    day_name = format(date, "%A"),
    is_weekend = day_of_week %in% c(0, 6)
  ) %>%
  select(-is_actual_holiday) %>%
  arrange(date, store_nbr)

log_info("Daily sales mart created: {nrow(daily_sales)} rows")

data.table::fwrite(daily_sales, file.path(out_dir, "05_daily_sales_mart.csv"))




