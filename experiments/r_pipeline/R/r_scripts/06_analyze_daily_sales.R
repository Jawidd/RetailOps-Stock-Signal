# scripts/06_analyze_sales.R

suppressPackageStartupMessages({
  library(dplyr)
  library(glue)
  library(logger)
  library(readr)
  library(data.table)
  library(ggplot2)
  library(scales)
})

log_info("{line}", line = paste(rep("=", 60), collapse = ""))
log_info("06_analyze_sales")

out_dir <- normalizePath(file.path(getwd(), "..", "output"), mustWork = FALSE)
plots_dir <- normalizePath(file.path(out_dir, "plots"), mustWork = FALSE)

dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(plots_dir, showWarnings = FALSE, recursive = TRUE)



log_info("Loading daily sales mart...")

daily_sales <- data.table::fread(file.path(out_dir, "05_daily_sales_mart.csv"))
daily_sales$date <- as.Date(daily_sales$date)

log_info("Loaded: {nrow(daily_sales)} rows")



log_info("Saving summary outputs...")

summary_stats <- daily_sales %>%
  summarise(
    total_days = n_distinct(date),
    total_stores = n_distinct(store_nbr),
    total_unit_sales = sum(total_units_sold, na.rm = TRUE),
    total_returned_units = sum(returned_units, na.rm = TRUE),
    avg_daily_units_sales = total_unit_sales / total_days,
    total_holiday_days = sum(as.integer(is_holiday), na.rm = TRUE),
    total_wage_days = sum(as.integer(is_wage_day), na.rm = TRUE),
    earthquake_days = sum(as.integer(is_earthquake_period), na.rm = TRUE)
  )

top_stores <- daily_sales %>%
  group_by(store_nbr, city, state, store_type, store_cluster) %>%
  summarise(
    total_units_sold = sum(total_units_sold, na.rm = TRUE),
    days_operated = n(),
    .groups = "drop"
  ) %>%
  mutate(
    avg_daily_units_sales = total_units_sold / days_operated
  ) %>%
  arrange(desc(total_units_sold)) %>%
  head(10)

weekend_analysis <- daily_sales %>%
  mutate(day_type = ifelse(is_weekend, "Weekend", "Weekday")) %>%
  group_by(day_type) %>%
  summarise(
    total_units_sold = sum(total_units_sold, na.rm = TRUE),
    days_count = n(),
    .groups = "drop"
  ) %>%
  mutate(
    avg_daily_units_sales = total_units_sold / days_count
  )

readr::write_csv(summary_stats, file.path(out_dir, "06_summary_stats.csv"))
readr::write_csv(top_stores, file.path(out_dir, "06_top_stores.csv"))
readr::write_csv(weekend_analysis, file.path(out_dir, "06_weekend_analysis.csv"))



log_info("Saving plots...")

p1 <- ggplot(top_stores, aes(x = reorder(as.character(store_nbr), total_units_sold), y = total_units_sold)) +
  geom_col() +
  coord_flip() +
  scale_y_continuous(labels = comma) +
  labs(
    title = "Top stores by total units sold",
    x = NULL,
    y = "total_units_sold"
  )

ggsave(file.path(plots_dir, "05_top_stores.png"), p1, width = 10, height = 6, dpi = 300)

p2 <- ggplot(weekend_analysis, aes(x = day_type, y = avg_daily_units_sales)) +
  geom_col() +
  scale_y_continuous(labels = comma) +
  labs(
    title = "avg_daily_units_sales: weekend vs weekday",
    x = NULL,
    y = "avg_daily_units_sales"
  )

ggsave(file.path(plots_dir, "05_weekend_weekday.png"), p2, width = 8, height = 6, dpi = 300)



