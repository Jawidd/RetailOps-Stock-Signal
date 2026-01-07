# scripts/07_generate_report.R

suppressPackageStartupMessages({
  library(rmarkdown)
  library(logger)
  library(glue)
})

log_info("{line}", line = paste(rep("=", 60), collapse = ""))
log_info("07_generate_report")

proj_dir <- normalizePath(file.path(getwd(), ".."), mustWork = FALSE)
out_dir <- file.path(proj_dir, "output")
reports_dir <- file.path(proj_dir, "reports")
dir.create(reports_dir, showWarnings = FALSE, recursive = TRUE)



log_info("Writing report...")

rmd_path <- file.path(reports_dir, "week1_report.Rmd")

rmd_content <- c(
'---',
'title: "RetailOps Week 1 Report"',
'date: "`r Sys.Date()`"',
'output:',
'  html_document:',
'    toc: true',
'    toc_float: true',
'    df_print: paged',
'---',
'',
'```{r setup, include=FALSE}',
'knitr::opts_chunk$set(echo = FALSE, message = FALSE, warning = FALSE)',
'library(readr)',
'library(dplyr)',
'library(knitr)',
'',
glue('out_dir <- "{normalizePath(out_dir, mustWork = FALSE)}"'),
'```',
'',
'# Summary',
'',
'```{r}',
'summary_path <- file.path(out_dir, "06_summary_stats.csv")',
'cat("file:", summary_path, "\\n")',
'cat("exists:", file.exists(summary_path), "\\n\\n")',
'if (file.exists(summary_path)) {',
'  x <- read_csv(summary_path, show_col_types = FALSE)',
'  kable(x)',
'}',
'```',
'',
'# Top stores',
'',
'```{r}',
'top_path <- file.path(out_dir, "06_top_stores.csv")',
'cat("file:", top_path, "\\n")',
'cat("exists:", file.exists(top_path), "\\n\\n")',
'if (file.exists(top_path)) {',
'  x <- read_csv(top_path, show_col_types = FALSE)',
'  kable(head(x, 10))',
'}',
'```',
'',
'# Weekend vs weekday',
'',
'```{r}',
'weekend_path <- file.path(out_dir, "06_weekend_analysis.csv")',
'cat("file:", weekend_path, "\\n")',
'cat("exists:", file.exists(weekend_path), "\\n\\n")',
'if (file.exists(weekend_path)) {',
'  x <- read_csv(weekend_path, show_col_types = FALSE)',
'  kable(x)',
'}',
'```'
)

writeLines(rmd_content, rmd_path)



log_info("Rendering report...")

output_file <- file.path(reports_dir, "week1_report.html")

tryCatch(
  {
    rmarkdown::render(
      input = rmd_path,
      output_file = output_file,
      quiet = FALSE
    )

    log_info("Report saved: {output_file}")
  },
  error = function(e) {
    log_error("Error: {e$message}")
  }
)




