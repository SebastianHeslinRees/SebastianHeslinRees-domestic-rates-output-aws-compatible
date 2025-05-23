# Clear the workspace
rm(list = ls())

# Install and load required libraries
if (!requireNamespace("dplyr", quietly = TRUE)) install.packages("dplyr")
if (!requireNamespace("assertthat", quietly = TRUE)) install.packages("assertthat")
if (!requireNamespace("aws.s3", quietly = TRUE)) install.packages("aws.s3")
if (!requireNamespace("data.table", quietly = TRUE)) install.packages("data.table")
if (!requireNamespace("httr", quietly = TRUE)) install.packages("httr")
if (!requireNamespace("jsonlite", quietly = TRUE)) install.packages("jsonlite")
if (!requireNamespace("dtplyr", quietly = TRUE)) install.packages("dtplyr")
if (!requireNamespace("devtools", quietly = TRUE)) install.packages("devtools")
if (!requireNamespace("arrow", quietly = TRUE)) install.packages("arrow")
if (!requireNamespace("glue", quietly = TRUE)) install.packages("glue")
if (!requireNamespace("config", quietly = TRUE)) install.packages("config")

devtools::install_github("smbache/loggr")

library(aws.s3)
library(assertthat)
library(data.table)
library(httr)
library(jsonlite)
library(dtplyr)
library(devtools)
library(pryr)
library(profvis)
library(arrow)
library(glue)
library(config)
library(yaml)
library(stringr)

Sys.setenv("AWS_DEFAULT_REGION" = "eu-west-2")

# Set working directory and load package
setwd(".")
load_all("popmodules")


# Load config from S3
s3_config_path <- "s3://dpa-population-projection-data/population_mid_year_estimates/configs/backseries_rates_config.yml"
local_config <- tempfile()
save_object(object = s3_config_path, file = local_config)
cfg <- config::get(file = local_config)

get_base_s3_path <- function(path_template) {
  str_replace(path_template, "year=\\{year\\}/?", "")
}

list_available_years <- function(base_path) {
  dataset <- open_dataset(base_path, format = "parquet")
  dataset %>% select(year) %>% collect() %>% distinct() %>% pull(year) %>% sort()
}

# Load static inputs
popn <- s3readRDS("s3://dpa-population-projection-data/population_mid_year_estimates/2022/population_gla_2022.rds")
births <- s3readRDS("s3://dpa-population-projection-data/population_mid_year_estimates/2022/births_gla_2022.rds")
names(popn)[names(popn) == "value"] <- "popn"
names(births)[names(births) == "value"] <- "births"
popn$popn[popn$popn < 0] <- 0

# Preprocessing helpers
check_and_remove_nans <- function(df, col_name) {
  n_nans <- sum(is.na(df[[col_name]]))
  if (n_nans > 0) {
    message(glue("⚠️ {n_nans} NA values in '{col_name}' — removed."))
    df <- df %>% filter(!is.na(.data[[col_name]]))
  }
  return(df)
}

remove_duplicates <- function(df) {
  df %>% distinct()
}

check_negatives <- function(df, col_name) {
  n_neg <- sum(df[[col_name]] < 0)
  if (n_neg > 0) {
    message(glue("⚠️ {n_neg} negative values in '{col_name}'"))
  }
}

# Output
output_path <- "s3://dpa-population-projection-data/population_mid_year_estimates/outputs/parquet_backseries_rates/"

# Main loop when year == "all"
if (tolower(cfg$year) == "all") {
  path_template <- cfg$dom_origin_destination_path
  base_path <- get_base_s3_path(path_template)
  all_years <- list_available_years(base_path)
  high_memory_threshold <- 2 * 1024^3  # 2 GB

  failed_years <- c()
  memory_intensive_years <- c()

  for (year in all_years) {
    message(glue("⏳ Starting processing for year {year}..."))

    tryCatch({
      mem_start <- mem_used()

      s3_path <- gsub("\\{year\\}", year, path_template)
      flows <- open_dataset(s3_path, format = "parquet") %>% collect()
      flows$year <- year

      flows <- flows %>%
        remove_duplicates() %>%
        check_and_remove_nans("value")
      check_negatives(flows, "value")

      rates_backseries <- get_rate_backseries(
        component_mye_path = flows,
        popn_mye_path = popn,
        births_mye_path = births,
        years_backseries = year,
        col_partial_match = c("gss_out", "gss_in"),
        col_aggregation = c("year", "gss_code" = "gss_out", "gss_in", "sex", "age"),
        col_component = "value",
        rate_cap = NULL
      )

      write_dataset(
        dataset = rates_backseries,
        path = output_path,
        format = "parquet",
        partitioning = "year"
      )

      mem_end <- mem_used()
      mem_delta <- mem_end - mem_start

      message(glue("✅ Year {year} processed and saved. Memory used: {round(as.numeric(mem_delta) / 1024^2, 2)} MB"))

      if (mem_delta > high_memory_threshold) {
        memory_intensive_years <- c(memory_intensive_years, year)
        message(glue("⚠️ Year {year} used high memory: {round(as.numeric(mem_delta) / 1024^2, 2)} MB"))
      }
    }, error = function(e) {
      message(glue("❌ ERROR processing year {year}: {e$message}"))
      failed_years <<- c(failed_years, year)
    })
  }

  message("✅ All years attempted.")

  if (length(failed_years) > 0) {
    message("❌ Failed years: ", paste(failed_years, collapse = ", "))
  }
  if (length(memory_intensive_years) > 0) {
    message("⚠️ Memory-intensive years: ", paste(memory_intensive_years, collapse = ", "))
  }

} else {
  # Process single year
  year <- cfg$year
  s3_path <- gsub("\\{year\\}", year, cfg$dom_origin_destination_path)
  flows <- open_dataset(s3_path, format = "parquet") %>% collect()
  flows$year <- as.numeric(year)

  flows <- flows %>%
    remove_duplicates() %>%
    check_and_remove_nans("value")
  check_negatives(flows, "value")

  rates_backseries <- get_rate_backseries(
    component_mye_path = flows,
    popn_mye_path = popn,
    births_mye_path = births,
    years_backseries = as.numeric(year),
    col_partial_match = c("gss_out", "gss_in"),
    col_aggregation = c("year", "gss_code" = "gss_out", "gss_in", "sex", "age"),
    col_component = "value",
    rate_cap = NULL
  )

  write_dataset(
    dataset = rates_backseries,
    path = output_path,
    format = "parquet",
    partitioning = "year"
  )

  message(glue("✅ Completed single year {year}."))
}


