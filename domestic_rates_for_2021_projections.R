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

Sys.setenv("AWS_DEFAULT_REGION" = "eu-west-2")

# Sys.setenv(
# AWS_ACCESS_KEY_ID = "",
# AWS_SECRET_ACCESS_KEY = "",
# AWS_DEFAULT_REGION = ""
# )


# Set the working directory to the folder containing the popmodules 
setwd(".")

# Load the package
load_all("popmodules")

message("Scenario domestic migration rates (2021 projections)")

library(arrow)
library(aws.s3)
library(glue)
library(config)
library(dplyr)

cfg <- config::get(file = "config/config.yml")
year <- cfg$year
base_path <- cfg$dom_origin_destination_path_template

get_flows_data <- function(year, base_path) {
  if (tolower(year) == "all") {
    message("Reading ALL years from S3...")

    bucket_parts <- strsplit(gsub("^s3://", "", base_path), "/")[[1]]
    bucket <- bucket_parts[1]
    prefix <- paste(bucket_parts[-1], collapse = "/")

    all_files <- get_bucket_df(bucket = bucket, prefix = prefix)
    parquet_files <- all_files %>%
      filter(grepl("\\.parquet$", Key)) %>%
      pull(Key)

    all_data <- lapply(parquet_files, function(key) {
      uri <- glue("s3://{bucket}/{key}")
      message(glue("Reading: {uri}"))
      arrow::read_parquet(arrow::s3_bucket(uri))
    })

    flows <- bind_rows(all_data)

  } else {
    message(glue("Reading flows for year {year}..."))
    parquet_path <- glue("{base_path}{year}/1d329d7900474c51be1834be9d0c727a-0.parquet")

    # Extract bucket and key
    bucket_parts <- strsplit(gsub("^s3://", "", parquet_path), "/")[[1]]
    bucket <- bucket_parts[1]
    key <- paste(bucket_parts[-1], collapse = "/")
    s3_obj <- arrow::s3_bucket(bucket, path = key)

    flows <- arrow::read_parquet(s3_obj)
  }

  return(flows)
}


# Load flow data
flows <- get_flows_data(year, base_path)
message("Flow data loaded.")

popn_mye_path <- "s3://dpa-population-projection-data/population_mid_year_estimates/2022/population_gla_2022.rds"
births_mye_path <- "s3://dpa-population-projection-data/population_mid_year_estimates/2022/births_gla_2022.rds"
# Monitor memory usage
mem_used()

# Load data from S3 using s3readRDS
message("Loading population data...")
popn <- s3readRDS(popn_mye_path)
message("Population data loaded.")
mem_used()

message("Loading births data...")
births <- s3readRDS(births_mye_path)
message("Births data loaded.")
mem_used()

message("Loading component data from Parquet...")
# Read the Parquet file using arrow::read_parquet
flows <- arrow::read_parquet(dom_origin_destination_path)
message("Component data loaded.")
mem_used()

# message("Loading full modelled estimates series data...")
# full_modelled_estimates_series_ew <- s3readRDS(full_modelled_estimates_series_ew_path)
# message("Full modelled estimates series data loaded.")
# mem_used()

# Load the full data and then sample it
#popn <- s3readRDS(popn_mye_path)
#births <- s3readRDS(births_mye_path)


# Load data from local paths for testing

# popn_mye_path <- "/Users/user1/Documents/get_rate_backseries/input_data/population_gla_2022.rds"
# #popn_mye_path <- "/Users/user1/Documents/get_rate_backseries/input_data/population_gla_2022_filtered.rds"
# #popn_mye_path <- "/Users/user1/Documents/get_rate_backseries/input_data/2021/population_2021.rds"
# #births_mye_path <- "/Users/user1/Documents/get_rate_backseries/input_data/2021/births_2021.rds"
# births_mye_path <- "/Users/user1/Documents/get_rate_backseries/input_data/births_gla_2022.rds"
# #dom_origin_destination_path <- "/Users/user1/Documents/get_rate_backseries/input_data/2021/origin_destination_2002_2022_(2023_geog).rds"
# #dom_origin_destination_path <- "/Users/user1/Documents/get_rate_backseries/input_data/2021/origin_destination_2002_2022_(2023_geog)_reduced_mem.csv"
# dom_origin_destination_path <- "/Users/user1/Documents/get_rate_backseries/input_data/2021/origin_destination_2002_2022_(2023_geog)_reduced_mem_2021.csv"

# #flows <- readRDS(dom_origin_destination_path)
# flows <- read.csv(dom_origin_destination_path)
# popn <- readRDS(popn_mye_path)
# births <- readRDS(births_mye_path)

head(flows)
head(popn)
head(births)

# Add a year column to flows with the value 2021 as numeric
flows$year <- as.numeric(2021)

# summary(flows)

# Check object sizes
object.size(flows)
object.size(popn)
object.size(births)


check_and_remove_nans <- function(df, col_name) {
  if (any(is.na(df[[col_name]]))) {
    n_nans <- sum(is.na(df[[col_name]]))
    message(paste("Dataframe", deparse(substitute(df)), "Column", col_name, "contains", n_nans, "missing values. Removing these rows."))
    df <- df %>% filter(!is.na(df[[col_name]]))
  } else {
    message(paste("Dataframe", deparse(substitute(df)), "No missing values found in column", col_name))
  }
  return(df)
}

check_duplicates <- function(df) {
  dups <- duplicated(df)
  n_duplicates <- sum(dups)

  if (n_duplicates > 0) {
    message(sprintf("Dataframe %s contains %d duplicate rows.", deparse(substitute(df)), n_duplicates))
  } else {
    message(sprintf("Dataframe %s has no duplicate rows.", deparse(substitute(df))))
  }
}

#remove duplicate rows in df
remove_duplicates <- function(df) {
  message("Removing duplicates from DataFrame...")
  initial_count <- nrow(df)  
  df_no_duplicates <- df %>% distinct()  # Remove duplicates
  final_count <- nrow(df_no_duplicates)  
  duplicates_removed <- initial_count - final_count  
  
  cat("Rows removed:", duplicates_removed, "\n")
  cat("New row count:", final_count, "\n")
  
  return(df_no_duplicates)  # Ensure the cleaned DataFrame is returned
}

# check for negative values in df
check_negatives <- function(df, col_name) {
  if (any(df[[col_name]] < 0)) {
    n_negatives <- sum(df[[col_name]] < 0)
    message(paste("Dataframe", deparse(substitute(df)), "Column", col_name, "contains", n_negatives, "negative values."))
  } else {
    message(paste("Dataframe", deparse(substitute(df)), "No negative values found in column", col_name))
  }
}

# check for duplcates in flows, popn and births
message("Checking for duplicates in dfs...")
check_duplicates(flows)
check_duplicates(popn)
check_duplicates(births)   


#replace value name in column head with popn
names(popn)[names(popn) == "value"] <- "popn"
names(births)[names(births) == "value"] <- "births" 


#check negative values in popn 
message("Checking for negative values in dfs...")
check_negatives(popn, "popn")
check_negatives(births, "births")
check_negatives(flows, "value")

#replace negative values in popn with 0 in popn column
message("Replacing negative values in popn with 0...")
popn$popn[popn$popn < 0] <- 0

head(popn)
head(births)
head(flows)
# Define S3 paths for output data
output_path <- "s3://dpa-population-projection-data/population_mid_year_estimates/outputs/"

message("scenario domestic migration rates (2021 projections)")

# local paths

# popn_mye_path <- "input_data/mye/2021/population_gla.rds"
# births_mye_path <-  "input_data/mye/2021/births_gla.rds"
# dom_origin_destination_path <- "input_data/domestic_migration/2021/domestic_migration_flows_ons_(2021_geog).rds"
# dir.create("input_data/scenario_data", showWarnings = FALSE)
 
# popn <- readRDS(popn_mye_path) %>% 
#   filter(!substr(gss_code,1,3) %in% c("E12","E92","W92","E13"))

# births <- readRDS(births_mye_path) %>% 
#   filter(!substr(gss_code,1,3) %in% c("E12","E92","W92","E13"))

# flows <- readRDS(dom_origin_destination_path) %>% 
#   filter(!substr(gss_in, 1, 3) %in% c("E12","E92","W92","E13")) %>% 
#   filter(!substr(gss_out, 1, 3) %in% c("E12","E92","W92","E13"))

#-------------------------------------------------------------------------------
# Check data types in each column of flows
message("Checking data types in each column of flows...")
lapply(flows, class)

# check unique years in dfs
unique(flows$year)
unique(popn$year)
unique(births$year)

#domestic
message("Calculating domestic migration rates...")
rates_backseries <- get_rate_backseries(component_mye_path = flows,
                                        popn_mye_path = popn,
                                        births_mye_path = births,
                                        years_backseries = 2006:2021,
                                        col_partial_match = c("gss_out","gss_in"),
                                        col_aggregation = c("year","gss_code"="gss_out","gss_in","sex","age"),
                                        col_component = "value",
                                        rate_cap = NULL)

message("rates_backseries produced successfully")

dom_rates_avg_2017_2021 <- rates_backseries %>% 
  calculate_mean_domestic_rates(last_data_year = 2021,
                                n_years_to_avg = 1, #5,
                                col_rate = "rate",
                                rate_cap = 0.8)

dom_rates_avg_2012_2021 <- rates_backseries %>% 
  calculate_mean_domestic_rates(last_data_year = 2021,
                                n_years_to_avg = 1, #10,
                                col_rate = "rate",
                                rate_cap = 0.8)

dom_rates_avg_2007_2021 <- rates_backseries %>% 
  calculate_mean_domestic_rates(last_data_year = 2021,
                                n_years_to_avg = 1, #15,
                                col_rate = "rate",
                                rate_cap = 0.8)

#-------------------------------------------------------------------------------

# Save output data to S3
s3saveRDS(dom_rates_avg_2017_2021, object = paste0(output_path, "2021_dom_5yr_avg.rds"))
s3saveRDS(dom_rates_avg_2012_2021, object = paste0(output_path, "2021_dom_10yr_avg.rds"))
s3saveRDS(dom_rates_avg_2007_2021, object = paste0(output_path, "2021_dom_15yr_avg.rds"))


message("Script completed successfully. Outputs saved to S3.")
