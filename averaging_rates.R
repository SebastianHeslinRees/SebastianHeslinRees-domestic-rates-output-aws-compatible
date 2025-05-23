message("Starting script to calculate domestic rates averages...")

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


# Set the working directory to the folder containing the popmodules 
setwd(".")

# Load the package
load_all("popmodules")

# Load s3 dataset 
message("Loading backseries rates dataset...")
# Full dataset for deployment
# rates_backseries <- arrow::open_dataset("s3://dpa-population-projection-data/population_mid_year_estimates/outputs/parquet_backseries_rates/") %>%
#   dplyr::collect()
# Sample dataset for testing locally 
rates_backseries <- s3readRDS(
  object = "population_mid_year_estimates/outputs/averaged_rates/rates_backseries_sample.rds",
  bucket = "dpa-population-projection-data"
)
message("Loading backseries rates dataset done.")


#view head of the dataset
head(rates_backseries)
# Check the structure of the dataset and memory usage
str(rates_backseries)

#number of rows and columns
message("Number of rows and columns in the dataset:")
message(paste0("Rows: ", nrow(rates_backseries)))
message(paste0("Columns: ", ncol(rates_backseries)))

#memory usage of the dataset
message("Memory usage of the dataset:")
message(pryr::object_size(rates_backseries))

rates_backseries <- rates_backseries %>%
  group_by(year, age) %>%
  slice_sample(n = 5) %>%  # randomly sample 5 rows per group
  ungroup()

# save sample to S3
s3saveRDS(rates_backseries, object = "s3://dpa-population-projection-data/population_mid_year_estimates/outputs/averaged_rates/rates_backseries_sample.rds")

# Take lastest year of data
last_data_year <- max(rates_backseries$year)

# Calculate the start year for the average
start_year_for_5year_avg <- last_data_year - 5 + 1
start_year_for_10year_avg <- last_data_year - 10 + 1
start_year_for_15year_avg <- last_data_year - 15 + 1

# Create the dynamic variable name
var_name_5y_avg <- paste0("dom_rates_avg_", start_year_for_5year_avg, "_", last_data_year)
var_name_10y_avg <- paste0("dom_rates_avg_", start_year_for_10year_avg, "_", last_data_year)
var_name_15y_avg <- paste0("dom_rates_avg_", start_year_for_15year_avg, "_", last_data_year)


# Calculate the average and assign to the dynamic variable name

message("Calculating 5-year average...")


df_5_year_avg <- rates_backseries %>% 
  calculate_mean_domestic_rates(last_data_year = last_data_year,
                                n_years_to_avg = 5,
                                col_rate = "rate",
                                rate_cap = 0.8)

message("Calculating 5 year average done.")


message("Calculating 10-year average...")

df_10_year_avg <- rates_backseries %>% 
  calculate_mean_domestic_rates(last_data_year = last_data_year,
                                n_years_to_avg = 10,
                                col_rate = "rate",
                                rate_cap = 0.8)

message("Calculating 10 year average done.")

message("Calculating 15-year average...")

df_15_year_avg <- rates_backseries %>% 
  calculate_mean_domestic_rates(last_data_year = last_data_year,
                                n_years_to_avg = 15,
                                col_rate = "rate",
                                rate_cap = 0.8)

message("Calculating 15 year average done.")

# Save the output to S3

output_path <- "s3://dpa-population-projection-data/population_mid_year_estimates/outputs/averaged_rates/"

# Construct the dynamic file name
file_name_5y <- paste0(last_data_year, "_dom_5yr_avg.rds")
file_name_10y <- paste0(last_data_year, "_dom_10yr_avg.rds")
file_name_15y <- paste0(last_data_year, "_dom_15yr_avg.rds")

# Save output data to S3
message("Saving 5-year average to S3...")
s3saveRDS(df_5_year_avg, object = paste0(output_path, file_name_5y))
message("Saving 10-year average to S3...")
s3saveRDS(df_10_year_avg, object = paste0(output_path, file_name_10y))
message("Saving 15-year average to S3...")
s3saveRDS(df_15_year_avg, object = paste0(output_path, file_name_15y))

message("Saved all files average to S3 done.")

message("script finished.")