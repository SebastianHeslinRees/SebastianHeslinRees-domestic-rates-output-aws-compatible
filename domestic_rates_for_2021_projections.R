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

devtools::install_github("smbache/loggr")



library(dplyr)
library(aws.s3)
library(assertthat)
library(data.table)
library(httr)
library(jsonlite)
library(dtplyr)
library(devtools)
library(pryr)
library(profvis)

Sys.setenv("AWS_DEFAULT_REGION" = "eu-west-2")

Sys.setenv(
AWS_ACCESS_KEY_ID = "",
AWS_SECRET_ACCESS_KEY = "",
AWS_DEFAULT_REGION = ""
)



# Set the working directory to the folder containing the popmodules
setwd(".")

# Load the package
load_all("popmodules")

message("Scenario domestic migration rates (2021 projections)")

# Define S3 paths for input data
popn_mye_path <- "s3://dpa-population-projection-data/population_mid_year_estimates/2022/population_gla_2022.rds"
births_mye_path <- "s3://dpa-population-projection-data/population_mid_year_estimates/2022/births_gla_2022.rds"
dom_origin_destination_path <- "s3://dpa-population-projection-data/population_mid_year_estimates/2022/origin_destination_2002_2022_(2023_geog).rds"
full_modelled_estimates_series_ew_path <- "s3://dpa-population-projection-data/population_mid_year_estimates/2022/full_modelled_estimates_series_EW(2023_geog).rds"

# Monitor memory usage
mem_used()

# # Load data from S3 using s3readRDS
# message("Loading population data...")
# popn <- s3readRDS(popn_mye_path)
# message("Population data loaded.")
# mem_used()

# message("Loading births data...")
# births <- s3readRDS(births_mye_path)
# message("Births data loaded.")
# mem_used()

# message("Loading component data...")
# flows <- s3readRDS(dom_origin_destination_path)
# message("Component data loaded.")
# mem_used()

# message("Loading full modelled estimates series data...")
# full_modelled_estimates_series_ew <- s3readRDS(full_modelled_estimates_series_ew_path)
# message("Full modelled estimates series data loaded.")
# mem_used()

# Load the full data and then sample it
#popn <- s3readRDS(popn_mye_path)
#births <- s3readRDS(births_mye_path)
flows <- s3readRDS(dom_origin_destination_path)


full_modelled_estimates_series_ew <- s3readRDS(full_modelled_estimates_series_ew_path)
#filter a sample of the data where year is 2021, to test on a smaller dataset
#flows <- flows %>% filter(year == 2021)
full_modelled_estimates_series_ew <- full_modelled_estimates_series_ew %>% filter(year == 2021) 

# Check object sizes
object.size(flows)
object.size(full_modelled_estimates_series_ew)

head(full_modelled_estimates_series_ew)
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

# show duplicate rows in df
check_duplicates <- function(df) {
  if (any(duplicated(df))) {
    n_duplicates <- sum(duplicated(df))
    message(paste("Dataframe", deparse(substitute(df)), "contains", n_duplicates, "duplicate rows."))
  } else {
    message(paste("Dataframe", deparse(substitute(df)), "No duplicate rows found."))
  }
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

#filter full_modelled_estimates_series_ew to get population and births dfs
popn <- full_modelled_estimates_series_ew %>% filter(component == "population")
births <- full_modelled_estimates_series_ew %>% filter(component == "births")
#mem size
object.size(popn)
object.size(births)

#replace value name in column head with popn
names(popn)[names(popn) == "value"] <- "popn"
names(births)[names(births) == "value"] <- "births" 

#check negative values in popn 
check_negatives(popn, "popn")
#replace negative values in popn with 0 in popn column
popn$popn[popn$popn < 0] <- 0

head(popn)
head(births)

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

#domestic
rates_backseries <- get_rate_backseries(component_mye_path = flows,
                                        popn_mye_path = popn,
                                        births_mye_path = births,
                                        years_backseries = 2021,
                                        col_partial_match = c("gss_out","gss_in"),
                                        col_aggregation = c("year","gss_code"="gss_out","gss_in","sex","age"),
                                        col_component = "value",
                                        rate_cap = NULL)

dom_rates_avg_2017_2021 <- rates_backseries %>% 
  calculate_mean_domestic_rates(last_data_year = 2021,
                                n_years_to_avg = 5,
                                col_rate = "rate",
                                rate_cap = 0.8)

dom_rates_avg_2012_2021 <- rates_backseries %>% 
  calculate_mean_domestic_rates(last_data_year = 2021,
                                n_years_to_avg = 10,
                                col_rate = "rate",
                                rate_cap = 0.8)

dom_rates_avg_2007_2021 <- rates_backseries %>% 
  calculate_mean_domestic_rates(last_data_year = 2021,
                                n_years_to_avg = 15,
                                col_rate = "rate",
                                rate_cap = 0.8)

#-------------------------------------------------------------------------------

# saveRDS(dom_rates_avg_2017_2021, "input_data/scenario_data/2021_dom_5yr_avg.rds")
# saveRDS(dom_rates_avg_2012_2021, "input_data/scenario_data/2021_dom_10yr_avg.rds")
# saveRDS(dom_rates_avg_2007_2021, "input_data/scenario_data/2021_dom_15yr_avg.rds")

# Save output data to S3
s3saveRDS(dom_rates_avg_2017_2021, object = paste0(output_path, "2021_dom_5yr_avg.rds"))
s3saveRDS(dom_rates_avg_2012_2021, object = paste0(output_path, "2021_dom_10yr_avg.rds"))
s3saveRDS(dom_rates_avg_2007_2021, object = paste0(output_path, "2021_dom_15yr_avg.rds"))

# Profile the script
profvis({
  source("domestic_rates_for_2021_projections.R")
})

message("Script completed successfully. Outputs saved to S3.")
