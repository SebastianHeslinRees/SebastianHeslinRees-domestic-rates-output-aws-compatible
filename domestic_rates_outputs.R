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

# Sys.setenv(
# AWS_ACCESS_KEY_ID = "",
# AWS_SECRET_ACCESS_KEY = "",
# AWS_DEFAULT_REGION = ""
# )


# Set the working directory to the folder containing the popmodules 
setwd(".")

# Load the package
load_all("popmodules")

s3_config_path <- "s3://dpa-population-projection-data/population_mid_year_estimates/configs/backseries_rates_config.yml"
# Download config from S3
local_config <- tempfile()
save_object(object = s3_config_path, file = local_config)

# Read the config from downloaded file
cfg <- config::get(file = local_config)


# Load config
#cfg <- config::get(file = "config/config.yml")

get_base_s3_path <- function(path_template) {
  str_replace(path_template, "year=\\{year\\}/?", "")
}

# Main function to read parquet data
read_parquet_from_s3 <- function(cfg) {
  year <- cfg$year
  path_template <- cfg$dom_origin_destination_path
  
  if (tolower(year) == "all") {
    base_path <- get_base_s3_path(path_template)
    message("Reading data for ALL years from base path: ", base_path)
    
    dataset <- open_dataset(base_path, format = "parquet")
  } else {
    s3_path <- gsub("\\{year\\}", year, path_template)
    message("Reading data for year: ", year)
    message("From path: ", s3_path)
    
    dataset <- open_dataset(s3_path, format = "parquet")
  }

  # Collect the dataset into a data frame (regardless of which path is taken)
  df <- dataset %>% collect()
  return(df)
}


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
flows <- read_parquet_from_s3(cfg)
message("Component data loaded.")
mem_used()


head(flows)
head(popn)
head(births)


#replace value name in column head with popn
names(popn)[names(popn) == "value"] <- "popn"
names(births)[names(births) == "value"] <- "births" 

# Conditionally add a year column
if (tolower(cfg$year) != "all") {
  numeric_year <- as.numeric(cfg$year)
  flows$year <- numeric_year
} else {
  numeric_year <- c(min(flows$year), max(flows$year))
}

# Check object sizes
object.size(flows)
object.size(popn)
object.size(births)


#Preprocess the data
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

# check unique years in dfs
message("Checking unique years in dfs...")
unique(flows$year)
unique(popn$year)
unique(births$year)

message(paste0("domestic migration rates backseries for flows:", cfg$year))

#domestic migration rates backseries
rates_backseries <- get_rate_backseries(component_mye_path = flows,
                                        popn_mye_path = popn,
                                        births_mye_path = births,
                                        years_backseries = numeric_year,
                                        col_partial_match = c("gss_out","gss_in"),
                                        col_aggregation = c("year","gss_code"="gss_out","gss_in","sex","age"),
                                        col_component = "value",
                                        rate_cap = NULL)

head(rates_backseries)

#-------------------------------------------------------------------------------
output_path <- "s3://dpa-population-projection-data/population_mid_year_estimates/outputs/parquet_backseries_rates/"

# Write the dataframe to S3 partitioned by year
write_dataset(
  dataset = rates_backseries,
  path = output_path,
  format = "parquet",
  partitioning = "year"
)

message("Script completed successfully. Outputs saved to S3.")

