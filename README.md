# Domestic rates output - Key Changes and Comments compared to orginal code base in population projection model

This document outlines key modifications and improvements made to the population projection model so it can be containerised

## 1. Function Naming and Duplication Issue

- The function `age_pop_on` has been renamed to `age_pop_on_2`. However, the existence of two versions without explanation is confusing. It would be beneficial to clarify why both versions are retained, or age_pop_on should be removed 

## 2. Data Pre-Processing Intergrated into container 

- Added basic data pre-processing steps within the script:
  - Checking and removing duplicates
  - Handling NaN (missing values)
  - Filtering out negative values
- Added memory usage checks to ensure efficiency.
- Renamed column headings for improved readability and consistency.

## 3. Validation Adjustments

- Removed certain validation checks to allow the model to work with selected year(s) only.
  ```r
  # Removed assertions to allow flexibility in year selection
  assert_that(all(years_backseries %in% population$year),
              msg = "get_rate_backseries expects the population dataframe to contain all of the years in years_backseries")

  assert_that(all(years_backseries %in% births$year),
              msg = "get_rate_backseries expects the births dataframe to contain all of the years in years_backseries")

  assert_that(all(years_backseries %in% component$year),
              msg = "get_rate_backseries expects the component dataframe to contain all of the years in years_backseries")
  ```

## 4. Handling Merge Errors

- To prevent errors during merging, added `allow.cartesian=TRUE`:
  ```r
  if(FALSE) { # Tidyverse approach
    rates <- left_join(popn, component, by=col_aggregation, allow.cartesian=TRUE) %>%
      mutate(rate = ifelse(popn == 0, 0, !!sym(col_component)/popn)) %>%
      select(-popn, -!!sym(col_component)) %>%
      rename(setNames(names(join_by), unname(join_by))) # Check this...
  }
  ```

## 5. Consistency in Variable Naming

- Clarified potential confusion between `years_backseries` and `backseries_years`. They appear to serve the same purpose but use different names in various parts of the script.
- Example:
  ```r
  # Domestic rate calculation
  rates_backseries <- get_rate_backseries(
    component_mye_path = flows,
    popn_mye_path = popn,
    births_mye_path = births,
    years_backseries = 2021,
    col_partial_match = c("gss_out","gss_in"),
    col_aggregation = c("year","gss_code"="gss_out","gss_in","sex","age"),
    col_component = "value",
    rate_cap = NULL
  )
  ```

  ```r
  # Validation check
  backseries_years <- (last_data_year - n_years_to_avg + 1):last_data_year
  assert_that(all(backseries_years %in% origin_destination_rates$year),
              msg = paste(c("calculate_mean_domestic_migration_rates expects these years to be present in the origin-destination data:",
                            backseries_years), collapse=" "))
  ```

## 6. Avoiding Hardcoded Year Values

-   This should not be hardcoded. Update to `dom_rates_avg_2017_2021` this will be change to dynamically use input year values instead of hardcoding them.
- Ensuring that if the input year changes, the calculations adjust accordingly.

---

