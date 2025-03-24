#' Calculate average origin-destination domestic migration rates based on past
#' data
#'
#' Given a dataframe of domestic migration rates calculate the mean rate over a
#' given number of years. If the sum of the out rates for any age/sex/LA combination
#' is greater than a defined cap then scale all rates so that sum is equal to the cap.
#'
#' @param origin_destination_rates dataframe. Origin-destination flows by geography, year, sex and age.
#' @param last_data_year numeric. The last year of data on which to calculate
#'   averages.
#' @param n_years_to_avg numeric. The number of years to use in calculating averages.
#' @param col_rate character. The column in the \code{origin_destinatation_flows}
#'   dataframe containing the rates
#' @param rate_cap numeric. A value which calculated rates cannot exceed.
#'   Where rates exceed \code{rate_cap} they are limited to the value of \code{rate_cap}.
#'
#' @return A data frame of origin-destination migration probabilities by LA, year, sex and age.
#'
#' @import dplyr
#' @import assertthat
#' @importFrom utils capture.output
#' @importFrom dtplyr lazy_dt
#'
#' @export

calculate_mean_domestic_rates <- function(origin_destination_rates,
                                           last_data_year,
                                           n_years_to_avg,
                                           col_rate = "rate",
                                           rate_cap = 0.8) {
  # validate input
  check_average_domestic_rate_input(origin_destination_rates, last_data_year, n_years_to_avg, col_rate)
  
  if(col_rate != "rate"){origin_destination_rates <- rename(origin_destination_rates, rate = !!col_rate)}
  
  backseries_years <- (last_data_year - n_years_to_avg + 1):last_data_year
  assert_that(length(backseries_years) == n_years_to_avg)
  
  pre_average_rates <- origin_destination_rates %>% 
    dtplyr::lazy_dt() %>% 
    filter(year %in% backseries_years) %>% 
    group_by(gss_out, gss_in, age, sex) %>%
    summarise(rate = rate/n_years_to_avg)
  
  outflow_sums <- pre_average_rates %>% 
    group_by(gss_out, sex, age) %>% 
    summarise(total_rate = sum(rate)) %>% 
    data.frame()
  
  #check if any areas would have > cap % of persons leaving
  if(any(outflow_sums$total_rate > rate_cap)) {
    ix <- outflow_sums$total_rate > rate_cap
    n <- sum(ix)
    
    #print a warning show what has been capped
    warning(paste0(capture.output({
      print(paste(c("After averaging the backseries, average_domestic_migration rates created rates with sums exceeding the cap of", rate_cap,
                    "at", n, "aggregation levels - these will be scaled to sum to", rate_cap),
                  collapse = " "))
      if(sum(ix) < 30) {
        print("Values:")
        print(outflow_sums[ix,])
      } else {
        print("First 30 values:")
        print(outflow_sums[ix,][1:30,])
      }
    }), collapse = "\n"))
    
    #do the capping - first set scaling factors for the affected areas
    outflow_sums <- outflow_sums %>% 
      dtplyr::lazy_dt() %>% 
      mutate(scaling = ifelse(total_rate > rate_cap, rate_cap/total_rate, 1))
    
    #apply scaling factors
    pre_average_rates <- pre_average_rates %>% 
      left_join(outflow_sums, by = c("gss_out", "age", "sex")) %>% 
      mutate(rate = rate*scaling) 
  }
  
  #sum the individual year rates
  average_rates <- pre_average_rates %>% 
    dtplyr::lazy_dt() %>% 
    group_by(gss_out, gss_in, age, sex) %>% 
    summarise(!!col_rate := sum(rate)) %>% 
    data.frame() %>% 
    arrange(gss_out, gss_in, sex, age)
  
  #check missing levels
  unmatched_levels <- setdiff(
    expand.grid(gss_out = unique(average_rates$gss_out),
                age = unique(average_rates$age),
                sex = unique(average_rates$sex),
                stringsAsFactors = FALSE),
    average_rates %>%
      dtplyr::lazy_dt() %>%
      select(gss_out, age, sex) %>%
      unique() %>%
      as.data.frame()
  )
  
  if(nrow(unmatched_levels) > 0) {
    warning(paste(c("calculate_mean_domestic_migration_rates found ", nrow(unmatched_levels), " aggregation levels with no net outmigration.",
                    " These levels will be absent from the output.")), sep=" ")
  }
  
  return(average_rates)
  
}




# ================================================================

# validate inputs for the above
check_average_domestic_rate_input <- function(origin_destination_rates, last_data_year, n_years_to_avg, col_rate) {
  
  assert_that(is.data.frame(origin_destination_rates),
              msg="calculate_mean_domestic_migration_rates expects that origin_destination_rates is a data frame")
  assert_that(is.count(last_data_year),
              msg="calculate_mean_domestic_migration_rates expects that last_data_year is a numeric")
  assert_that(is.count(n_years_to_avg),
              msg="calculate_mean_domestic_migration_rates expects that n_years_to_avg is a numeric")
  assert_that(is.character(col_rate),
              msg="calculate_mean_domestic_migration_rates expects that col_rate is a character")
  assert_that(col_rate %in% names(origin_destination_rates),
              msg="calculate_mean_domestic_migration_rates expects that col_rate is a column in component_data dataframe")
  
  backseries_years <- (last_data_year - n_years_to_avg + 1):last_data_year
  assert_that(all(backseries_years %in% origin_destination_rates$year),
              msg = paste(c("calculate_mean_domestic_migration_rates expects these years to be present in the origin-destination data:",
                            backseries_years), collapse=" "))
}
