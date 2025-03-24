#' Calculate average mortality probabilities based on past data and assign to
#' first projection year
#'
#' Compares past rates by LA and sex to an input
#' curve to derive a set of scaling factors. The scaling factors are then
#' averaged or trended forward using regression. The resulting scaling
#' factor is applied to the age specific input curve to produce a set of rates.
#'
#' @param popn Character. Path to the MYE population data
#' @param births Character. Path to the MYE births component data
#' @param deaths Character. Path to the MYE deaths component data
#' @param target_curves Character. Path to the SNPP target mortality curves
#' @param last_data_year numeric. The last year of death and population data on
#'   which to calculate averages.
#' @param n_years_to_avg numeric. The number of years to use in calculating
#'   averages/trending forward.
#' @param avg_or_trend Character. Should the averaged be calculated as the mean
#'   \code{Average}, or by linear regression \code{Trend}.
#' @param data_col Character. The column in the \code{deaths} dataframe containing
#'   the rates. Defaults to \code{deaths}
#' @param output_col Character. The name of the column in the output dataframe
#'   containing the calculated rates
#' @param project_rate_from Numeric. The year for which a rate is being calculated.
#'   Default \code{last_data_year+1}.
#' @param col_aggregation Character. Deafult c("gss_code","year","sex")
#' @param col_geog Character. Defaultr "gss_code"
#'
#' @return A data frame of mortality probabilities rates by LA, year, sex and age
#'   with the same age structure as the target curves and overall rates scaled so
#'   that they are consistent with past deaths.
#'
#' @import dplyr
#' @import assertthat
#'
#' @export


scaled_mortality_curve <- function(popn, births, deaths,
                                   target_curves, last_data_year,
                                   n_years_to_avg, avg_or_trend,
                                   data_col="deaths", output_col,
                                   project_rate_from = last_data_year+1,
                                   col_aggregation = c("gss_code","year","sex"),
                                   col_geog = "gss_code"){
  
  population <- .path_or_dataframe(popn)
  births <- .path_or_dataframe(births)
  deaths <- .path_or_dataframe(deaths)
  target_curves <- .path_or_dataframe(target_curves)
  
  validate_scaled_mortality_curve_inputs(population, births, deaths, target_curves,
                                         last_data_year, n_years_to_avg,
                                         avg_or_trend, data_col, output_col)
  
  #remove year column if it exists
  target_curves <- target_curves[!names(target_curves)=="year"]
  
  #Deaths denominator
  aged_on_population <- popn_age_on2(popn = population,
                 births=births,
                 col_aggregation=c(col_aggregation, "age"),
                 col_geog = col_geog) %>%
    select_at(c(col_aggregation, "age", "popn")) %>%
    arrange_at(c(col_aggregation, "age"))
  
  # Calculate the total deaths per year for each geography and sex that the target mortality curve would would create from population
  # Compare to the total deaths per year for each geography and sex in the actual deaths
  # *scaling* tells you what you would need to scale each geog and sex of the target curve by to get the same total deaths as the actuals
  # This is done because we prefer the ONS age structure for the first projection year to the previous actuals age structures
  
  scaling_backseries <- left_join(aged_on_population, target_curves, by = c("gss_code", "age", "sex")) %>%
    mutate(curve_count = rate * popn) %>%
    left_join(deaths, by = c(col_aggregation, "age")) %>%
    rename(value = !!data_col) %>%
    group_by(across(!!col_aggregation)) %>%
    summarise(actual = sum(value),
              curve_count = sum(curve_count),
              .groups = 'drop_last') %>%
    as.data.frame() %>%
    mutate(scaling = ifelse(curve_count == 0,
                            0,
                            actual / curve_count)) %>%
    select_at(c(col_aggregation, "scaling"))
  
  if(avg_or_trend == "trend"){
    averaged_scaling_factors <- calculate_rate_by_regression(scaling_backseries,
                                                             n_years_regression = n_years_to_avg,
                                                             last_data_year = last_data_year,
                                                             data_col = "scaling",
                                                             col_aggregation =  col_aggregation[col_aggregation != "year"],
                                                             project_rate_from = project_rate_from)
  }
  
  if(avg_or_trend == "average"){
    averaged_scaling_factors <- calculate_mean_from_backseries(scaling_backseries,
                                                               n_years_to_avg = n_years_to_avg,
                                                               last_data_year = last_data_year,
                                                               data_col = "scaling",
                                                               col_aggregation = col_aggregation[col_aggregation != "year"],
                                                               project_rate_from = project_rate_from)
  }
  
  jump_off_rates <- averaged_scaling_factors %>%
    left_join(target_curves, by = c("gss_code", "sex")) %>%
    mutate(jump_off_rate = scaling * rate) %>%
    select_at(c(col_aggregation, "age", "jump_off_rate")) %>%
    rename(!!output_col := jump_off_rate) %>% 
    data.frame()
  
  mortality_rates <- left_join(scaling_backseries, target_curves, by=c("gss_code","sex")) %>%
    filter(!is.na(scaling)) %>%
    mutate(rate = rate*scaling) %>%
    select_at(c(col_aggregation, "age", "rate")) %>%
    rename(!!output_col := rate) %>%
    filter(year < project_rate_from) %>%
    rbind(jump_off_rates)  %>%
    arrange_at(c("year", col_aggregation, "age"))
  
  return(mortality_rates)
  
}

#-----------------------------------------------

# Function to check that the input to scaled_mortality_curves is all legal
validate_scaled_mortality_curve_inputs <- function(popn, births, deaths, target_curves,
                                                   last_data_year, n_years_to_avg,
                                                   avg_or_trend, data_col, output_col){
  
  # test input parameters are of the correct type
  
  assert_that(is.numeric(last_data_year),
              msg="scaled_mortality_curve expects that last_data_year is an numeric")
  assert_that(is.numeric(n_years_to_avg),
              msg="scaled_mortality_curve expects that n_years_to_avg is an numeric")
  assert_that(is.character(avg_or_trend),
              msg="scaled_mortality_curve expects that avg_or_trend is a character")
  assert_that(is.character(data_col),
              msg="scaled_mortality_curve expects that data_col is a character")
  assert_that(is.character(output_col),
              msg="scaled_mortality_curve expects that output_col is a character")
  assert_that(data_col %in% names(deaths),
              msg="scaled_mortality_curve expects that data_col is a column in deaths dataframe")
  
  if("year" %in% names(target_curves)){
    assert_that(length(unique(target_curves$year))==1,
                msg = "scaled_mortality_curve: target_curves dataframe has data for more than 1 year. This is unacceptable.")
  }
  
  invisible(TRUE)
  
}

