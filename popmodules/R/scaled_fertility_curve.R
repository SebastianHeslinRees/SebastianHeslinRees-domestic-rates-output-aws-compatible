#' Calculate average fertility rates based on past data and assign to first
#' projection year
#'
#' Compares past rates by LA, age and sex to an input
#' curve to derive a set of scaling factors. The scaling factors are then
#' averaged or trended forward using regression. The resulting scaling
#' factor is applied to the input curve to produce a set of rates.
#'
#' @param popn String or data frame. Path to population data, or a
#'   data frame containing the data.
#' @param births String or data frame. Path to births data, or a 
#'   data frame with the data.
#' @param target_curves String or data frame. Path to the SNPP
#'   target fertility curves, or a data frame with the data.
#' @param last_data_year Numeric. The last year of births data on which to
#'   calculate averages.
#' @param n_years_to_avg Numeric. The number of years to use in calculating
#'   averages/trend forward.
#' @param avg_or_trend Character. Should the averaged be calculated as the mean
#'   \code{Average}, or by linear regression \code{Trend}.
#' @param data_col Character. The column in the \code{births} dataframe
#'   containing the rates. Defaults to \code{births}
#' @param output_col Character. The name of the column in the output dataframe
#'   containing the calculated rates
#' @param project_rate_from Numeric. The year for which a rate is being
#'   calculated. Default \code{last_data_year+1}
#' @param col_geog Character. The column containing geographic codes or names
#'
#' @return A data frame of fertility fertility rates by LA, year, sex and age
#'  with the same age structure as the target curves and overall rates scaled
#'  so that they are consistent with past births.
#'
#' @import dplyr
#' @import assertthat
#' @importFrom dtplyr lazy_dt
#'
#' @export

scaled_fertility_curve <- function(popn,
                                   births,
                                   target_curves,
                                   last_data_year,
                                   n_years_to_avg,
                                   avg_or_trend,
                                   data_col="births",
                                   output_col,
                                   project_rate_from = last_data_year+1,
                                   col_geog = "gss_code"){
  
  
  population <- .path_or_dataframe(popn)
  births <- .path_or_dataframe(births)
  target_curves <- .path_or_dataframe(target_curves)
  
  validate_scaled_fertility_curve_input(population, births, target_curves,
                                        last_data_year, n_years_to_avg,
                                        avg_or_trend, data_col, output_col)
  
  #remove year column if it exists
  target_curves <- target_curves[!names(target_curves)=="year"]
  
  col_aggregation <- unique(c("year", "gss_code", col_geog))
  
  births <- lazy_dt(births) %>%
    group_by(across(!!col_aggregation)) %>%
    summarise(births = sum(births)) %>%
    data.frame()
  
  assert_that(n_years_to_avg <= length(unique(births$year)),
              msg = paste("scaled_fertility_curve couldn't construct", n_years_to_avg, "years of fertility data from the inputs"))
  
  aged_on_population <-  population %>%
    filter(sex == "female", age %in% unique(target_curves$age)) %>%
    popn_age_on(births = NULL,
                col_aggregation = c(col_aggregation, "age", "sex"),
                col_geog = col_geog)
  
  target_curves <- filter(target_curves, gss_code %in% unique(births$gss_code))
  
  # Calculate the total births per year for each geography and sex that the target fertility curve would would create from population
  # Compare to the total births per year for each geography and sex in the actual births
  # *scaling* tells you what you would need to scale each geog and sex of the target curve by to get the same total births as the actuals
  # This is done because we prefer the ONS age structure for the first projection year to the previous actuals age structures
  scaling_backseries <- aged_on_population %>% 
    lazy_dt() %>%
    right_join(target_curves, by = c("gss_code", "age", "sex")) %>%
    mutate(curve_count = rate * popn) %>%
    group_by(across(!!col_aggregation)) %>%
    summarise(curve_count = sum(curve_count)) %>%
    filter(year %in% births$year) %>%
    ungroup() %>%
    data.frame() %>%   # dtplyr needs to take a break part way through
    lazy_dt() %>%
    left_join(births, by = col_aggregation)  %>%
    data.frame() %>%
    rename(actual = data_col) %>%
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
    left_join(target_curves, by = c("gss_code")) %>%
    mutate(jump_off_rate = scaling * rate,
           sex = "female") %>%
    select_at(c(col_aggregation, "sex", "age", "jump_off_rate")) %>%
    rename(!!output_col := jump_off_rate) %>% 
    data.frame()
  
  fertility_rates <- left_join(scaling_backseries, target_curves, by=c("gss_code")) %>%
    filter(!is.na(scaling)) %>%
    mutate(rate = rate*scaling) %>%
    select_at(c(col_aggregation, "sex", "age", "rate")) %>%
    rename(!!output_col := rate) %>%
    filter(year < project_rate_from) %>%
    rbind(jump_off_rates)  %>%
    arrange_at(c("year", col_aggregation, "age"))
  
  return(fertility_rates)
  
}

#--------------------------------------------------------------------

validate_scaled_fertility_curve_input <- function(population, births, target_curves, last_data_year, n_years_to_avg,
                                                  avg_or_trend, data_col, output_col){
  
  # test input parameters are of the correct type
  
  assert_that(is.numeric(last_data_year),
              msg="scaled_fertility_curve expects that last_data_year is an numeric")
  assert_that(is.numeric(n_years_to_avg),
              msg="scaled_fertility_curve expects that n_years_to_avg is an numeric")
  assert_that(is.character(avg_or_trend),
              msg="scaled_fertility_curve expects that avg_or_trend is a character")
  assert_that(is.character(data_col),
              msg="scaled_fertility_curve expects that data_col is a character")
  assert_that(is.character(output_col),
              msg="scaled_fertility_curve expects that output_col is a character")
  assert_that(data_col %in% names(births),
              msg="scaled_fertility_curve expects that data_col is a column in births dataframe")
  
  if("year" %in% names(target_curves)){
    assert_that(length(unique(target_curves$year))==1,
                msg = "scaled_mortality_curve: target_curves dataframe has data for more than 1 year. This is unacceptable.")
  }
  
  invisible(TRUE)
}


