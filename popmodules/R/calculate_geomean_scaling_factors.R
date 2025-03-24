#' Calculate a set of scaling factors to apply to fertility or mortality rates
#'
#' For a backseries of years, given population for small areas and
#' fertility/mortality rates for a higher geography, calculate a set of
#' implied births/deaths. Compare these to observed births/deaths and
#' create scaling factors for the rates. Finally, calculate the geometric
#' mean of the scaling factors.
#'
#' The function calculates the difference between a population and a (lower
#' resolution) target, and then returns updated domestic in- and outmigration
#' rates that include the residual.
#'
#' @param popn A data frame containing a population at small area (ward or msoa)
#'   by sex and age.
#' @param future_rates A data frame containing mortality or fertility rates at
#'   local authority level by sex and age.
#' @param data_years Numeric vector of years to average.
#' @param constraint A dataframe of total births or deaths for each small area.
#' @param constraint_data_col A string giving the column with data in the
#'   constraint file.
#'
#' @return A dataframe of scaling factors by sex and age for each small area.
#'
#' @import dplyr
#'
#' @export

# TODO tests
calculate_geomean_scaling_factors <- function(popn, future_rates, data_years, constraint, constraint_data_col){

  validate_geomean_scaling_factors_inputs(popn, future_rates, data_years, constraint, constraint_data_col)

  component_from_rate <- popn_age_on(as.data.frame(popn),
                                     col_aggregation = c("year", "gss_code_small_area", "age", "sex"),
                                     col_geog = "gss_code_small_area") %>%
    filter(year %in% data_years) %>%
    dtplyr::lazy_dt() %>%
    left_join(unique(select(popn, gss_code, gss_code_small_area)), by="gss_code_small_area") %>% 
    left_join(future_rates, by=c("gss_code","sex","age")) %>%
    mutate(value_from_rate = rate*popn) %>%
    group_by(year, gss_code, gss_code_small_area) %>%
    summarise(value_from_rate = sum(value_from_rate)) %>%
    as.data.frame()

  scaling_dataframe <- component_from_rate %>%
    calculate_scaling_factors(constraint = constraint,
                              col_aggregation = c("year","gss_code_small_area"),
                              col_popn = "value_from_rate",
                              col_constraint = constraint_data_col,
                              rows_to_constrain = TRUE) %>%
    dtplyr::lazy_dt() %>%
    group_by(gss_code_small_area) %>%
    summarise(scaling = exp(mean(log(scaling)))) %>% # geometric mean (we know all +ve and no NAs)
    as.data.frame()

  return(scaling_dataframe)
}



validate_geomean_scaling_factors_inputs <- function(popn, future_rates, data_years, constraint, constraint_data_col) {

  validate_population(popn, col_aggregation = c("year", "gss_code_small_area", "age", "sex"), col_data = "popn",
                      test_complete = TRUE, test_unique = TRUE, check_negative_values = TRUE)
  validate_population(future_rates, col_aggregation = c("gss_code", "age", "sex"), col_data = "rate",
                      test_complete = TRUE, test_unique = TRUE, check_negative_values = TRUE)
  validate_population(constraint, col_aggregation = c("year", "gss_code_small_area"), col_data = constraint_data_col,
                      test_complete = TRUE, test_unique = TRUE, check_negative_values = TRUE)
  
  validate_join_population(popn, future_rates, cols_common_aggregation = c("gss_code", "age", "sex"), one2many=FALSE)

  assertthat::assert_that(all(data_years %in% (popn$year+1)),  # +1 because we age on
                          msg = "calculate_geomean_scaling_factors was given a popn dataframe missing some expected years")
  assertthat::assert_that(all(data_years %in% constraint$year),
                          msg = "calculate_geomean_scaling_factors was given a popn dataframe missing some expected years")
  as.data.frame(popn) %>%
    filter(year %in% (data_years-1)) %>%
    validate_join_population(constraint, cols_common_aggregation = c("year", "gss_code_small_area"), one2many=FALSE)

  # This is maybe too strict - we could just exclude zeroes from the calculation, but our backseries doesn't have any zeroes so ¯\_(o-o)_/¯
  assertthat::assert_that(!any(constraint[[constraint_data_col]] == 0),
    msg = "calculate_geomean_scaling_factors was passed birth rates of zero")
}
