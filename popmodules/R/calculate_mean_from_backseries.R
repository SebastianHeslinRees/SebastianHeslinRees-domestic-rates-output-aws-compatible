#' Calculate averages based on past data
#'
#' Given a backseries of data the function will return an average.
#'
#' @param data_backseries Dataframe. A set of rates or flows by LA, sex and age.
#' @param n_years_to_avg numeric. Number of years data to include in the
#'   average.
#' @param last_data_year numeric. The final year of data to include in the
#'   average.
#' @param data_col Character. The name of the column containing the data.
#' @param col_aggregation Character. The columns to group_by.
#' @param project_rate_from Numeric. The year for which the average being
#'   calculated is to be used. Default \code{last_data_year+1}.
#'
#' @return A data frame of mortality probabilities, fertility rates, or
#'   international migration flows/rates.
#'
#' @import dplyr
#' @importFrom assertthat assert_that
#'
#' @export

calculate_mean_from_backseries <- function(data_backseries, n_years_to_avg, last_data_year,
                                           data_col, col_aggregation = c("gss_code","sex"),
                                           project_rate_from = last_data_year+1){

  assert_that(is.data.frame(data_backseries),
              msg="calc_trend_rate expects that data_backseries is a dataframe")
  assert_that(is.numeric(n_years_to_avg),
              msg="calc_trend_rate expects that n_years_to_avg is an numeric")
  assert_that(is.numeric(last_data_year),
              msg="calc_trend_rate expects that last_data_year is an numeric")
  assert_that(is.character(data_col),
              msg="calc_trend_rate expects that data_col is a column")
  assert_that(data_col %in% names(data_backseries),
              msg = "in calc_trend_rate the rate_col variable must be the name of a column in the data_backseries dataframe")

  back_years <- c((last_data_year - n_years_to_avg + 1):last_data_year)

  assert_that(all(back_years %in% data_backseries$year),
              msg = "calculate_mean_from_backseries expects the backseries dataframe to contain the data years specified by n_years_to_avg and last_data_year")

  averaged <- data_backseries %>%
    rename(value = data_col) %>%
    filter(year %in% back_years) %>%
    group_by(across(!!col_aggregation)) %>%
    summarise(value = sum(value)/n_years_to_avg) %>%
    ungroup() %>%
    mutate(year = project_rate_from) %>% 
    select_at(c("year", col_aggregation, "value")) %>%
    rename(!!data_col := value) %>% 
    data.frame()

  return(averaged)

}

