#' Use linear regression to calculate future values based on past data
#'
#' Given a backseries of values the function will return an future value
#'
#' @param data_backseries Dataframe. A set of values by LA, sex and age
#' @param n_years_regression numeric. Number of years data to include in the regression calculation
#' @param last_data_year numeric. The final year of data to include in the regression calculation
#' @param data_col Character. The name of the column containing the values
#' @param col_aggregation Character. The columns to group_by
#' @param project_rate_from Numeric. The year for which the rate being
#'   calculated is to be used. Default \code{last_data_year+1}.
#'
#' @return A data frame of mortality probabilities, fertility rates, or
#' international migration flows/rates.
#'
#' @import dplyr
#' @importFrom assertthat assert_that
#' @importFrom purrr map map_dbl
#' @importFrom stats lm
#'
#' @export

calculate_rate_by_regression <- function(data_backseries, n_years_regression, last_data_year, data_col,
                                         col_aggregation=c("gss_code","sex"),
                                         project_rate_from = last_data_year+1){

  assert_that(is.data.frame(data_backseries),
              msg="calculate_rate_by_regression expects that data_backseries is a dataframe")
  assert_that(is.numeric(n_years_regression),
              msg="calculate_rate_by_regression expects that n_years_regression is an numeric")
  assert_that(is.numeric(last_data_year),
              msg="calculate_rate_by_regression expects that last_data_year is an numeric")
  assert_that(is.character(data_col),
              msg="calculate_rate_by_regression expects that data_col is a column")
  assert_that(data_col %in% names(data_backseries),
              msg = "in calculate_rate_by_regression the rtae_col variable must be the name of a column in the data_backseries dataframe")

  back_years <- c((last_data_year - n_years_regression + 1):last_data_year)
  assert_that(all(back_years %in% data_backseries$year),
              msg = "calculate_rate_by_regression expects the backseries dataframe to contain the data years specified by n_years_regression and last_data_year")

  regression <- function(df){
    lm(value ~ year, data=df)
  }

  get_coef <- function(df){
    coef(df)
  }


  trended <- as.data.frame(data_backseries) %>%
    rename(value = data_col) %>%
    filter(year %in% back_years) %>%
    mutate(year = n_years_regression - last_data_year + year)%>%
    group_by(across(!!col_aggregation)) %>%
    tidyr::nest() %>%
    mutate(
      Model = map(data, regression),
      Coef = Model %>% map(get_coef),
      Intercept = Coef %>% map_dbl("(Intercept)"),
      Slope = Coef %>% map_dbl("year"),
      value = Slope * (n_years_regression+1) + Intercept,
      value = ifelse(value < 0, 0, value)) %>%
    as.data.frame()  %>%
    mutate(year = project_rate_from) %>%
    select_at(c("year", col_aggregation, "value")) %>%
    rename(!!data_col := value)

  return(trended)

}
