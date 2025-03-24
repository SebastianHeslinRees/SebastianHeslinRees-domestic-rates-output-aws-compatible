#' Template births function that returns a fixed number of births (default zero)
#' for each aggregation level, and sets their age to zero
#'
#' This is designed to be used as a placeholder births function during model
#' testing and a template for other births functions, along with its tests.
#'
#' @param pop A data frame containing population data
#' @param col_aggregation A string giving the names of columns to which the
#'   output births will be aggregated to. Default \code{c("gss_code","sex",
#'   "age")}
#' @param const Numeric. Number of births to return per geography. Defaults to
#'   zero, but can be set to any positive number
#' @param col_age A string giving the name of the age column, if it exists in
#'   the input, or if it needs to be created for the output. It needn't be an
#'   aggregation level.
#'
#' @return A data frame of births with one row for each distinct value of the
#'   \code{col_aggregation} columns, a column named births with value
#'   \code{const} and a column named \code{col_age} with value 0.
#'
#' @importFrom assertthat assert_that
#' @importFrom stats complete.cases
#'
#' @examples
#'
#' pop <- expand.grid(year = 2000, age=20:23, gss_code=c("a","b","c"), sex=c("f","m"), popn = 100)
#'
#' pop_births <- null_births(pop,
#'                           col_aggregation = c("year", "gss_code", "age", "sex"),
#'                           const = 0,
#'                           col_age = "age")
#'
#' # equivalent to
#' pop_births <- null_births(pop)
#'
#' @export

null_births <- function(pop,
                        col_aggregation = c("year", "gss_code", "age", "sex"),
                        const = 0,
                        col_age = "age") {

  validate_births_input(pop, col_aggregation, const, col_age)
  col_aggregation <- col_aggregation[ col_aggregation != col_age] # validate_births_input warns if this is necessary
  col_aggregation <- names(pop)[ names(pop) %in% col_aggregation ] # reorder to match column ordering in pop
  output_cols <- c(col_aggregation, col_age, "births")
  
  births <- dplyr::group_by_at(pop, .vars = col_aggregation) %>%
    dplyr::summarise(births = const, !!sym(col_age) := 0) %>%
    data.frame() %>% 
    select_at(output_cols)

  validate_births_output(pop, col_aggregation, births, col_age)

  return(births)
}




# ---------------------------------------------------------------------------

# Check the function input is valid
validate_births_input <- function(pop, col_aggregation, const, col_age) {

  assert_that(is.data.frame(pop),
              msg = "null_births needs a data frame as input")
  assert_that(is.character(col_aggregation),
              msg = "null_births needs a character vector as the col_aggregation parameter")
  assert_that(is.numeric(const) && length(const) == 1,
              msg = "null_births needs a single numeric value as the const parameter")
  assert_that(const >= 0,
              msg = "null_births needs a positive value of the const parameter")
  assert_that(is.string(col_age),
              msg = "null_births needs a string as the col_age parameter")
  assert_that(all(col_aggregation %in% names(pop)),
              msg = paste(c("null_births was given column name(s) not present in the input population data",
                            "\nColumn names provided:",col_aggregation,
                            "\nColumn names of input population:",names(pop)),
                          collapse=" "))
  assert_that(!"births" %in% col_aggregation,
              msg = "births cannot currently be used as an aggregation column in null_births: the name is reserved for output. If this is really important to you, update the function to give a customisable name to the output births column.")
  if("births" %in% names(pop)) {
    warning("births is a column name in the input to null_births: the output births column will contain calculated births and will probably mess with any binds or joins!!")
  }
  if(any(duplicated(col_aggregation))) {
    warning("duplicated column names were provided to null_births: these will be removed")
    col_aggregation <- unique(col_aggregation)
  }

  validate_population(pop,
                      col_aggregation = col_aggregation,
                      test_complete = TRUE,
                      test_unique = TRUE,
                      check_negative_values = TRUE)

  if(nrow(pop) == 0) {
    warning("null_births was given an empty input table")
  }

  # If we were working with a fertility dataset as well, we would validate its aggregation levels
  # and check a join is possible with validate_join_population()
}




# Check the function output isn't doing anything unexpected
validate_births_output <- function(pop, col_aggregation, births, col_age) {

  assert_that(all(col_aggregation %in% names(births)))

  assert_that("births" %in% names(births))
  assert_that(col_age %in% names(births))

  assert_that(all(complete.cases(births)))

  validation_comparison_cols <- setdiff(col_aggregation, col_age)

  validate_join_population(pop,
                           births[validation_comparison_cols],
                           cols_common_aggregation = validation_comparison_cols,
                           aggregation_levels_match = FALSE,
                           many2one = TRUE,
                           one2many = FALSE)
}
