#' Template deaths function that returns a fixed number of deaths (default zero)
#' for each aggregation level.
#'
#' This is designed to be used as a placeholder deaths function during model
#' testing and a template for other deaths functions, along with its tests.
#'
#' @param pop A data frame containing population data
#' @param col_aggregation A string giving the names of columns to which the
#'   output deaths will be aggregated to. Default \code{c("year", "gss_code",
#'   "age", "sex")}
#' @param col_popn String. Name of column with population counts. Default "popn"
#' @param const Numeric. Number of deaths to return per geography. Defaults to
#'   zero, but can be set to any positive number
#' @param error_negative_pop Logical. Throw error if deaths result in a negative
#'   population count. Setting to FALSE creates a warning instead. Default TRUE.
#'
#' @return A data frame of deaths with one row for each distinct value of the
#'   \code{col_aggregation} columns, a column named deaths with value
#'   \code{const} and a column named age with value 0.
#'
#' @importFrom assertthat assert_that
#' @importFrom stats complete.cases
#'
#' @export
#'
null_deaths <- function(pop,
                        col_aggregation = c("year", "gss_code", "age", "sex"),
                        col_popn="popn",
                        const = 0,
                        error_negative_pop = TRUE) {

  validate_deaths_input(pop, col_aggregation, col_popn, const, error_negative_pop)
  col_aggregation <- names(pop)[ names(pop) %in% col_aggregation] # order to match input ordering, remove duplicates

  # Calculate deaths
  deaths <- dplyr::mutate(pop, deaths = const)

  # Check for negative population
  ix_negative <- deaths[["deaths"]] > deaths[[col_popn]]
  if(any(ix_negative) & error_negative_pop) {
    stop(paste("null_deaths created more deaths than population in", sum(ix_negative), "rows"))
  }
  if(any(ix_negative) & !error_negative_pop) {
    warning(paste("null_deaths created more deaths than population in", sum(ix_negative), "rows.",
                  "Adjusting deaths to prevent negative population"))
    deaths[ix_negative, "deaths"] <- deaths[ix_negative, col_popn]
  }

  # Select the columns we want to output
  deaths <- deaths[c(col_aggregation, "deaths")]

  validate_deaths_output(pop, col_aggregation, col_popn, error_negative_pop, deaths)

  deaths
}




# ---------------------------------------------------------------------------

# Check the function input is valid
validate_deaths_input <- function(pop, col_aggregation, col_popn, const, error_negative_pop) {

  assert_that(is.data.frame(pop),
              msg = "null_deaths needs a data frame as input")
  assert_that(is.character(col_aggregation),
              msg = "null_deaths needs a string or character vector as the col_aggregation parameter")
  assert_that(is.string(col_popn),
              msg = "null_deaths needs a string as the col_popn parameter")
  assert_that(is.numeric(const) && length(const) == 1,
              msg = "null_deaths needs a single numeric value as the const parameter")
  assert_that(const >= 0,
              msg = "null_deaths needs a positive value of the const parameter")
  assert_that(is.logical(error_negative_pop),
              msg = "null_deaths needs a TRUE/FALSE value for the error_negative_pop parameter")
  assert_that(!col_popn %in% col_aggregation,
              msg = "null_deaths was given a population col_popn column that is also a named aggregation column")
  assert_that(!"deaths" %in% col_aggregation,
              msg = "null_deaths can't handle an aggregation column called also deaths. If this is really important to you, update the function to give a customisable name to the output deaths column.")
  if("deaths" %in% names(pop)) {
    warning("deaths is already a column name in the input to null_deaths. The output will contain the calculated deaths instead and this will probably muck up subsequent joins or binds!!")
  }

  if(any(duplicated(col_aggregation))) {
    warning("duplicated column names were provided to null_deaths: these will be removed")
    col_aggregation <- unique(col_aggregation)
  }

  validate_population(pop,
                      col_aggregation = col_aggregation,
                      col_data = col_popn,
                      test_complete = TRUE,
                      test_unique = TRUE,
                      check_negative_values = TRUE)

  if(nrow(pop) == 0) {
    warning("null_deaths was given an empty input table")
  }

  # If we were working with a mortality dataset as well, we would validate its aggregation levels
  # and check a join is possible with validate_join_population()
}




# Check the function output isn't doing anything unexpected
validate_deaths_output <- function(pop, col_aggregation, col_popn, error_negative_pop, deaths) {

  assert_that(all(col_aggregation %in% names(deaths)))

  assert_that("deaths" %in% names(deaths))

  assert_that(all(complete.cases(deaths)))

  validate_join_population(pop,
                           deaths,
                           cols_common_aggregation = col_aggregation,
                           aggregation_levels_match = FALSE,
                           many2one = TRUE,
                           one2many = FALSE)

}
