#' Apply rates to a population.
#'
#' Given a cohort population and a data frame of rates at the same or lower
#' resolution, return a data table of the population's aggregation levels, and a
#' component count resulting from the rate applied to the population.
#' 
#' The output dataframe will consist of the columns in the \code{popn} input
#' dataframe, plus any columns specified in \code{additional_rate_cols}, and the
#' output data column \code{col_out}. Output has 1 row for each distinct level of
#' the \code{col_aggregation}.
#'
#' @param popn A data frame containing population data
#' @param rates A data frame containing rates data
#' @param col_aggregation A string or character vector giving the names of
#'   columns on which to join (\code{by} in \code{dplyr::left_join}).
#'   If names differ between the two input data frames, use a named character
#'   vector, e.g. \code{c("gss_code"="LSOA11CD")}. Default \code{c("year",
#'   "gss_code", "age", "sex")}.
#' @param col_popn String. Name of column in \code{popn} containing population
#'   counts. Default "popn".
#' @param col_rate String. Name of column in \code{rates} containing rate
#'   data. Default "rate".
#' @param col_out String. Name of the resulting component count column (popn*rate)
#'   in the output dataframe. Default "component".
#' @param additional_popn_cols String. Names of columns in \code{popn}
#'   which should be included in the output but which are not included in
#'   \code{col_aggregation}. Used when multiple aggregation levels apply to the
#'   same rates. Default NULL.
#' @param additional_rate_cols String. Names of columns in \code{rates}
#'   which should be included in the output but which are not included in
#'   \code{col_aggregation}. Used when multiple rates apply to the same
#'   aggregation level, e.g. out migration to multiple locations. The resulting
#'   left join with \code{rates} can therefore return a much larger data
#'   frame. Default NULL.
#' @param validate_geog Boolean. Should the geography in the two input dataframes
#'   be checked using the \code{popmodules::validate_same_geog} function. Default FALSE.
#' @param col_geog String or NULL. The column containing geographic codes/names.
#'   Must be set if \code{validate_geog = TRUE}. Column must be present in both
#'   input dataframes. Default NULL.
#'
#' @return A data frame of component counts calculated as input popn * rate, with one row
#'   for each distinct level of the input \code{col_aggregation} columns.
#'
#' @import assertthat
#' @import dplyr
#' @importFrom rlang is_bool
#'
#' @export
#'

apply_rate_to_population <- function(popn,
                                     rates,
                                     col_aggregation = c("year", "gss_code", "sex", "age"),
                                     col_popn = "popn",
                                     col_rate = "rate",
                                     col_out = "component",
                                     additional_popn_cols = NULL,
                                     additional_rate_cols = NULL,
                                     validate_geog = FALSE,
                                     col_geog = NULL) {
  
  # Validate inputs
  # ---------------
  validate_input_apply_rate_to_population(popn, rates, col_aggregation,
                                          col_popn, col_rate, col_out,
                                          additional_popn_cols, additional_rate_cols,
                                          validate_geog, col_geog)
  
  
  join_by <- .convert_to_named_vector(col_aggregation)
  
  all_rate_cols <- c(as.character(join_by), additional_rate_cols, col_rate)
  all_popn_cols <- c(names(join_by), additional_popn_cols, col_popn)
  
  col_output_order <- c(setdiff(all_popn_cols, col_popn),
                        additional_rate_cols, col_out) 
  
  popn <- popn %>% 
    select_at(all_popn_cols) %>% 
    rename(popn__ = col_popn)
  
  rates <- rates %>% 
    select_at(all_rate_cols) %>%
    rename(rate__ = col_rate)
  
  # Make sure the columns that are factors match
  rates <- .match_factors(popn, rates, col_aggregation)
  
  # Apply rates
  output <- left_join(popn, rates, by = join_by) %>%
    mutate(component__ = popn__ * rate__) %>%
    rename(!!col_out := component__) %>% 
    select_at(col_output_order)
  
  return(output)
}


# ---------------------------------------------------------------------------


# Check the function input is valid
validate_input_apply_rate_to_population <- function(popn, rates, col_aggregation,
                                                    col_popn, col_rate, col_out,
                                                    additional_popn_cols, additional_rate_cols,
                                                    validate_geog, col_geog) {
  
  # Type checking
  assert_that(is.data.frame(popn),
              msg = "apply_rate_to_population needs a data frame as input")
  assert_that(is.data.frame(rates),
              msg = "apply_rate_to_population needs a data frame of rates data")
  assert_that(is.character(col_aggregation),
              msg = "apply_rate_to_population needs a string or character vector as the col_aggregation parameter")
  assert_that(is.string(col_popn),
              msg = "apply_rate_to_population needs a string as the col_popn parameter")
  assert_that(is.string(col_rate),
              msg = "apply_rate_to_population needs a string as the col_rate parameter")
  assert_that(is.string(col_out),
              msg = "apply_rate_to_population needs a string as the col_out parameter")
  assert_that(is.string(col_out),
              msg = "apply_rate_to_population needs a string as the col_out parameter")
  assert_that(identical(additional_rate_cols, NULL) | is.character(additional_rate_cols),
              msg = "additional_rate_cols should be NA or a character vector")
  assert_that(col_popn != col_rate,
              msg = "col_popn and col_rate cannot be the same")
  assert_that(is_bool(validate_geog),
              msg = "apply_rate_to_population needs a boolean as the validate_geog parameter")
  
  if(validate_geog){
    assert_that(!is.null(col_geog),
                msg = "apply_rate_to_population: col_geog parameter cannot be NULL if validate_geog = TRUE")
    assert_that(all(col_geog %in% names(popn)),
                msg = "apply_rate_to_population: col_geog not in popn dataframe")
    assert_that(all(col_geog %in% names(rates)),
                msg = "apply_rate_to_population: col_geog not in rates dataframe")
  }
  
  # Col_aggreagtion columns are in the input dataframes
  join_by <- .convert_to_named_vector(col_aggregation)
  assertthat::assert_that(all(names(join_by) %in% names(popn)),
                          msg = "in apply_rate_to_population join columns not in popn dataframe")
  
  assertthat::assert_that(all(as.character(join_by) %in% names(rates)),
                          msg = "in apply_rate_to_population join columns not in rates dataframe")
  
  
  #Additional columns are in the relevant dataframes
  if(!identical(additional_rate_cols, NA)) {
    assert_that(all(additional_rate_cols %in% names(rates)),
                msg = "in apply_rate_to_population some columns named in additional_rate_cols not in rates dataframe")
  } else {
    additional_rate_cols <- NULL
  }
  
  if(!identical(additional_popn_cols, NA)) {
    assert_that(all(additional_popn_cols %in% names(popn)),
                msg = "in apply_rate_to_population some columns named in additional_popn_cols not in popn dataframe")
  } else {
    additional_popn_cols <- NULL
  }
  
  #output column name doesn't exist elsewhere
  all_rate_cols <- c(as.character(join_by), additional_rate_cols)
  all_popn_cols <- c(names(join_by), additional_popn_cols)
  
  assertthat::assert_that(!col_out %in% all_popn_cols,
                          msg = "in apply_rate_to_population col_out cannot exist in input pop dataframe")
  assertthat::assert_that(!col_out %in% all_rate_cols,
                          msg = "in apply_rate_to_population col_out cannot exist in input rates dataframe")
  
  assert_that(!col_popn %in% names(col_aggregation),
              msg = "apply_rate_to_population was given a population count column name that is also a named aggregation column")
  assert_that(!col_rate %in% as.character(col_aggregation),
              msg = "apply_rate_to_population was given a rate column name that is also a named aggregation column")
  
  assert_that(!any(duplicated(names(col_aggregation))),
              msg = "duplicated population column names were provided to apply_rate_to_population")
  assert_that(!any(duplicated(as.character(col_aggregation))),
              msg = "duplicated rates column names were provided to apply_rate_to_population")
  assert_that(any(col_aggregation %in% names(rates)),
              msg = "in apply_rate_to_population, no aggregation column names were found in rates - at least one must be present")
  assert_that(!col_rate %in% names(col_aggregation),
              msg = "apply_rate_to_population can't have a col_rate that is also a named aggregation column in the input")
  assert_that(is.numeric(popn[[col_popn]]),
              msg = paste("apply_rate_to_population needs a numeric column in the specified population count col:", col_popn))
  assert_that(is.numeric(rates[[col_rate]]),
              msg = paste("apply_rate_to_population needs a numeric column in the specified rates rate col:", col_rate))
  assert_that(!col_out %in% names(col_aggregation),
              msg = paste("apply_rate_to_population can't handle an output count column with the same name as one of the aggregation columns",
                          "\nOutput column:", col_out,
                          "\nAggregation columns:", col_aggregation))
  
  assert_that(length(join_by) > 0,
              msg = "apply_rate_to_population must share some aggregation column names with the input rates, or a column mapping must be included in the col_aggregation parameter")
  
  #Geography codes/names match
  if(validate_geog){
    validate_same_geog(popn, rates, col_geog, col_geog)
  }
  
  invisible(TRUE)
  
}

