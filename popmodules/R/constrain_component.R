#' Scale one population to match the totals of another
#'
#' Given a cohort population and a data frame of target marginal population
#' subtotals, return the input population scaled so that its population
#' subtotals match the target's at each grouping level.
#'
#' @param popn A data frame containing population data.
#' @param constraint A data frame containing population data at the same
#'   resolution or lower.
#' @param col_aggregation A string or character vector giving the join mapping between
#'   \code{popn} and \code{constraint}. Equivalent to \code{by} in \code{dplyr} joins
#' @param col_popn String. Name of column in \code{popn} containing population
#'   counts. Default "popn".
#' @param col_constraint String. Name of column in \code{constraint} containing population
#'   counts. Default col_popn.
#' @param rows_to_constrain Which rows of the input population are to be
#'   constrained. This can be provided as a logical vector (e.g. \code{TRUE} for
#'   all rows or \code{c(TRUE, TRUE, FALSE)} or using dplyr's non-standard
#'   evaluation, e.g. for a population with a \code{gss_code} column,
#'   \code{gss_code == "E090000001"} for City of London, or \code{grepl("^E09",
#'   gss_code)} for all of London. Default TRUE.
#'
#' @return A data frame of component counts calculated as input popn * rate, with one row
#'   for each distinct level of the input \code{popn} dataframe
#'
#' @import assertthat
#' @import dplyr
#'
#' @examples
#'
#' popn <- expand.grid(year=2000, age=20:21, gss_code=c("a","b"), sex=c("f","m"), popn = 100)
#' constraint <- expand.grid(year=2000, age=20:21, sex=c("f","m"), country = "a", popn = 400)
#'
#' scaled <- constrain_component(popn,
#'                          constraint,
#'                          col_aggregation = c("year", "sex", "age"),
#'                          col_popn = "popn",
#'                          col_constraint = "popn")
#'
#' @export
#'

# TODO add nesting!

constrain_component <- function(popn,
                                constraint,
                                col_aggregation = c("year", "sex", "age", "country"),
                                col_popn,
                                col_constraint = col_popn,
                                rows_to_constrain = TRUE) {

  assertthat::assert_that(sum(is.na(filter(popn, rows_to_constrain)))==0,
                              msg="in constrain_component, there are NA values present in the popn data frame")

  scaling_factors <- calculate_scaling_factors(popn, constraint,
                                               col_aggregation = col_aggregation,
                                               col_popn = col_popn,
                                               col_constraint = col_constraint,
                                               rows_to_constrain = rows_to_constrain)

  scaled_popn <- scaling_factors %>%
    mutate(!!sym(col_popn) := !!sym(col_popn) * scaling) %>%
    select_at(names(popn))

  return(scaled_popn)
}

