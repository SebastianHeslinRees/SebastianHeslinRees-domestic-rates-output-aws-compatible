#' Scale a set of cross-border flows to match the totals of another
#'
#' A wrapper for \code{get_scaling_factor} which prepares origin-destination
#' flow data, constraining flows into of England to match the target data frame
#' in \code{in_constraint} and flows out of England to another the target data
#' frame in \code{out_constraint}.
#'
#' @param domestic_flow A data frame containing origin-destination flows
#' @param in_constraint A data frame containing cross-border inflow totals at
#'   the same resolution or lower.
#' @param out_constraint A data frame containing cross-border outflow totals at
#'   the same resolution or lower.
#' @param col_flow String. Name of column in \code{domestic_flow} containing
#'   population counts. Default "flow"
#'
#' @return A data frame of scaled domestic flows by authority, sex and single
#'   year of age.
#'
#' @import dplyr
#' @importFrom assertthat assert_that
#'
#' @export

constrain_cross_border <- function(domestic_flow, in_constraint, out_constraint, col_flow = "flow"){

  cols <- names(domestic_flow)

  #this isn't the prettiest way of doing this but its the fastest I can find
  domestic_flow <- domestic_flow %>%
    mutate(country_in = ifelse(substr(gss_in,1,1) == "E", "E", "X"),
           country_out = ifelse(substr(gss_out,1,1) == "E", "E", "X"),
           scale_in =  country_in == "E" & country_out == "X",
           scale_out = country_in == "X" & country_out == "E")

  in_constraint$country_out = "X"
  out_constraint$country_in = "X"

  #Scale flows
  domestic_flow <- domestic_flow %>%
    constrain_component(constraint = in_constraint,
                        col_aggregation = c("year", "sex", "age", "country_in"="country", "country_out"),
                        col_popn = col_flow,
                        col_constraint = "cross_in",
                        rows_to_constrain = domestic_flow$scale_in) %>%
    constrain_component(constraint = out_constraint,
                        col_aggregation = c("year", "sex", "age", "country_out"="country", "country_in"),
                        col_popn = col_flow,
                        col_constraint = "cross_out",
                        rows_to_constrain = domestic_flow$scale_out) %>%
    select(!!cols)

  return(domestic_flow)
}
