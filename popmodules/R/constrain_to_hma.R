#' Scale one population to match the totals of another in a housing market area
#'
#' Given a cohort population and a data frame of target marginal population
#' subtotals, return the input population scaled so that its population
#' subtotals match the target's at each grouping level.
#'
#' @param popn A data frame containing population data.
#' @param constraint A data frame containing population data at the same
#'   resolution or lower.
#' @param hma_list A named list where the name is an housing market area and the
#'  elements are vectors of the gss_codes inside that HMA
#' @param col_aggregation A string or character vector giving the join mapping between
#'   \code{popn} and \code{constraint}. Equivalent to \code{by} in \code{dplyr} joins
#' @param col_popn String. Name of column in \code{popn} containing population
#'   counts. Default "popn".
#' @param col_constraint String. Name of column in \code{constraint} containing population
#'   counts. Default col_popn.
#'
#' @return A data frame of component counts calculated as input popn * rate, with one row
#'   for each distinct level of the input \code{popn} dataframe
#'
#' @import dplyr
#' @importFrom tibble enframe
#' @importFrom tidyr unnest
#' 
#' @export

constrain_to_hma <- function(popn, constraint, hma_list,
                             col_aggregation = c("year","hma","sex","age"),
                             col_popn, col_constraint = col_popn){


  if("hma" %in% names(popn)) {
    warning("constrain_to_hma ignoring hma column in input popn")
    popn <- select(popn, -hma)
  }

  if(!"data.frame" %in% class(hma_list)) {
    hma_list <- tibble::enframe(hma_list, "hma", "gss_code") %>% tidyr::unnest(cols = c("hma","gss_code")) %>% as.data.frame
  }
  hma_list <- .match_factors(popn, hma_list, col_mapping = "gss_code")

  if(!"hma" %in% names(constraint)) {
    assertthat::assert_that("gss_code" %in% names(constraint))

    constraint <- left_join(hma_list, constraint, by="gss_code") %>% # remove areas outside of HMAs
      dtplyr::lazy_dt() %>%
      group_by(across(!!col_aggregation)) %>%
      summarise(!!col_constraint := sum(!!sym(col_constraint))) %>%
      as.data.frame()
  }

  scaled_popn <- popn %>%
    left_join(hma_list, by="gss_code") %>%
    constrain_component(constraint = constraint,
                        col_aggregation = col_aggregation,
                        col_popn = col_popn,
                        col_constraint = col_constraint,
                        rows_to_constrain = popn$gss_code %in% hma_list$gss_code) %>%
    select(names(popn)) %>%
    as.data.frame()

  return(scaled_popn)

}
