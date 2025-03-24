#' Calculate a residual between populations and convert to gross migration flows
#'
#' A method of reconciling the difference between a population and a target
#' constraint. Rather than scaling the whole population to match, the difference
#' is used to adjust gross migration flows.
#'
#' The function calculates the difference between a population and a (lower
#' resolution) target, and then returns updated gross in- and out-migration
#' flows that include the residual.
#'
#' @param popn A data frame containing a population.
#' @param target A data frame containing a population to be constrained to.
#'   Must share column names with \code{popn} to enable a join.
#' @param col_aggregation Character vector. Names of columns to join \code{popn}
#'   to \code{target}.
#' @param inflow_df A data frame containing migration inflows.
#'  It will be scaled to include additional inflows.
#' @param outflow_df A data frame containing migration outflows.
#'  It will be scaled to include additional outflows.
#' @param col_popn A string with the name of the column containing population
#'   counts in \code{popn}
#' @param col_target A string with the name of the column containing population
#'   counts in \code{target}
#' @param col_inflow A strings with the name of the column containing inflow
#'   counts in \code{inflow_df}. Default 'dom_in'
#' @param col_outflow A strings with the name of the column containing inflow
#'   counts in \code{outflow_df}. Default 'dom_out'
#' @param rows_to_constrain A logical vector limiting the rows of \code{popn}
#'   where this function is applied. Must be all TRUE or FALSE in each grouping
#'   level specified by \code{col_aggregation}.
#'
#' @return A list of two data frames. \code{inflow} contains the input domestic
#'   inflows scaled to include the calculated positive residuals. \code{outflow}
#'   contains the input domestic outflows scaled to include the
#'   calculated negative residuals.
#'
#' @import dplyr
#'
#' @export

adjust_gross_migration <- function(popn, target, inflow_df, outflow_df,
                                      col_aggregation = c("year","gss_code"),
                                      col_popn, col_target,
                                      col_inflow = "dom_in", col_outflow = "dom_out",
                                      rows_to_constrain = TRUE){

  validate_join_population(popn[rows_to_constrain,], target, cols_common_aggregation = col_aggregation,
                           many2one = TRUE, one2many = FALSE, warn_unused_shared_cols = FALSE)

  #Do some renaming
  col_popn_new <- paste0(col_popn, ".x")
  col_target_new  <- paste0(col_target,  ".y")
  popn <- rename(popn, !!col_popn_new := !!col_popn)
  target <- rename(target, !!col_target_new := !!col_target)

  #find diff between popn and target
  domestic_adjustment <- popn %>%
    dtplyr::lazy_dt() %>%
    group_by(across(!!col_aggregation)) %>%
    summarise(!!col_popn_new := sum(!!sym(col_popn_new))) %>%
    as.data.frame() %>%
    dtplyr::lazy_dt() %>%
    left_join(target, by=col_aggregation) %>%
    mutate(net_dom_adjustment = !!sym(col_target_new) - !!sym(col_popn_new)) %>%
    select(c(col_aggregation, net_dom_adjustment)) %>%
    as.data.frame()

  #if the change is positive then it's an addition to in migration
  #if negative then add to out migration
  gross_in_adjustment <- domestic_adjustment %>%
    mutate(net_dom_adjustment = ifelse(net_dom_adjustment > 0, net_dom_adjustment, 0))
  gross_out_adjustment <- domestic_adjustment %>%
    mutate(net_dom_adjustment = ifelse(net_dom_adjustment < 0, -1 * net_dom_adjustment, 0))

  inflow_target <- inflow_df %>%
    rename(inflow__ = !!col_inflow) %>% 
    dtplyr::lazy_dt() %>%
    group_by(across(!!col_aggregation)) %>%
    summarise(inflow_orig = sum(inflow__)) %>%
    as.data.frame() %>%
    dtplyr::lazy_dt() %>%
    left_join(gross_in_adjustment, by = col_aggregation) %>%
    mutate(inflow_target = inflow_orig + net_dom_adjustment)  %>%
    select(c(col_aggregation, inflow_target)) %>%
    as.data.frame()

  outflow_target <- outflow_df %>%
    rename(outflow__ = !!col_outflow) %>% 
    dtplyr::lazy_dt() %>%
    group_by(across(!!col_aggregation)) %>%
    summarise(outflow_orig = sum(outflow__)) %>%
    as.data.frame() %>%
    dtplyr::lazy_dt() %>%
    left_join(gross_out_adjustment, by = col_aggregation) %>%
    mutate(outflow_target = outflow_orig + net_dom_adjustment)  %>%
    select(c(col_aggregation, outflow_target)) %>%
    as.data.frame()

  assert_that(all(complete.cases(inflow_target)))
  assert_that(all(complete.cases(outflow_target)))

  #Adjust the gross flows
  adjusted_inflow <- constrain_component(popn = inflow_df,
                                         constraint = inflow_target,
                                         col_aggregation = col_aggregation,
                                         col_popn = col_inflow,
                                         col_constraint = "inflow_target",
                                         rows_to_constrain = rows_to_constrain)

  adjusted_outflow <- constrain_component(popn = outflow_df,
                                          constraint = outflow_target,
                                          col_aggregation = col_aggregation,
                                          col_popn = col_outflow,
                                          col_constraint = "outflow_target",
                                          rows_to_constrain = rows_to_constrain)

  return(list(inflow = adjusted_inflow,
              outflow = adjusted_outflow))

}

