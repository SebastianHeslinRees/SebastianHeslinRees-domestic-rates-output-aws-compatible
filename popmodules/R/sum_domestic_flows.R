#' Convert origin-destination flow data to gross flow by origin or destination
#'
#' Sum origin-destination flows to give total flows by either origin or destination.
#' The type of flow is specified in the \code{in_or_out} parameter. The function
#' expects column names \code{gss_in} and \code{gss_out}. Net flows cannot be
#' calculated using this function.
#'
#' @param domestic_flow A data frame containing sya/sex flows between geographic areas
#' @param in_or_out A string indicating whether the flows should be summed for
#'  the destination (in) or origin (out) area.
#' @param flow_col A string. The name of the column containing the flow data.
#'  Default \code{flow}
#'
#' @return A dataframe with flows into or out of an area summed
#'
#' @import dplyr
#' @importFrom dtplyr lazy_dt
#' @importFrom tidyr complete
#' @importFrom assertthat assert_that
#' 
#' @export

sum_domestic_flows <- function(domestic_flow, in_or_out, flow_col = "flow"){

  assert_that(in_or_out %in% c("in", "out"),
              msg = "In sum_domestic_flows in_or_out must be either in or out")
  assert_that(flow_col %in% names(domestic_flow),
              msg = "In sum_domestic_flows flow_col must be in domestic_flow dataframe")
  
  gss_col <- ifelse(in_or_out == "in", "gss_in", "gss_out")
  
  data_col <- ifelse(in_or_out == "in", "dom_in", "dom_out")
  
  col_aggregation <- c("year", gss_col, "sex", "age")
  
  dom <- dtplyr::lazy_dt(domestic_flow) %>%
    rename(flow = !!flow_col) %>%
    group_by(across(!!col_aggregation)) %>%
    summarise(flow = sum(flow)) %>%
    as.data.frame() %>%
    rename(gss_code = !!gss_col) %>%
    tidyr::complete(year, gss_code, age=0:90, sex, fill=list(flow=0)) %>%
    rename(!!data_col := flow) %>%
    as.data.frame()
  
  return(dom)
}
