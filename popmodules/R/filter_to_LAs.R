#' Filter a dataframe to standard model geography
#'
#' Filter a dataframe so that it includes only district-level data for England
#' and Wales and national-level data for Northern Ireland and Scotland
#'
#' @param df A dataframe to be filtered
#' @param gss_col String. The name of the column in df containing gss_codes.
#'   Default 'gss_code'.
#'
#' @import assertthat
#' @importFrom dtplyr lazy_dt
#' @import dplyr
#' @export

filter_to_LAs <- function(df, gss_col="gss_code") {

  assert_that(is.data.frame(df), msg = "In filter_to_LAs, the 'df' paramater is not a dataframe")
  assert_that(gss_col %in% names(df), msg = paste("In filter_to_LAs(),",gss_col,"not in input df"))
  
  codes <- c("E06", "E07", "E08", "E09", "W06", "W07", "W08", "N92", "S92")
  
  df <- dtplyr::lazy_dt(df) %>%
    rename(gss___ = !!gss_col) %>% 
    filter(substr(gss___, 1, 3) %in% codes) %>%
    as.data.frame() %>% 
    rename(!!gss_col := gss___)
  
  return(df)
  
}
