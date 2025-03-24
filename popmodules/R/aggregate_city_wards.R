#' Aggregate City of London ward data up to borough level 
#'
#' Sum data for individual wards in City of London up to the local authority
#' level. Return the dataframe with all other areas unchanged.
#'
#' @param df A dataframe containing ward-level data
#' @param data_col String. The column name of containing data in \code{df}
#' @param ward_lookup A dataframe. Ward to local authority gss code lookup
#'
#' @return A data frame with City of London wards aggregated to a single
#'   geographic aggregation
#' 
#' @import dplyr
#' @import assertthat
#' @importFrom dtplyr lazy_dt
#' 
#' @export

aggregate_city_wards <- function(df, data_col,
                                 ward_lookup = "input_data/lookup/2011_ward_to_district.rds"){
  
  col_aggregation <- setdiff(names(df), data_col)
  
  assertthat::assert_that(!"E09000001" %in% df$gss_code_ward,
                          msg="ward data already contains aggregated total for City of London")
  
  city_wards <- readRDS(ward_lookup) %>%
    filter(gss_code == "E09000001")
  
  city <- dtplyr::lazy_dt(df) %>%
    filter(gss_code_ward %in% city_wards$gss_code_ward) %>%
    as.data.frame() %>%
    mutate(gss_code_ward = "E09000001") %>%
    dtplyr::lazy_dt() %>%
    group_by(across(!!col_aggregation)) %>%
    summarise(value = sum(!!sym(data_col))) %>%
    as.data.frame() %>%
    rename(!!data_col := value)
  
  out_df <- filter(df, !gss_code_ward %in% city_wards$gss_code_ward) %>%
    rbind(city)
  
  return(out_df)
  
}
