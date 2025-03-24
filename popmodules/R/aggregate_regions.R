#' Aggregate district-level data to regional totals
#'
#' @param x A data frame containing population/component data. The last column
#'   is assumed to contain the data.
#' @param gss_col String with the column name to join to the regional lookup's
#'   \code{gss_code} column. Default \code{"gss_code".}
#' @param col_aggregation A string of column names (other than geography) to
#'   group by when aggregating to region. Default \code{c("year", "age",
#'   "sex")}.
#' @param england Logical indicating whether or not an England total should also
#'   be calculated. Default \code{FALSE}
#' @param lookup String. File path for a gss_code to region gss code lookup
#'
#' @return The input data frame with additional rows for English regions and
#'   Wales
#'
#' @import dplyr
#'
#' @export

aggregate_regions <- function(x,
                              gss_col = "gss_code",
                              col_aggregation = c("year", "age", "sex"),
                              england = FALSE,
                              lookup = "input_data/lookup/district_to_region.rds"){
  

  lookup <- readRDS(lookup)
  
  col_aggregation <- union(col_aggregation, "region_gss_code")
  
  data_col <- last(names(x))

  if(any(c("E12", "W92") %in% substr(x$gss_code, 1, 3))) {
    warning("aggregate_regions called on data frame with regional England and Wales data. These won't be recalculated.")
    output_df <- x
    
  } else {
  
    regions <- x %>%
      rename(value := !!data_col) %>%
      dtplyr::lazy_dt() %>%
      filter(substr(gss_code,1,1) %in% c("E","W")) %>% 
      left_join(lookup, by = setNames("gss_code", gss_col)) %>%
      group_by(across(!!col_aggregation)) %>% 
      summarise(value = sum(value)) %>%
      as.data.frame() %>%
      rename(gss_code = region_gss_code,
             !!data_col := value)
    
    output_df <- rbind(x, regions)
  }
  
  if(england){
    
    if("E92" %in% substr(x$gss_code, 1, 3)) {
      warning("aggregate_regions called on data frame with national England data. This won't be recalculated.")

    } else {
      
      eng <- x %>%
        rename(value := !!data_col) %>%
        dtplyr::lazy_dt() %>%
        filter(substr(gss_code,1,3) %in% c("E06","E07","E08","E09")) %>% 
        mutate(region_gss_code = "E92000001") %>%
        group_by(across(!!col_aggregation)) %>% 
        summarise(value = sum(value)) %>%
        as.data.frame() %>%
        rename(gss_code = region_gss_code,
               !!data_col := value)
      
      output_df <- rbind(output_df, eng)
    }   
  }
  
  return(data.frame(output_df))
  
}
