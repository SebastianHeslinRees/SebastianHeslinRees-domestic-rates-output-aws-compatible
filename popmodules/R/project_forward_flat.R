#' Extend a dataframe containing time series data to a year beyond the current
#' maximum year in the data
#' 
#' Project a population data frame forward by taking the most recent year and
#' repeating it for each year up to an input last_proj_yr. If the last_proj_yr
#' is less than or equal to the maximum year in the data the input dataframe
#' will be returned unchanged. 
#'
#' @param df A population data frame
#' @param last_proj_yr The last year of the projection
#' @param showWarnings Logical. If the function returns the input dataframe unchanged
#'  should this be reported. Default FALSE.
#'
#' @return A data frame containing the past years and the projection years
#'
#' @importFrom assertthat assert_that
#'
#' @export

project_forward_flat <- function(df, last_proj_yr, showWarnings = FALSE) {
  
  hold_yr <- max(df$year)
  first_proj_yr <- hold_yr+1
  
  if(hold_yr >= last_proj_yr){
    
    if(showWarnings) {
      
      warning(paste0("project_forward_flat was given a dataframe finishing in ",
                     hold_yr," and asked to project forward to ", last_proj_yr,
                     ".\nReturning the input dataframe unchanged."))
    }
    return(df)
  }
  
  # filter to the hold year
  hold_data <- dplyr::filter(df, year == hold_yr)
  
  # TODO check that the df is not empty
  # TODO validate popn
  # copy hold_yr into the projection yrs
  projection_yrs <- first_proj_yr:last_proj_yr
  projection <- lapply(projection_yrs, function(x) dplyr::mutate(hold_data, year = x))
  
  projection <- dplyr::bind_rows(df, projection)
  
  # TODO check that there aren't any missing years between the past data and the projections
  # TODO check that there aren't any duplicate years
  # TODO validate projection
  # TODO add module function rules checks - must return fert/mort df
  
  return(projection)
}