#' Check that the number of rows in a dataframe is consistent with the expected
#' number of rows
#' 
#' Note that this is a crude test and only checks that \code{nrow(x)} is consistent
#' with the expected number of rows. It says nothing about the content of those
#' rows. Failure of the test indicated incompleteness. Passing the test does
#' not indicate completeness but can be used as a crude proxy.
#' 
#' @param x A dataframe
#' @param test_cols Character. The aggregation levels to be tested
#' @param nested_cols Character. Any of the aggregation levels in \code{completed_cols} 
#'   which are nested. For example geography columns.
#' @param warn_only Logical. If TRUE the function will return a warning, if FALSE
#'  an error is produced. Default FALSE.
#' 
#' @import dplyr
#' 
#' @export

validate_complete <- function(x,
                              test_cols = c("year","gss_code","gss_code_ward","sex","age"),
                              nested_cols = c("gss_code", "gss_code_ward"),
                              warn_only = FALSE){
  
  #TODO Re-visit the parameter defaults
  
  non_nested_cols <- test_cols[!test_cols %in% nested_cols]
  unique_nested <-  x[nested_cols] %>% unique()
  
  a <- nrow(unique_nested)
  for(i in non_nested_cols){

    a <- a*length(unique(x[[i]]))
  }
  
  if(nrow(x)!=a){
    if(!warn_only){
      stop(msg = "validate_complete: missing values in input dataframe. Is the nested_cols parameter set correctly?")
    } else {
      warning(msg = "validate_complete: missing values in input dataframe. Is the nested_cols parameter set correctly?")
    }
  }
  
  invisible()
}


