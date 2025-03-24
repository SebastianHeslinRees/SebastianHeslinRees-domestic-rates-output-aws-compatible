#' Read data from files into a named list
#'
#' @param files Named list or named vector where the value is the file path and the
#'  name is the name of the list element to write the data to
#'
#' @return List of dataframes
#' 
#' @importFrom assertthat assert_that
#'
#' @export
#'
#' @examples
#' \dontrun{
#' a <- list(
#'   cross_border_in_constraint =
#'     "input_data/constraints/npp_2016_cross_border_in_constraint.rds",
#'   cross_border_out_constraint =
#'     "input_data/constraints/npp_2016_cross_border_out_constraint.rds")
#'
#' g <- get_data_from_file_2(files = a)
#' }

get_data_from_file <- function(files) {
  
  x <- list()
  
  for(i in seq(files)){
    
    assert_that(file.exists(files[[i]]),
                msg = paste0(names(files)[[i]], ": ", files[[i]],
                             "\nFile does not exist at specified path"))
    
    
    x[[names(files)[[i]]]] <- readRDS(files[[i]])
  }
  
  return(x)
  
}
