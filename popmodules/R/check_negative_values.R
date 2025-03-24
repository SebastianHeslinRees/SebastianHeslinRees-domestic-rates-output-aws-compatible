#' Test a column in a dataframe for negatives and return a warning or amend the values
#'
#' If negatives exist print a warning. If the \code{warn_only} parameter is
#' FALSE then change the negatives to a user-defined value.
#'
#' @param x A data frame.
#' @param data_col String. The name of a column in the data frame containing
#'   numeric data.
#' @param warn_only  Logical. When TRUE, only warn that the data_col contains
#'   negatives. When change negative values to the change_value.
#' @param change_value Numeric. If \code{warn_only = FALSE} this the value that
#'   negatives area changed to. Default 0
#'
#' @return The input data frame. If \code{warn_only} is FALSE, negative values
#'   of \code{data_col} are set to the \code{change_value} value.
#'   
#' @importFrom assertthat assert_that
#'
#' @export

check_negative_values <- function(x, data_col, warn_only = FALSE, change_value = 0){

  #Validate
  assert_that(data_col %in% names(x),
                          msg = paste("check_negative_values was passed a data_col column that isn't in the dataframe:", data_col))
  assert_that(any(c("numeric", "integer") %in% class(x[[data_col]])),
                          msg = paste("check_negative_values was passed a non-numeric column:", data_col))
  assert_that(is.data.frame(x),
              msg = "check_negative_values expected a dataframe")
  assert_that(is.logical(warn_only),
              msg = paste("check_negative_values expected a non-logical value:", warn_only))
  assert_that(!any(is.na(x[data_col])), msg = paste("check_negative_values: NAs in", data_col, " column"))

  #Function body
  ix <- x[[data_col]] < 0
  
  if(any(ix)) {

    zero_msg <- ifelse(warn_only,
                       "Warning only, returning dataframe unchanged",
                       paste("Setting negative values to", change_value))

    n <- sum(ix)
    total_negative <- round(sum(x[[data_col]][ix]),2)

    warning(paste0(capture.output({
      print(paste0("Negative values of ", data_col, " found at ",
                   n, " aggregation levels summing to ",
                   total_negative))
      print(zero_msg)
      # if(sum(ix) < 30) {
      #   print("Values:")
      #   print(x[ix,])
      # } else {
      #   print("First 30 values:")
      #   print(x[ix,][1:30,])
      # }
    }), collapse = "\n"))

    if(!warn_only){
      x[[data_col]][ix] <- change_value
    }
  }

  return(x)
}
