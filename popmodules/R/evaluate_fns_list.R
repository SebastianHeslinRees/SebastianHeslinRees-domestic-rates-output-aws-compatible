#' Evaluate a chain of functions passed as a list
#'
#' Each element of the argument \code{fns_args_list} is a list.
#' These function lists must contain a function \code{fn} and
#' a list \code{args} of arguments. The output of one function will
#' be passed to the next as it's first argument (like a dplyr pipe).
#'
#' @param fns_args_list List of functions and their arguments
#'
#' @return The output from the final function in the funs_args_list
#'
#' @examples
#' \dontrun{
#'  funs_args_list <- list(
#'   list(fn = function1, args = list(arg1_1, arg1_2, ....)),
#'   list(fn = function2, args = list(arg2_1, arg2_2, ....)),
#'   .....
#' )
#' }
#'
#' @export

evaluate_fns_list <- function(fns_args_list) {

  ## the first creates an initial dataframe
  subject <- do.call(fns_args_list[[1]]$fn, fns_args_list[[1]]$args)
  n_fns_remaining <- length(fns_args_list) - 1

  ## the rest take in component as their first argument
  if (n_fns_remaining > 0) {
    for (i in 1:n_fns_remaining) {
      ind <- i + 1

      # add component on as the first argument to the function
      n_args <- length(fns_args_list[[ind]]$args)
      all_args <- fns_args_list[[ind]]$args
      all_args[[n_args + 1]] <- subject
      all_args <- all_args[c(n_args + 1, 1:n_args)]

      subject <- do.call(fns_args_list[[ind]]$fn, all_args)
    }
  }
  return(subject)
}
