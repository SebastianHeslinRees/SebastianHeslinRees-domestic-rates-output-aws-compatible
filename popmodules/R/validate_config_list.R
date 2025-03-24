#' Validate a model's config_list against an expected list of config settings
#'
#' The GLA population models all take a \code{config_list} as input, a list
#' containing every customisable model setting. This function compares the
#' config list against a provided vector giving the expected elements in the
#' config list. It warns if the list contains additional settings, and throws an
#' error if the list is missing something the model is expecting. Otherwise it
#' returns TRUE.
#'
#' @param config_list List containing model config settings
#' @param expected_config Character vector containing the names of expected
#'   elements in \code{config_list}
#'
#' @return TRUE, invisibly
#'
#' @export

validate_config_list <- function(config_list, expected_config) {
  
  extra_config_elements <- setdiff(names(config_list), expected_config)
  if(length(extra_config_elements) > 0) {
    warning(paste(c("The model was given unexpected extra config settings:", extra_config_elements), collapse = " "))
  }
  
  missing_config_elements <- setdiff(expected_config, names(config_list))
  if(length(missing_config_elements) > 0) {
    stop(paste(c("The model needs additional config settings:", missing_config_elements), collapse = " "))
  } 
  
  invisible(TRUE)
}