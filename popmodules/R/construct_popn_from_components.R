#' Add components to a starting population
#'
#' Shortcut function to combine population components
#'
#' @param start_population Data frame with initial population. All subsequent
#'   inputs should be at the same resolution.
#' @param addition_data a List of component dataframes contain data to be added
#'   to the start population
#' @param subtraction_data a List of component dataframes contain data to be
#'   subtracted from the start population
#' @param col_aggregation Aggregation columns common to all the above to join
#'   on.
#' @param data_are_subsets Logical. Set to TRUE when addition or subtraction
#'   component data are provided for subsets of the \code{start_population},
#'   e.g. for London only. Default \code{false}.
#'
#' @return A data frame with all components added.
#'
#' @import dplyr
#' @import assertthat
#' @importFrom dtplyr lazy_dt
#' @importFrom data.table rbindlist
#'
#' @export
#' 
construct_popn_from_components <- function(start_population,
                                           addition_data,
                                           subtraction_data,
                                           col_aggregation = c("year","gss_code","sex","age"),
                                           data_are_subsets = FALSE){
  
  #remove any areas in addition and subtraction that aren't in start_population
  addition_data <- lapply(addition_data, function(x){filter(x, gss_code %in% unique(start_population$gss_code))})
  subtraction_data <- lapply(subtraction_data, function(x){filter(x, gss_code %in% unique(start_population$gss_code))})
  
  validate_construct_popn_from_component_input(start_population, addition_data, subtraction_data, col_aggregation, data_are_subsets)
  
  start_population <- mutate(start_population, var = "start") %>%
    rename(popn = last(names(start_population))) %>%
    select_at(c(col_aggregation, "var", "popn"))
  
  
  
  for(i in seq(addition_data)){
    addition_data[[i]] <- tidyr::pivot_longer(addition_data[[i]],
                                              ncol(addition_data[[i]]),
                                              names_to = "var",
                                              values_to = "popn") %>%
      select(col_aggregation, "var", "popn")
  }
  
  for(i in seq(subtraction_data)){
    subtraction_data[[i]] <- tidyr::pivot_longer(subtraction_data[[i]],
                                                 ncol(subtraction_data[[i]]),
                                                 names_to = "var",
                                                 values_to = "popn") %>%
      mutate(popn = popn*-1) %>%
      select(col_aggregation, "var", "popn")
  }
  
  constructed <- rbind(start_population,
                       rbindlist(addition_data),
                       rbindlist(subtraction_data)) %>%
    lazy_dt() %>%
    group_by(across(!!col_aggregation)) %>%
    summarise(popn = sum(popn)) %>%
    as.data.frame() %>%
    select(col_aggregation, "popn")
  
  return(constructed)
  
}

validate_construct_popn_from_component_input <- function(start_population,
                                                         addition_data,
                                                         subtraction_data,
                                                         col_aggregation,
                                                         data_are_subsets){
  #validation
  #are addition and subtraction lists
  assertthat::assert_that(is.list(addition_data),
                          msg="In construct_popn_from_components addition_data must be a list of dataframes")
  assertthat::assert_that(is.list(subtraction_data),
                          msg="In construct_popn_from_components subtraction_data must be a list of dataframes")
  
  #are the elements of those lists dataframes
  assertthat::assert_that(is.data.frame(start_population),
                          msg = "construct_popn_from_components: start_population must be a dataframe")
  
  assertthat::assert_that(
    all(sapply(addition_data, is.data.frame)),
    msg = "construct_popn_from_components: All elements the addition_data list must be dataframes"
  )
  
  assertthat::assert_that(
    all(sapply(subtraction_data, FUN = function(x) is.data.frame(x))),
    msg = "construct_popn_from_components: All elements the subtraction_data list must be dataframes"
  )
  
  #is the final column of each df numeric
  assertthat::assert_that(
    all(sapply(addition_data, FUN = function(x) is.numeric(x[[ncol(x)]]))),
    msg = "construct_popn_from_components: The final column in every addition_data dataframe must be numeric"
  )
  
  assertthat::assert_that(
    all(sapply(subtraction_data, FUN = function(x) is.numeric(x[[ncol(x)]]))),
    msg = "construct_popn_from_components: The final column in every subtraction_data dataframe must be numeric"
  )
  
  #the final column is not a col_aggregation column
  assertthat::assert_that(
    all(sapply(addition_data, FUN = function(x) !names(x)[[ncol(x)]] %in% col_aggregation)),
    msg = "construct_popn_from_components: the final column in 1 of the addition dataframes is one of the col_aggregation columns, das ist verbotten"
  )
  
  assertthat::assert_that(
    all(sapply(subtraction_data, FUN = function(x) !names(x)[[ncol(x)]] %in% col_aggregation)),
    msg = "construct_popn_from_components: the final column in 1 of the subtraction dataframes is one of the col_aggregation columns, das ist verbotten"
  )
  
  #every data frame contains col_aggregation fields
  assertthat::assert_that(
    all(sapply(addition_data, FUN = function(x) all(col_aggregation %in% names(x)))),
    msg = "construct_popn_from_components: one or more col_aggregation columns not found in one of the addition_data dataframes"
  )
  
  assertthat::assert_that(
    all(sapply(subtraction_data, FUN = function(x) all(col_aggregation %in% names(x)))),
    msg = "construct_popn_from_components: one or more col_aggregation columns not found in one of the subtraction_data dataframes"
  )
  
  #dataframes don't contain additional gss codes
  if(!data_are_subsets) {
    assertthat::assert_that(
      all(sapply(addition_data, FUN = function(x) setequal(x$gss_code, start_population$gss_code))),
      msg = "construct_popn_from_components: dataframes in addition_data contain gss_codes that aren't in start_population"
    )
    
    assertthat::assert_that(
      all(sapply(subtraction_data, FUN = function(x) setequal(x$gss_code, start_population$gss_code))),
      msg = "construct_popn_from_components: dataframes in subtraction_data contain gss_codes that aren't in start_population"
    )
  }
}
