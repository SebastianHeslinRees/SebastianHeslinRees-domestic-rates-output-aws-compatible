#' Aggregate a single-year-of-age population into age groups
#'
#' Given an input population by sya the function will assign each age to a
#' defined age group using \code{base::cut} and then group and summarise the
#' data using \code{dplyr} functions, summing each column specified in
#' \code{data_cols} by age group.
#'
#' Note: all columns not specified in \code{data_cols} are assumed to be
#' aggregation levels.
#'
#' @param population A data frame containing population data by single year of
#'   age
#' @param age_groups A numeric vector of breaks in the age data. The breaks are
#'   defined as the highest value in each bin. See \code{cut} for more details.
#'   Note: if \code{include.lowest} is set to \code{TRUE} (as it is by default)
#'   the lowest value must also be present
#' @param labels String. The labels to be used in the \code{age_group} column in
#'   the output. Must be the same length as age_groups.
#' @param data_cols String. A vector with the names of any columns containing
#'   data to be aggregated
#' @param include.lowest Logical. Passed to \code{cut}. Does the variable
#'   \code{age_groups} include the lowest age. If set to FALSE the function will
#'   assume the first bin contains any value less than and including the stated
#'   value. Default \code{TRUE}
#' @param factors Logical. Should the \code{age_group} column be coerced to factor?
#'  If TRUE levels will be taken from \code{labels}. Default FALSE 
#'
#' @return A data frame of population data grouped by age group
#'
#' @importFrom stats complete.cases
#' @importFrom dtplyr lazy_dt
#'
#' @export

population_into_age_groups <- function(population, age_groups, labels=NULL, data_cols, include.lowest=T,
                                       factors = FALSE){

  if(is.null(labels)){
    labels <- .make_labels(age_groups)
  }
  
  pop <- population %>%
    mutate(age_group = cut(age,
                           breaks = age_groups,
                           include.lowest = include.lowest,
                           labels = labels)) %>%
    mutate(age_group = as.character(age_group)) %>%
    select(-age)

  cols <- names(pop)[!names(pop) %in% data_cols]

  pop <- lazy_dt(pop) %>%
    group_by(across(!!cols)) %>%
    summarise(across(everything(), sum)) %>%
    ungroup() %>%
    as.data.frame()

  if(factors){
    pop <- mutate(pop, age_group = factor(age_group, levels = labels))
  }
  
  return(pop)

}

.make_labels <- function(vec){
  
  x <- c()
  for(i in 2:length(vec)){
    
    if (i == 2){ 
      a <- vec[i-1]
    } else {
      a <- vec[i-1]+1 
    }
    
    if(i == length(vec)){
      b <- paste(vec[i-1]+1, "and over")
    } else {
      b <- paste(a, "to", vec[i])  
    }
    
    x <- c(x,b)
  }
  
  return(x)
}