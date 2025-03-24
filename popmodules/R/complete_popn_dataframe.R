#' Complete the sex and age of a population or rates dataframe
#'
#'
#' Example use would be a fertility dataframe which only has data for
#' females aged 15-49. The function completes the dataframe for the entire
#' population with a rate of 0 for imputed aggregation levels..
#'
#'
#' @param x A dataframe containing population data. Any age column must be numeric.
#' @param sexes Character. The sexes to be imputed. Default c('female','male')
#' @param ages Numeric. The ages to be imputed. Default 0:90
#' @param col_sex String. The with the sex column name.
#'   Takes NULL if there is no sex column.
#' @param col_age String. The the age column name.
#'   Takes NULL if there is no age column.
#' @param col_data String. The with the data column name.
#' @param fill_value Numeric. The value to assign to the imputed levels.
#'   Default 0.
#'
#' @return A dataframe that contains entries for all sexes in \code{sexes} and
#' all ages in \code{ages}. New entries are created with a rate value of \code{fill_value}.
#'
#' @import dplyr
#' @import rlang
#' @importFrom tidyr complete nesting
#' 
#' @export


complete_popn_dataframe <- function(x,
                                 sexes = c('female','male'),
                                 ages = 0:90,
                                 col_sex = "sex",
                                 col_age = "age",
                                 col_data = "rate",
                                 fill_value = 0){
  
  fill_list <- list(value = fill_value)
  names(fill_list) <- col_data
  
  other_cols <- setdiff(names(x), c(col_sex, col_age, col_data))
  
  completed <- x %>% 
    complete(nesting(!!!syms(other_cols)),
             !!sym(col_sex) := sexes,
             !!sym(col_age) := ages,
             fill = fill_list) %>% 
    data.frame()
  
  return(completed)
  
}



