#' Apply a constraint to a dataframe.
#' 
#' Constrain values in one dataframe to those in another at a higher geography.#' 
#' 
#' @param x dataframe. Data to be constrained
#' @param constraint dataframe. Constraint data
#' @param constraint_lookup dataframe. Lookup between operational geography and
#'   constraint geography
#' @param mapping Character. Column aggregation to use in constraining
#' @param data_col String. Column in x containing data. Default NULL will use
#'   last column in x.
#' @param col_geog String. Column in x containing operational geography
#' 
#' @import dplyr
#' 
#' @export
#' 
#' @return A dataframe containing constrained values for the selected geography

apply_constraint <- function(x,
                             constraint,
                             constraint_lookup,
                             mapping,
                             data_col = NULL,
                             col_geog = "area_code"){
  
  if(is.null(data_col)){data_col <- last(names(x))}
  
  to_constrain <- filter(x, !!sym(col_geog) %in% constraint_lookup[[col_geog]])
  no_constraint <- filter(x, !(!!sym(col_geog) %in% constraint_lookup[[col_geog]]))
  
  if(!all(names(constraint_lookup) %in% names(to_constrain))){
    
    to_constrain <- to_constrain %>% 
      left_join(constraint_lookup, by = col_geog)
  }
  
  x_out <- constrain_component(to_constrain,
                               constraint,
                               col_aggregation = mapping,
                               data_col) %>% 
    select(names(x)) %>% 
    bind_rows(no_constraint)
  
  return(x_out)
  
}