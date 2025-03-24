#' Reorder a dataframe by geographies for output
#'
#' Sorts a data frame to put gss codes in a specified order. Codes are sorted
#' in the first instance by the type of geographic entity (eg. nation, region, district)
#' and then alphabetically within those groups.
#'
#' @param df A data frame.
#' @param gss_col A string containing the name of the column with gss codes.
#'   Default "gss_code".
#' @param order A character string of the gss code geographic identifiers
#'   (i.e. the first 3 characters of the gss code) to determine the order of
#'   sorting. Default c("E92","N92","S92","W92","E12","S12","E06","E07","E08","E09","W06").
#' 
#' @return The input data frame, reordered by the rankings in the order parameter
#'   
#' @import dplyr
#' @export


reorder_for_output <- function(df,
                               gss_col = "gss_code",
                               order = NULL) {
  
  if(!gss_col %in% names(df)){
    message(paste("In reorder_for_output, the column", gss_col, "is not in the dataframe. Returning dataframe unsorted."))
    return(df)
  }
  
  if(is.null(order)){
    order <- c("E92","N92","S92","W92","E12","S12","E06","E07","E08","E09","W06")
  }
  
  df %>% 
    mutate(gss_factor = !!sym(gss_col)) %>% 
    mutate(gss_factor = substr(gss_factor,1,3)) %>% 
    arrange(factor(gss_factor, levels = order), !!gss_col) %>% 
    select(-gss_factor)
  
}
