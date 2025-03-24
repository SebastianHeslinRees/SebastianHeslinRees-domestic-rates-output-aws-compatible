#' Convert an input list into a dataframe for use in the trend model
#'
#' Information on rate or flows to be used in the trend model are passed to the
#' model as a named list. The list names are years and each element contains a
#' \code{path} variable and a \code{transition} variable.
#' 
#' \code{path} is a String. The file path to a dataframe containing rates or flows
#' \code{transition} is Logical. Is the year interpolating from one set of rates
#'   to another
#'
#' The function extracts information from the list of lists and arranges it in a
#' dataframe which is used by the \code{get_rates_or_flows} function to read
#' and calculate model data
#'
#' @param data_list A named list of lists. Each list name is a year. Each list
#'   contains a list with a \code{path} and \code{transition} variable
#' @param first_proj_yr Numeric. The first projection year
#' @param last_proj_yr Numeric. The last projection year
#'
#' @return A dataframe containing paths and information for calculating rates
#'
#' @import dplyr
#' @import tidyr
#' @export

get_rates_flows_info <- function(data_list, first_proj_yr, last_proj_yr){
  
  list_name <- deparse(substitute(data_list))
  validate_get_rates_or_flows_info(data_list, first_proj_yr, list_name)
  
  last_yr <- data_list %>% names() %>% as.numeric() %>% max()
  last_yr <- max(c(last_yr, last_proj_yr))
  
  df_info <- data.table::rbindlist(data_list, idcol="year") %>%
    as.data.frame() %>% 
    mutate(year = as.numeric(year)) %>%
    right_join(data.frame(year = first_proj_yr:last_yr), by = "year") %>% 
    arrange(year) %>% 
    tidyr::fill(transition) %>%
    
    mutate(next_path = path) %>%
    tidyr::fill(next_path, .direction="up") %>%
    mutate(next_path = lead(next_path),
           next_path = ifelse(is.na(path),NA,next_path)) %>% 
    
    mutate(period_start = ifelse(!is.na(path),year, NA)) %>%
    tidyr::fill(period_start) %>%
    
    mutate(period_end = lead(period_start),
           period_end = ifelse(period_start==period_end,NA,period_end),
           period_end = ifelse(year == last_yr, last_yr+1, period_end)) %>%
    tidyr::fill(period_end, .direction = "up") %>%
    
    mutate(transition_period = period_end-period_start,
           period_step = year - period_start) %>% 
    select(-period_start, -period_end)
  
  return(df_info)
}

validate_get_rates_or_flows_info <- function(data_list, first_proj_yr, list_name){
  
  x <- as.numeric(as.numeric(names(data_list)))
  assertthat::assert_that(!any(is.na(x)),
                          msg =  paste0(list_name, ": names in data_list must be years and convertable to numeric"))
  
  x <- min(x)
  assertthat::assert_that(x==first_proj_yr,
                          msg=( paste0(list_name, ": the first year in data_list is ", x,
                                       ", it must be the same as first_proj_yr (",first_proj_yr,")")))
  
  
  for(i in 1:length(data_list)){
    assertthat::assert_that(is.list(data_list[[i]]),
                            msg=paste0(list_name, "[",i,"] must be a list"))
    
    assertthat::assert_that(setequal(names(data_list[[i]]), c("path","transition")),
                            msg=paste0(list_name, ": data_list[",i,"] must have elements 'path' and 'transition'"))
    
    
    assertthat::assert_that(file.exists(data_list[[i]]$path),
                            msg = paste0(list_name, "[", i, "] file does not exist"))
  }
  assertthat::assert_that(!data_list[[length(data_list)]]$transition,
                          msg = paste0(list_name, ": The last entry in data_list can't have transition = TRUE"))
}
