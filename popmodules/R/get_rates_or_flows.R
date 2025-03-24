#' Return a dataframe containing rates or flows for use in the trend model
#'
#' A function to read in data and where necessary create transitional data between
#' one known data point and the next. The returned dataframe contains a data column
#' \code{data_col} with the rate or flow to use in the model and also columns \code{value_1}
#' and \code{value_2} to be used in the next call to this function.
#'
#' @param df A data frame or NULL. The dataframe is the output of this function in
#'   the previous year iteration
#' @param df_info A data frame. The output from the function \code{get_rates_flows_info}
#' @param projection_year Numeric
#' @param first_proj_yr Numeric. The first projection year
#' @param col_aggregation Character. The non-data columns in the dataframe \code{df}
#' @param data_col String. The column containing the flow or rate information to be
#'   in the model
#' @param geog_code_col String. Which column in df contains the geography code
#' @param standardise_geog Logical. Should the geography column be renamed to the 
#'  generic 'area_code' or left alone. flex model needs TRUE, trend model needs FALSE.
#'  Default TRUE
#'
#' @return A dataframe containing rates or flows to use in the model and also
#'   rates or flows to be used in future calculations
#'
#' @import dplyr
#' @import assertthat
#' 
#' @export

get_rates_or_flows <- function(df, df_info, projection_year, first_proj_yr,
                               col_aggregation, data_col, geog_code_col,
                               standardise_geog = TRUE){
  
  #value_1 = data_col for df_info$path
  #value_2 = data_col for df_info$next_path
  #data_col = actual rate used in projection_year-1
  
  curr_yr_info <- filter(df_info, year == projection_year)
  
  #---
  
  .validate_rates_flows_input(df, df_info, projection_year, first_proj_yr,
                              col_aggregation, data_col, geog_code_col,
                              standardise_geog, curr_yr_info)
  #---
  
  # return input if rates will be the same as last year
  if(is.na(curr_yr_info$path) && !curr_yr_info$transition) {
    return(df)
  }
  
  #---
  
  #If rates aren't the same as last year its because:
  # a) Its the first year of the projection
  # b) There are completely new rates
  # c) Its a transition period and rates need to be calculated
  
  #--- FIRST YEAR / NEW DATA
  
  if(!is.na(curr_yr_info$path)){
    
    df <- readRDS(curr_yr_info$path)
    
    if(standardise_geog){
      df <- .standardise_df(df, col_geog = geog_code_col, data_col=data_col)
    }
    
    df <- select(df, !!col_aggregation, !!data_col) %>% 
      .validate_rates_or_flows(col_aggregation, data_col) 
  }
  
  #--- TRANSITION
  
  if(curr_yr_info$transition){
    
    if(curr_yr_info$period_step == 0){
      
      df <- rename(df, value_1 = !!data_col)
      
      #add next set of data
      df_1 <- readRDS(curr_yr_info$next_path)
      
      if(standardise_geog){
        df_1 <- .standardise_df(df_1, col_geog = geog_code_col, data_col=data_col)
      }
      
      df <- df_1 %>% 
        select(!!col_aggregation, !!data_col) %>% 
        .validate_rates_or_flows(col_aggregation, data_col) %>%
        rename(value_2 = !!data_col) %>%
        full_join(df, by = col_aggregation) %>% 
        tidyr::replace_na(list(value_1 = 0, value_2 = 0))
    }
    
    #---
    
    #remove column named data_col if it exists
    if(data_col %in% names(df)){ df <- select(df, -!!data_col) }
    
    df <- df %>% 
      mutate(increment = (value_2-value_1)/curr_yr_info$transition_period,
             value = value_1+(increment*curr_yr_info$period_step)) %>%
      select(-increment) %>%
      check_negative_values("value") %>%
      rename(!!data_col := value)
  }
  
  return(df)
  
}


.validate_rates_or_flows <- function(df, col_aggregation, data_col) {
  
  validate_population(df, col_aggregation, data_col,
                      test_complete = FALSE, test_unique = TRUE, check_negative_values = TRUE)
  
  if(data_col == "rate") {
    assert_that(max(df$rate) <= 1 & min(df$rate) >= 0, msg = "rate data contains rates outside the range 0-1")
  }
  invisible(df)
}

.validate_rates_flows_input <- function(df, df_info, projection_year, first_proj_yr,
                                        col_aggregation, data_col, geog_code_col,
                                        standardise_geog, curr_yr_info){
  
  
  
  
  assert_that(nrow(curr_yr_info)==1,
              msg=paste0("In get_rates_or_flows, Year ", projection_year,
                         ", too many rows in df_info dataframe"))
  
  assert_that(!is.null(df) | !is.na(curr_yr_info$path),
              msg = paste0("In get_rates_or_flows, Year ", projection_year,
                           ", Must provide input df or a path to read from in the current year of df_info"))
  
  invisible()
}