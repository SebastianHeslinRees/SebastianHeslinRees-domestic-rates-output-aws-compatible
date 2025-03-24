#' Age on population
#'
#' Given a cohort population, advance the cohort's age by one timestep/age band
#' and (if specified) increase the date year.
#'
#' The function works with cohort age data as either numeric data or as an
#' ordered factor (e.g. age bands). If age is numeric then all age bands must be
#' equally spaced by the interval given in \code{timestep}, usually 1.
#'
#' Ageing of the population is done by either adding the timestep to the age
#' (for a numeric age) or by incrementing to the next level of the factor (for
#' age as an ordered factor). If \code{col_year} is given, the year will also
#' be increased by \code{timestep}.
#'
#' The highest age/age band is considered to be unbounded. This means that when
#' a population is aged on, the population in this band remains in this band,
#' and the age/band below it is added to it. An ageing population will all
#' eventually be moved into this age band if no other processes are operating.
#'
#' @param popn Data frame containing a cohort population.
#' @param col_aggregation Character vector of columns in \code{popn} acting as
#'   aggregation levels. Default \code{c("year","gss_code","age","sex")}. In the case
#'   of multiple geography columns, only supply the highest resolution column;
#'   other columns will be dropped.
#' @param col_age String denoting the age column. Default "age".
#' @param col_year String denoting the year column. Can be NULL. Default "year".
#' @param col_data String or Character denoting the column(s) containing data.
#'   For multiple columns pass as a vector \code{c("popn","popn_2")}.
#'   Default "popn".
#' @param timestep Numeric denoting the model time step in years. If age data
#'   are numeric, they must be spaced by this interval. If they are an ordered
#'   factor, the function will advance the age by one age band regardless of
#'   this parameter. The \code{col_year} column, if present, will also be
#'   increased by \code{timestep} years. Default 1.
#' @param births Data frame, 0 or \code{NULL}. Births data at the same
#'   aggregation levels as \code{popn} but coded to the lowest age band that can
#'   be joined to the output with \code{rbind}. If zero, then the output rows for
#'   zero-year-olds, all of which are zero. If \code{NULL} then no
#'   zero-year-olds are included in the output. Default \code{NULL}.
#' @param col_births String denoting the name of the column containing data in 
#'   the \code{births} dataframe.
#' @param col_geog Character vector denoting the geography column(s). Default "gss_code".
#'   Only used in validation. Only used when births dataframe is specified.
#'
#' @return A data frame containing the population with age advanced by one time
#'   step (or age band). The lowest age will not be present in this output. Rows
#'   are arranged by \code{col_aggregation}, in that order.
#'
#' @import assertthat
#' @import dplyr
#' @importFrom dtplyr lazy_dt
#' @importFrom plyr mapvalues
#' @importFrom stringr str_detect
#'
#' @export

popn_age_on2 <- function(popn,
                        col_aggregation=c("year","gss_code","age","sex"),
                        col_age = "age",
                        col_year = "year",
                        col_data = "popn",
                        timestep = 1,
                        births = NULL,
                        col_births = "births",
                        col_geog = "gss_code") {
  
  # Validate inputs
  
  # validate_popn_age_on2_input(popn,
  #                            col_aggregation,
  #                            col_age,
  #                            col_year,
  #                            col_data,
  #                            col_geog,
  #                            timestep,
  #                            template_age_levels=NULL,
  #                            births,
  #                            col_births)
  
  # Standardise data
  if(!col_age %in% col_aggregation) {
    col_aggregation <- c(col_aggregation, col_age)
  }
  
  # reorder col_aggregation to match input column order
  col_aggregation <- intersect(names(popn), col_aggregation)
  
  # Increment the year
  aged <- popn %>% 
    mutate(!!col_year := !!sym(col_year) + timestep) %>% 
    mutate(!!col_age := !!sym(col_age) + timestep)
 
  # Increment age
  max_age <- max(popn[[col_age]])
  col_agg_no_age <- col_aggregation[(col_aggregation!= sym(col_age))]
  
  last_age <- aged %>% 
    #lazy_dt() %>%  #causes an error
    filter(across(!!col_age) >= max_age) %>%
    group_by(across(!!col_agg_no_age)) %>%
    summarise(across(!!col_data, sum), .groups = 'drop_last') %>% 
    mutate(!!col_age := max_age) %>% 
    select(c(!!col_aggregation, !!col_data)) %>% 
    data.frame()
  
  aged <- aged %>% 
    filter(across(!!col_age) < max_age) %>%
    select(names(last_age)) %>% 
    bind_rows(last_age) %>% 
    data.frame() %>%
    arrange(across(!!col_aggregation))
  
  #-----------------------------------------------------------------------------
  
  #Add births
  
  if(!is.null(births)) {
    
    if(!is.data.frame(births) && births == 0) {
      
      births <- aged %>% 
        select(!!col_aggregation) %>% 
        mutate(!!col_age := 0) %>% 
        mutate(!!col_births := 0) %>% 
        unique()
    }
    
    births <- births %>%
      filter(across(!!col_age) == 0) %>% 
      rename(!!col_data := col_births) %>%
      select(names(aged)) %>% 
      data.frame()
    
    aged <- bind_rows(births, aged) %>%
      arrange(across(!!col_aggregation))
    
    # if(!is.null(col_year)){
    #   common_years <- intersect(births[[col_year]], popn[[col_year]])
    #   aged <- aged %>% filter(across(!!col_year) %in% common_years)
    # }
  }
  
  #-----------------------------------------------------------------------------
  
  # Match input formatting
  popn_factors <- names(popn)[ sapply(popn, is.factor) ]
  popn_not_factors <- setdiff(names(aged), popn_factors)
  aged <- mutate_at(aged, .vars = popn_factors, .funs = as.factor)
  aged <- mutate_at(aged, .vars = popn_not_factors, .funs = as.vector)
  
  return(aged)
}

# -------------------------------------------------------------------------

validate_popn_age_on2_input <- function(popn,
                                       col_aggregation,
                                       col_age,
                                       col_year,
                                       col_data,
                                       col_geog,
                                       timestep,
                                       template_age_levels,
                                       births,
                                       col_births) {
  
  # Type checking
  assert_that(is.data.frame(popn),
              msg = "popn_age_on: popn parameter must be a data frame")
  assert_that(is.character(col_aggregation),
              msg = "popn_age_on: col_aggregation parameter must be a string or character vector")
  assert_that(is.string(col_age),
              msg = "popn_age_on: col_age parameter must be a string")
  assert_that(is.string(col_data) | is.character(col_data),
              msg = "popn_age_on: col_data parameter must be a string or character vector")
  assert_that(is.character(col_geog),
              msg = "popn_age_on: col_geog parameter must be a string")
  assert_that(is.null(col_year) || is.string(col_year),
              msg = "popn_age_on: col_year parameter must be NULL or a string")
  assert_that(is.number(timestep),
              msg = "popn_age_on: timestep parameter must be a number")
  assert_that(is.null(template_age_levels) || is.vector(template_age_levels) || is.factor(template_age_levels)  ,
              msg = "popn_age_on needs a vector, a factor, or NULL as the template_age_levels parameter")
  assert_that(is.null(births) || (is.numeric(births) && births == 0) || is.data.frame(births),
              msg = "popn_age_on: births parameter must be NULL, 0 or a data frame")
  assert_that(is.string(col_births),
              msg = "popn_age_on: col_births parameter must be a string")
  
  # Check for naming conflicts
  assert_that(all(names(col_aggregation) %in% names(popn)),
              msg = "in popn_age_on, all columns named in col_aggregation must be columns in the popn data frame")
  assert_that(col_age %in% names(popn),
              msg = "popn_age_on was given a col_age parameter that isn't a column in the input popn data frame")
  assert_that(is.null(col_year) || col_year %in% names(popn),
              msg = "popn_age_on was given a col_year parameter that isn't a column in the input popn data frame")
  assert_that(all(col_data %in% names(popn)),
              msg = "popn_age_on was given a col_data parameter that isn't a column in the input popn data frame")
  assert_that(all(col_geog %in% names(popn)),
              msg = "popn_age_on was given a col_geog parameter that isn't a column in the input popn data frame")
  assert_that(!any(duplicated(col_aggregation)),
              msg = "duplicated aggregation column names were provided to popn_age_on")
  
  
  if(is.data.frame(births)){
    assert_that(all(names(col_aggregation) %in% names(popn)),
                msg = "in popn_age_on, all columns named in col_aggregation must be columns in the births data frame")
    assert_that(col_births %in% names(births),
                msg = "popn_age_on was given a col_births parameter that isn't a column in the input births data frame")
  }
  
  #Check geographies are consistent
  if(is.data.frame(births)){
    validate_same_geog(popn, births, col_1 = col_geog, col_2 = col_geog)
  }
  
  # Validate template age levels (if supplied)
  if(!is.null(template_age_levels)) {
    template_age_levels <- unique(template_age_levels)
    
    if(is.factor(template_age_levels)) {
      assert_that(setequal(template_age_levels, levels(template_age_levels)),
                  msg = "popn_age_on's template_age_levels is a factor with the levels mismatching the contents")
    }
    
    missing_popn_age_levels <- setdiff(template_age_levels, popn[[col_age]])
    assert_that(length(missing_popn_age_levels) == 0,
                msg = paste(c("popn_age_on's input popn is missing levels present in the provided template_age_levels:",
                              missing_popn_age_levels), collapse = " "))
    
    missing_template_age_levels <- setdiff(popn[[col_age]], template_age_levels)
    assert_that(length(missing_template_age_levels) == 0,
                msg = paste(c("popn_age_on's input popn contains levels not present in the provided template_age_levels:",
                              missing_template_age_levels), collapse = " "))
  }
  
  # Other checks
  assert_that(nrow(popn) > 0,
              msg = "popn_age_on was given a 'popn' data frame with 0 rows of input")
  assert_that(length(unique(popn[[col_age]])) > 1,
              msg = "popn_age_on was given a data frame containing only one age value")
  assert_that(is.null(col_year) || is.numeric(popn[[col_year]]),
              msg = "popn_age_on needs numeric values in the col_year year column")
  if(is.data.frame(births)){
    assert_that(nrow(popn) > 0,
                msg = "popn_age_on was given a 'births' data frame with 0 rows of input")
  }
  
  if(is.factor(popn[[col_age]])) {
    assert_that(setequal(popn[[col_age]], levels(popn[[col_age]])),
                msg = "popn_age_on's template_age_levels is a factor with the levels mismatching the contents")
  }
  
  assert_that(is.numeric(popn[[col_age]]) || is.ordered(popn[[col_age]]),
              msg = "popn_age_on: data in col_age must be numeric or an ordered factor")
  assert_that(timestep > 0,
              msg = "popn_age_on: timestep must be > 0")
  if(floor(timestep) != timestep) {
    warning("popn_age_on was given a non-integer timestep - it's not been tested for this situation so check the outputs")
  }
  if(!is.ordered(popn[[col_age]])) {
    assert_that(all( diff(sort(unique(popn[[col_age]]))) == timestep), # we know the data are complete so this simplification is valid
                msg = "popn_age_on needs all ages to be equally spaced (by timestep) in its input data (alternately, convert to an ordered factor)")
  }
  
  #Test for duplicate rows
  assert_that(!any(duplicated(popn[col_aggregation])),
              msg = "popn_age_on: duplicated aggregation levels in input dataframe. Is the col_aggregation parameter set correctly?")
  
  #Check for missing data
  validate_complete(popn, col_aggregation, col_geog)
  
  #Test for NAs
  assert_that(sum(is.na(popn[col_aggregation]))==0,
              msg = "popn_age_on: NAs in one or more of the col_aggregation columns")
  
  #Births
  if(is.data.frame(births)){
    
    assert_that(length(col_data) == 1,
                msg = "in popn_age_on births can only be added if there is 1 data column")
    
  }
  
  invisible(TRUE)
  
}
