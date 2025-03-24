#' Project mortality/fertility rates forward based on a national trend
#'
#' Applies the national rate of change in mortality/fertility to a set of local
#' authority ASMRs/ASFRs to produce a mortality/fertility probability trajectory.
#' This takes the projection for the first projection year as an input, and
#' extends it to the required number of projection years
#'
#' @param jump_off_rates Dataframe. A set of LA/age/sex rates for the first
#'   projection year
#' Likely calculated from \code{scaled_mortality_curve()} or \code{scaled_fertility_curve()}
#' @param rate_col Character. The column in the jump_off_rates dataframe
#'   containing the rates.
#' @param rates_trajectory String or dataframe. The file path for the national-level
#'   ASMR/ASFR trajectory or a dataframe containing the data.
#' @param first_proj_yr Integer. The first projection year.
#' @param n_proj_yr Integer. Number of years to project
#' @param npp_var Character. NPP trend variant. Either \code{2018_principal},
#'   \code{2018_high}, \code{2018_low},  \code{2016_principal}, \code{2016_high}, or
#'   \code{2016_low}. Defaults to \code{2018_principal}.
#' @param col_geog String. The geography column in the dataframe
#'
#' @return A data frame of fertility probabilities for all projection years.
#' @import dplyr
#' @import dtplyr
#' @importFrom assertthat assert_that
#' @export

project_rates_npp <- function(jump_off_rates,
                              rate_col,
                              rates_trajectory,
                              first_proj_yr,
                              n_proj_yr,
                              npp_var="2018_principal",
                              col_geog = "gss_code"){
  
  rates_trajectory <- .path_or_dataframe(rates_trajectory)

  #Validate
  validate_proj_rates_npp_input(jump_off_rates, rate_col, rates_trajectory, first_proj_yr, n_proj_yr, npp_var)
  
  final_projection_year <- first_proj_yr + n_proj_yr -1
  
  if(first_proj_yr-1 > max(jump_off_rates$year)){
    jump_off_rates <- project_forward_flat(jump_off_rates, first_proj_yr-1)
  }
  jump_off_year <- max(jump_off_rates$year)
  assert_that(all((jump_off_year + 1):final_projection_year %in% rates_trajectory$year))
  backseries <- filter(jump_off_rates, year < jump_off_year) %>% data.frame() 
  jump_off_rates <- filter(jump_off_rates, year == jump_off_year) %>% data.frame() 
  
  # calculate the rate changes relative to the first year, and apply this to the
  # jump off rate to calculate the rest of the projection years
  rates <- rates_trajectory %>%
    lazy_dt() %>% 
    filter(variant == npp_var) %>%
    filter(year > jump_off_year) %>%
    filter(year <= final_projection_year) %>%
    arrange(year) %>%
    group_by(sex, age) %>%
    mutate(change = change + 1,
           cumprod = cumprod(change))%>%
    ungroup() %>%
    arrange(age, sex, year) %>%
    left_join(select(jump_off_rates, -year), by=c("sex","age")) %>%
    rename(jump_rate = rate_col) %>%
    mutate(rate = cumprod*jump_rate) %>%
    rename(!!rate_col := rate) %>%
    select(names(jump_off_rates)) %>% 
    data.frame() %>% 
    rbind(jump_off_rates, backseries) %>% 
    arrange(year, across(!!col_geog), sex, age)
  
  return(rates)
  
}

#--------------------------------------------------------------------

# Function to check that the input to project_fertility_rates is all legal

validate_proj_rates_npp_input <- function(jump_off_rates,
                                          rate_col,
                                          rates_trajectory,
                                          first_proj_yr,
                                          n_proj_yr,
                                          npp_var) {
  
  # test input parameters are of the correct type
  assert_that(is.data.frame(jump_off_rates),
              msg="in project_npp_rates: jump_off_rates expects a data frame as input")
  assert_that(is.character(rate_col),
              msg="in project_npp_rates: rate_col should be character")
  assert_that(is.numeric(first_proj_yr),
              msg="in project_npp_rates: first_proj_year expects an integer as input")
  assert_that(is.numeric(n_proj_yr),
              msg="in project_npp_rates: n_proj_yr expects an integer as input")
  assert_that(is.character(npp_var),
              msg="in project_npp_rates: npp_var expects character input")
  
  #check required columns are present
  assert_that("year" %in% names(rates_trajectory),
              msg = "in project_npp_rates the rates_trajectory dataframe must have a 'year' column")
  assert_that("sex" %in% names(rates_trajectory),
              msg = "in project_npp_rates the rates_trajectory dataframe must have a 'sex' column")
  assert_that("age" %in% names(rates_trajectory),
              msg = "in project_npp_rates the rates_trajectory dataframe must have an 'age' column")
  assert_that("sex" %in% names(jump_off_rates),
              msg = "in project_npp_rates the jump_off_rates dataframe must have a 'sex' column")
  assert_that("age" %in% names(jump_off_rates),
              msg = "in project_npp_rates the jump_off_rates dataframe must have an 'age' column")
  
  
}
