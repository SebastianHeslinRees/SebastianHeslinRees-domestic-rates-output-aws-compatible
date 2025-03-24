#' Aggregate LA-level flow data to national, regional and sub-regional flows 
#'
#' Sum data for individual LAs for all 4 UK countries, 9 English regions and 2 of
#' the sub-regions (Inner & Outer London)
#'
#' @param domestic_flow A dataframe containing origin destination data with the
#'  columns \code{year, gss_in, gss_out, age, sex}
#' @param region_lookup A dataframe frame lookup between LA gss codes and region
#'  gss_codes. Columns \code{gss_code, gss_code_region}
#' @param flow_col A string giving the data column name in the input data frame.
#'  Default \code{flow}
#' @param inner_outer_lookup A file path to the location of a lookup between London
#'  Boroughs and the Inner London/Outer London subregions
#'
#' @return A list containing 3 data frames of origin destination flow data.
#'  List names are: \code{[[1]]regional_flow, [[2]]national_flow, [[3]]sub_regional_flow}
#' 
#' @import dplyr
#' @importFrom dtplyr lazy_dt
#' 
#' @export

aggregate_regional_flows <- function(domestic_flow, region_lookup, flow_col = "flow",
                                     inner_outer_lookup =  "input_data/lookup/inner_and_outer_london.rds"){
  
  domestic_flow <- domestic_flow %>% rename(flow = !!flow_col)
  
  #flows between 9 English regions & other 3 home nations
  regional_flow <- dtplyr::lazy_dt(domestic_flow) %>%
    left_join(region_lookup, by=c("gss_in"="gss_code")) %>%
    select(-gss_in) %>%
    rename(gss_in = region_gss_code) %>%
    left_join(region_lookup, by=c("gss_out"="gss_code")) %>%
    select(-gss_out) %>% 
    rename(gss_out = region_gss_code) %>%
    filter(gss_in != gss_out) %>%
    group_by(year, gss_in, gss_out, age, sex) %>%
    summarise(!!flow_col := sum(flow)) %>%
    as.data.frame()
  
  #flows between all 4 home nations
  national_lookup <- data.frame(country = c("E","N","S","W"),
                                gss_code = c("E92000001",
                                             "N92000002",
                                             "S92000003",
                                             "W92000004"),
                                stringsAsFactors = FALSE)
  
  national_flow <- dtplyr::lazy_dt(domestic_flow) %>%
    mutate(country_out = substring(gss_out, 1, 1),
           country_in  = substring(gss_in,  1, 1)) %>%
    select(-gss_out, -gss_in) %>% 
    left_join(national_lookup, by=c("country_out"="country")) %>% 
    rename(gss_out = gss_code) %>% 
    left_join(national_lookup, by=c("country_in"="country")) %>% 
    rename(gss_in = gss_code) %>% 
    select(-country_out, -country_in) %>% 
    filter(gss_in != gss_out) %>%
    group_by(year, gss_in, gss_out, age, sex) %>%
    summarise(!!flow_col := sum(flow)) %>%
    as.data.frame()
  
  inner_outer_lookup <- readRDS(inner_outer_lookup)
  
  outer_codes <- filter(inner_outer_lookup, outer == TRUE)$gss_code
  inner_codes <- filter(inner_outer_lookup, inner == TRUE)$gss_code
  
  sub_regional_flow <- dtplyr::lazy_dt(domestic_flow) %>%
    mutate(gss_out = case_when(gss_out %in% outer_codes ~ "E13000002",
                               gss_out %in% inner_codes ~ "E13000001",
                               TRUE ~ gss_out),
           gss_in = case_when(gss_in %in% outer_codes ~ "E13000002",
                              gss_in %in% inner_codes ~ "E13000001",
                              TRUE ~ gss_in)) %>% 
    filter(gss_in != gss_out) %>%
    group_by(year, gss_in, gss_out, age, sex) %>%
    summarise(!!flow_col := sum(flow)) %>%
    as.data.frame()
  
  return(list(regional_flow = regional_flow,
              national_flow = national_flow,
              sub_regional_flow = sub_regional_flow))
  
}
