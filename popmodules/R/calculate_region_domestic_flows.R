#' Calculate domestic in, out and net migration for a group of LAs
#'
#' @param domestic_rates A data frame containing a rates of domestic migration
#'   between all LAs by age and sex
#' @param nat_chng_popn A data frame containing natural change population by
#'   sex and age
#' @param gss_code_list A character vector containing gss codes
#' @param region_name A string giving the name of the region being output
#'
#' @return A data frame with domestic in, out and net flows by sex, age and year
#'   for the projection period
#'
#' @import dplyr
#'
#' @export

calculate_region_domestic_flows <- function(domestic_rates, nat_chng_popn, gss_code_list, region_name){

  dom_out_rates <- domestic_rates %>%
    filter(gss_out %in% gss_code_list) %>%
    filter(!gss_in %in% gss_code_list) %>%
    rename(gss_code = gss_out) %>%
    select(-gss_in)

  dom_in_rates <- domestic_rates %>%
    filter(gss_in %in% gss_code_list) %>%
    filter(!gss_out %in% gss_code_list) %>%
    rename(gss_code = gss_out) %>%
    select(-gss_in)

  dom_out_list <- list()
  dom_in_list <- list()

  for(yr in min(nat_chng_popn$year):max(nat_chng_popn$year)){
    dom_out_list[[yr]] <- left_join(dom_out_rates, filter(nat_chng_popn, year == yr),
                                    by = c("gss_code", "age", "sex")) %>%
      mutate(dom_out = rate*popn)

    dom_in_list[[yr]] <- left_join(dom_in_rates, filter(nat_chng_popn, year == yr),
                                   by = c("gss_code", "age", "sex")) %>%
      mutate(dom_in = rate*popn)
  }

  dom_out <- data.table::rbindlist(dom_out_list) %>%
    dtplyr::lazy_dt() %>% 
    group_by(year) %>%
    summarise(dom_out = sum(dom_out)) %>% 
    data.frame() #%>%
   # rbind(historic_out)

  dom_in <- data.table::rbindlist(dom_in_list) %>%
    dtplyr::lazy_dt() %>% 
    group_by(year) %>%
    summarise(dom_in = sum(dom_in)) %>% 
    data.frame() #%>%
    #rbind(historic_in)

  dom_net <- left_join(dom_out, dom_in, by="year") %>%
    mutate(dom_net = dom_in-dom_out) %>%
    mutate(area = region_name)

  dom_net_with_name <- select(dom_net, area, year, dom_in, dom_out, dom_net) %>%
    as.data.frame()

  return(dom_net_with_name)

}

