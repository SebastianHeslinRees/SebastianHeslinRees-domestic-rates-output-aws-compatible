#' Calculate average international rates based on past data
#'
#' A wrapper for \code{calculate_mean_from_backseries} which prepares mye
#' and international in/out flow component data.
#'
#' @param popn_mye_path Character. The path to the mye backseries population
#'   when \code{flow_or_rate} is \code{"rate"}.
#' @param births_mye_path Character. The path to the mye births backseries when
#'   \code{flow_or_rate} is \code{"rate"}.
#' @param flow_or_rate Character. Either "flow" or "rate" - determines whether
#'   the function returns absolute flows or migration rates.
#' @param component_path Character The path to the international in or out
#'   migration component from the mye.
#' @param last_data_year Integer. The last year of data on which to calculate
#'   averages.
#' @param n_years_to_avg Integer. The number of years to use in calculating
#'   averages.
#' @param data_col Character. The column in the component dataframe containing
#'   the flow data.
#' @param first_proj_yr Numeric. The first year of the model's projection (may
#'   not always be the last_data_year + 1).
#' @param n_proj_yr Numeric. The number of years to project forward.
#' @param rate_cap Numeric. A cap constraining the maximum rates output.
#' @param modify_rates_and_flows Numeric. A factor to scale the output rates or
#'   flows by. Applied last, but before the cap. Default 1.
#' @param project_rate_from Numeric. The year for which the average being
#'   calculated is to be used. Default \code{last_data_year+1}.
#'
#' @return A data frame of international migration probabilities.
#'
#' @import dplyr
#' @importFrom assertthat assert_that
#' @importFrom utils capture.output
#'
#' @export

calculate_mean_international_rates_or_flows <- function(popn_mye_path, births_mye_path, flow_or_rate,
                                                        component_path, last_data_year, n_years_to_avg, data_col,
                                                        first_proj_yr, n_proj_yr, rate_cap, modify_rates_and_flows=1,
                                                        project_rate_from = last_data_year+1) {

  population <- .path_or_dataframe(popn_mye_path)
  births <- .path_or_dataframe(births_mye_path)
  component <- .path_or_dataframe(component_path)

  if(flow_or_rate == "rate"){

    population <- population %>%
      popn_age_on(births = births) %>%
      data.frame()

    rate_backseries <- population %>%
      filter(year %in% unique(component$year)) %>%
      left_join(component, by=c("gss_code","year","sex","age")) %>%
      rename(value = data_col) %>%
      mutate(rate = ifelse(popn == 0, 0, value/popn)) %>%
      select(gss_code, year, sex, age, rate)


    rates <- calculate_mean_from_backseries(rate_backseries, n_years_to_avg, last_data_year, "rate",
                                            col_aggregation = c("gss_code","sex","age"),
                                            project_rate_from = project_rate_from)

    #Document any rates that are capped
    if(any(rates$rate > rate_cap)) {
      ix <- rates$rate > rate_cap
      n <- sum(ix)

      warning(paste0(capture.output({
        print(paste(c("In averaging the backseries, calculate_international_rates_and_flows created rates exceeding the cap of", rate_cap,
                      "at", n, "aggregation levels - these will be changed to", rate_cap),
                    collapse = " "))
        if(sum(ix) < 30) {
          print("Values:")
          print(rates[ix,])
        } else {
          print("First 30 values:")
          print(rates[ix,][1:30,])
          print("Levels affected:")
          sapply(c("gss_out", "age", "sex"), function(col) {
            print("col:")
            print(unique(rates[ix, get(col)]))
          })
        }
      }), collapse = "\n"))

      rates <-  rates %>%
        mutate(rate = rate*modify_rates_and_flows) %>%
        mutate(rate = ifelse(rate > rate_cap, rate_cap, rate))

    }


    last_proj_yr <- n_proj_yr + first_proj_yr - 1

    output_df <- project_forward_flat(df = rates, last_proj_yr = last_proj_yr) %>%
      rename(!!data_col := rate) %>%
      select(year, gss_code, sex, age, data_col)
  }

  if(flow_or_rate == "flow"){

    back_years <- c((last_data_year - n_years_to_avg + 1):last_data_year)

    assert_that(all(back_years %in% unique(component$year)),
                msg=("in calculate_international_rates_and_flows backyears must be present in component data"))

    flows <- component %>%
      rename(value = data_col) %>%
      filter(year %in% back_years) %>%
      group_by(gss_code, sex, age) %>%
      summarise(value = sum(value)/n_years_to_avg, .groups = 'drop_last') %>%
      ungroup() %>%
      mutate(value = value*modify_rates_and_flows) %>%
      rename(!!data_col := value) %>%
      mutate(year = project_rate_from)

    last_proj_yr <- n_proj_yr + first_proj_yr - 1

    output_df <- project_forward_flat(df = flows, last_proj_yr = last_proj_yr) %>%
      select(year, gss_code, sex, age, data_col)

  }

  return(as.data.frame(output_df))

}
