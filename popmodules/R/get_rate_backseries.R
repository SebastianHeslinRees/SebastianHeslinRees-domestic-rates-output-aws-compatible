#' Divide a component of change by population to give historical rates
#'
#' Takes file paths to the mid year estimates for the desired component of
#' change, total population, and birth rates, and returns a data frame with
#' rates.
#'
#' The function assumes that MYE data has the standard mid-year-estimate
#' aggregation columns: \code{c("gss_code","sex","age","year")}. The component
#' file is assumed to have the same columns unless this is specified in the
#' \code{col_aggregation} parameter and with data stored in the \code{value}
#' column Note, however, that a column named "year" is currently required in all
#' input datasets.
#'
#' If the components of change data is known to be at fewer levels than the
#' population mid-year estimates, then the \code{col_partial_match} parameter
#' should name the columns where this is the case. This will be true for example
#' with fertility data, which is limited to a certain age range.
#'
#' The name of the component of change in question need not be specified if it
#' is one of these ONS default MYE components: "births", "deaths", "popn",
#' "int_in", "int_out", "int_net", "dom_in", "dom_out", "dom_net". Otherwise,
#' specify it via the \code{col_component} parameter.
#'
#' Due to the way data comparisons work, the population must be aged on as part
#' of the calculations, and the function also needs births estimates as input.
#'
#' @param component_mye_path Path to the mid-year estimates of the desired
#'   component of change.
#' @param popn_mye_path Path to population mid-year estimates.
#' @param births_mye_path Path to births mid-year estimates.
#' @param years_backseries The years for which the backseries are to be calculated. Numeric vector.
#' @param col_partial_match List of columns in \code{component_mye_path} where
#'   levels are a subset of those in \code{popn_mye_path}.
#' @param col_aggregation Vector of column names used for aggregation in the
#'   input in \code{component_mye_path}. If column names are different from MYE
#'   columns, specify the mapping from MYE to the component data with a named
#'   character vector. e.g. in origin-destination data,
#'   \code{c("gss_code"="gss_out", "gss_in")}. Default
#'   \code{c("year","gss_code","age","sex")}.
#' @param col_component String. Column name for data in the
#'   \code{component_mye_path} data frame. Need not be specified when one of
#'   "births", "deaths", "popn", "int_in", "int_out", "int_net", "dom_in",
#'   "dom_out", "dom_net".
#' @param rate_cap Numeric. Upper limits to values in the \code{col_component}
#'   column. NULL applies no limits. Default 1.
#'
#' @return A data frame containing rates for the component of change (in the
#'   "rate" column)
#'
#' @import dplyr
#' @importFrom assertthat assert_that
#'
#' @export

get_rate_backseries <- function(component_mye_path,
                                popn_mye_path,
                                births_mye_path,
                                years_backseries,
                                col_partial_match = NULL,
                                col_aggregation = c("year","gss_code","age","sex"),
                                col_component = NULL,
                                rate_cap = 1) {
  
  # TODO split this into two functions - can a function for domestic migration matrix be built as a
  # separate function which calls this one?  This function has become hard to read.
  
  # TODO To simplify the function make it strict on passing the col_component name
  
  # List valid names of population components
  usual_col_component <- c("births", "deaths", "popn", "int_in", "int_out", "int_net", "dom_in", "dom_out", "dom_net")
  
  
  # LOAD AND SET UP POPULATION DATA
  # -------------------------------
  
  # population is aged on due to definitions of MYE to ensure the correct denominator
  # population is population at 30th June
  # changes are changes that occurred in the 12 months up to 30th June
  # age is the age the cohort is at 30th June
  # TODO add link to ONS documentation for the above
  popn <- .path_or_dataframe(popn_mye_path)
  births <- .path_or_dataframe(births_mye_path)
  component <- .path_or_dataframe(component_mye_path)
  
  validate_get_rate_backseries_inputs(popn, births, component, years_backseries,
                                      col_partial_match, col_aggregation, col_component, rate_cap)
  
  
  popn <- popn  %>%
    popn_age_on(births = births)%>%
    filter(year %in% years_backseries) %>%
    # validate_population(col_data = "popn",
    #                     test_complete = TRUE,
    #                     test_unique = TRUE,
    #                     check_negative_values = TRUE)
  
  
  # LOAD AND SET UP COMPONENT DATA
  # ------------------------------

  print(names(component))  # This will show the column names in the component dataset
  
  if(is.null(col_component)) {
    col_component <- usual_col_component
  }
  col_component <- intersect(names(component), col_component)
  
  assert_that(length(col_component) == 1,
              msg = paste(c("get_rate_backseries couldn't find a unique column for the component in the file",
                            component_mye_path,
                            "\nColumn names:", names(component),
                            "\nComponent names searched for:", col_component), collapse=" "))
  
  # WRANGLE AND VALIDATE THE DATASETS FOR A JOIN
  # --------------------------------------------
  
  
  # Fill missing values as zero
  # TODO throw error or write a message if there are NAs?
  ix <- is.na(component[[col_component]])
  component[ix, col_component] <- 0
  
  # Set negative values to zero
  component <- check_negative_values(component, col_component)
  
  # TODO Figure out if this is necessary
  # levels <- col_aggregation[ !col_aggregation %in% col_partial_match]
  # if(!is.null(col_partial_match)) {
  #   n_levels_popn <- popn[levels] %>%
  #     sapply(function(col) length(unique(col))) %>%
  #     prod()
  #   n_levels_component <- component[levels] %>%
  #     sapply(function(col) length(unique(col))) %>%
  #     prod()
  #   assert_that(n_levels_component == n_levels_popn,
  #               msg = paste(c("get_rate_backseries found differering aggregation levels in the populations at",
  #                             component_mye_path, "and", popn_mye_path, "when comparing on", levels), collapse=" "))
  # }
  
  # validate the component population and check levels match popn for the join
  col_aggregation <- .convert_to_named_vector(col_aggregation)
  join_by <- col_aggregation[ names(col_aggregation) %in% names(popn)]
  
  popmodules::validate_population(component,
                                  col_aggregation = unname(col_aggregation),
                                  col_data = col_component,
                                  test_complete = is.null(col_partial_match),
                                  test_unique = TRUE,
                                  check_negative_values = TRUE)
  
  # TODO investigate another way to validate this join: currently it's failing due to insufficient memory
  
  # (when it's run as part of the model with a bunch of other memory usage)
  #popmodules::validate_join_population(popn,
  #                                     component,
  #                                     cols_common_aggregation = join_by,
  #                                     aggregation_levels_match = !is.null(col_partial_match),
  #                                     many2one = !any(names(popn) %in% col_aggregation),
  #                                     one2many = !any(col_aggregation %in% names(popn)),
  #                                     warn_unused_shared_cols = FALSE)
  
  # TODO check the methodological decision to set value to 0 if popn is 0
  
  
  
  # JOIN THE DATASETS
  # -----------------
  
  # we're gonna use data.table because the data are huge when we're dealing with origin-destination data
  # see below for tidyverse equivalent
  
  popn <- data.table::setDT(popn)
  #data.table::setkeyv(popn, c("year","gss_code","age","sex"))
  component <- data.table::setDT(component)
  #data.table::setkeyv(component, unname(col_aggregation))
  
  rates <- merge(popn, component, by.x = names(join_by), by.y = unname(join_by))
  rates[, rate := ifelse(popn == 0, 0, get(col_component)/popn)]
  data.table::setnames(rates, names(join_by), unname(join_by))
  data.table::setDF(rates)
  rates <- select(rates, -popn, -!!sym(col_component))
  
  if(FALSE) { # the above but with tidyverse
    rates <- left_join(popn, component, by=col_aggregation) %>%
      mutate(rate = ifelse(popn == 0, 0, !!sym(col_component)/popn)) %>%
      select(-popn, -!!sym(col_component)) %>%
      rename(setNames(names(join_by), unname(join_by))) # check this...
  }
  
  
  if(!is.null(rate_cap)) {
    ix <- rates$rate > rate_cap
    if(any(ix)) {
      n <- sum(ix)
      warning(paste(c("get_rate_backseries found", n, "rates >", rate_cap, "- these will be capped.",
                      "\nInput was", component_mye_path, "and", popn_mye_path,
                      "\nLocations:", unique(rates[ix, "gss_code"]),
                      "\nTODO: edit code to move this to happen after the rates have been averaged in the next step"), collapse = " "))
      rates$rate[ix] <- 1
    }
  }
  
  return(rates)
  
  # TODO add module function rules checks - must return fert/mort df
  
}

validate_get_rate_backseries_inputs <- function(population,
                                                births,
                                                component,
                                                years_backseries,
                                                col_partial_match,
                                                col_aggregation,
                                                col_component,
                                                rate_cap){
  
  assert_that(is.data.frame(population),
              msg="get_rate_backseries expects that population is a data frame")
  assert_that(is.data.frame(births),
              msg="get_rate_backseries expects that births is a data frame")
  assert_that(is.data.frame(component),
              msg="get_rate_backseries expects that the component is a data frame")
  assert_that(is.numeric(years_backseries),
              msg="get_rate_backseries expects that years_backseries is a object")
  assert_that(is.null(col_partial_match) | is.character(col_partial_match),
              msg="get_rate_backseries expects that col_partial_match is a character object")
  assert_that(is.character(col_aggregation),
              msg="get_rate_backseries expects that col_aggregation is a character object")
  assert_that(is.null(col_component) | is.string(col_component),
              msg="get_rate_backseries expects that col_component is a single string")
  
  assert_that(is.numeric(population$year),
              msg = "get_rate_backseries expects that year column in the population dataframe is numeric")
  assert_that(is.numeric(births$year),
              msg = "get_rate_backseries expects that year column in the births dataframe is numeric")
  assert_that(is.numeric(component$year),
              msg = "get_rate_backseries expects that year column in the component dataframe is numeric")
  assert_that(is.null(rate_cap) | is.number(rate_cap),
              msg = "get_rate_backseries expects a single number for rate_cap")
  
  
  # validate_population(population, col_aggregation = c("gss_code", "year", "sex", "age"), col_data = "popn",
  #                     test_complete = TRUE, test_unique = TRUE, check_negative_values = TRUE)
  # validate_population(births, col_aggregation = c("gss_code", "year", "sex", "age"), col_data = "births",
  #                     test_complete = TRUE, test_unique = TRUE, check_negative_values = TRUE)
  
  assert_that(all(years_backseries %in% population$year),
              msg = "get_rate_backseries expects the population dataframe to contain all of the years in years_backseries")
  
  assert_that(all(years_backseries %in% births$year),
              msg = "get_rate_backseries expects the births dataframe to contain all of the years in years_backseries")
  
  assert_that(all(years_backseries %in% component$year),
              msg = "get_rate_backseries expects the component dataframe to contain all of the years in years_backseries")
  
  invisible(TRUE)
}
