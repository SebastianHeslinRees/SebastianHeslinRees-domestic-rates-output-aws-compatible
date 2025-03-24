#' Scale births to match national totals by sex and age
#'
#' A wrapper for \code{calculate_scaling_factors} which prepares births data to
#' feed into the function and then calculates scaled births.
#'
#' Constraints are applied to sex, age and country (which is inferred from
#' gss_code), with each combination of these variables (where available)
#' constrained to the NPP national total.
#'
#' The NPP births constraint is provided for ages 15-46 while the model produces
#' births for ages 15-49. This function wrangles the input birth to bin together
#' all births from ages higher than the NPP constraints in the highest bin. (It
#' does the same for ages lower, but throws an error if it sees any because
#' there shouldn't be any.)
#'
#' @param births A data frame containing births by mother's age data.
#' @param constraint A data frame containing births data at the same
#'   resolution or lower.
#'
#' @return A data frame of scaled births by mother's year of age.
#'
#' @import dplyr
#' @import assertthat
#' @importFrom testthat expect_equal
#'
#' @export

constrain_births <- function(births, constraint){

  births <- births %>%
    select(year, gss_code, sex, age, births)

  max_constraint_age <- max(constraint$age)
  min_constraint_age <- min(constraint$age)

  births <- mutate(births, country = substr(gss_code,1,1))
  if(!"country" %in% names(constraint) & "gss_code" %in% names(constraint)) {
    constraint <- mutate(constraint, country = substr(gss_code,1,1))
  }

  #group ages in births that are >= max age and <= min age
  # (first check there are no births below the minimum constraint age - there shouldn't be, though the code would be fine if there were...)
  assert_that(sum(filter(births, age < min_constraint_age)$births) == 0)

  births <- mutate(births, age_band = ifelse(age < min_constraint_age, min_constraint_age,
                                             ifelse(age > max_constraint_age, max_constraint_age, age)))

  constraint <- rename(constraint, age_band = age)

  scaling <- calculate_scaling_factors(births, constraint, col_popn="births",
                                       col_aggregation = c("year","sex","age_band","country"),
                                       rows_to_constrain = births$country %in% unique(constraint$country) & births$sex == "female") %>%
    mutate(births = births * scaling) %>%
    select(year, gss_code, sex, age, births)

  assert_that(all(complete.cases(births)))

  return(scaling)
}
