library(popmodules)
library(testthat)
library(dplyr)


#Test 1 check the function scales properly

popn <- expand.grid(year=2000, age=20:21, gss_code=c("E01","E02","S03"), sex=c("female","male"), popn = 100, stringsAsFactors = FALSE)

constraint <- expand.grid(year=2000, gss_code=c("E01","E02","S03"), popn = 800, stringsAsFactors = FALSE)

output <- expand.grid(year=2000, age=20:21, gss_code=c("E01","E02"), sex=c("female","male"), popn = 100, scaling = 2, stringsAsFactors = FALSE) %>%
  rbind(expand.grid(year=2000, age=20:21, gss_code="S03", sex=c("female","male"), popn = 100, scaling = 1, stringsAsFactors = FALSE)) %>%
  arrange(sex, gss_code, age) %>% 
  data.frame()

#-------------------------------------------------------------------------


test_that("calculate_scaling_factors can scale up and down", {
  output_out <- mutate(output, scaling = 2)
  expect_equivalent(calculate_scaling_factors(popn,
                                   constraint,
                                   col_aggregation = c("year", "gss_code"),
                                   col_popn = "popn",
                                   col_constraint = "popn"),
                    output_out)


  #test scale down
  constraint_in <- dplyr::mutate(constraint, popn = popn/4)
  output_out <- dplyr::mutate(output, scaling = 0.5)

  expect_equivalent(calculate_scaling_factors(popn, constraint_in,
                                              col_aggregation = c("year", "gss_code"),
                                              col_popn = "popn"),
                    output_out)

  # but warns when the population it's trying to scale is zero
  popn_in <- mutate(popn, popn = 0)
  output_out <- mutate(popn_in, scaling = 0) %>%
    arrange(sex, gss_code, age) %>% 
    data.frame()
  expect_warning(temp <- calculate_scaling_factors(popn_in,
                                                   constraint,
                                                   col_aggregation = c("year", "gss_code"),
                                                   col_popn = "popn",
                                                   col_constraint = "popn"))
  expect_equal(temp, output_out)
})

test_that("calculate_scaling_factors handles the case when the constraint isn't yet aggregated", {
  constraint_in <- mutate(popn, popn = popn*2)
  output_out <- mutate(output, scaling = 2)
  expect_equal(calculate_scaling_factors(popn,
                                         constraint_in,
                                         col_aggregation = c("year", "gss_code"),
                                         col_popn = "popn",
                                         col_constraint = "popn"),
                output_out)
})



test_that("calculate_scaling_factors works when passing a logical and dplyr-style vector of values to scale over", {
  expect_equivalent(calculate_scaling_factors(popn,
                                   constraint,
                                   col_aggregation = c("year", "gss_code"),
                                   col_popn = "popn",
                                   col_constraint = "popn",
                                   rows_to_constrain = grepl("^E", popn$gss_code)),
                    output)

  expect_equivalent(calculate_scaling_factors(popn,
                                              constraint,
                                              col_aggregation = c("year", "gss_code"),
                                              col_popn = "popn",
                                              col_constraint = "popn",
                                              rows_to_constrain = grepl("^E", gss_code)),
                    output)


  # but fails when it's not split neatly by aggregation level
  expect_error(calculate_scaling_factors(popn,
                                         constraint,
                                         col_aggregation = c("year", "gss_code"),
                                         col_popn = "popn",
                                         col_constraint = "popn",
                                         rows_to_constrain = sex == "female"))
})

#Test it can handle a single number
test_that("calculate_scaling_factors can scale up and down", {
  constraint <- data.frame(year = 2000, popn = 2400)
  output_out <- mutate(output, scaling = 2) %>%
    arrange(sex, gss_code, age)
  expect_equivalent(calculate_scaling_factors(popn,
                                   constraint=constraint,
                                   col_aggregation = "year",
                                   col_popn = "popn",
                                   col_constraint = "popn"),
                    output_out)
})

#Force errors
test_that("calculate_scaling_factors should throw an error", {
  expect_error(calculate_scaling_factors(popn,
                              constraint=constraint,
                              col_aggregation = c("year","age"),
                              col_popn = "popn",
                              col_constraint = "popn"))

  expect_error(calculate_scaling_factors(popn,
                              constraint=constraint,
                              col_aggregation = "year",
                              col_popn = "trevor",
                              col_constraint = "popn"))

  expect_error(calculate_scaling_factors(popn,
                              constraint=constraint,
                              col_aggregation = "year",
                              col_popn = "popn",
                              col_constraint = "cindy"))
})

