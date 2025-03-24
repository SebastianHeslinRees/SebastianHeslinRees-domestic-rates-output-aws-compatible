library(testthat)
library(dplyr)

#### Basic functionality and calculation tests ####

#test 1: multiply 2 numbers, use function defaults
test_popn <- data.frame(gss_code=c("a","b"), popn = 100, stringsAsFactors = FALSE)
test_rate <- data.frame(gss_code=c("a","b"), rate = 0.5, stringsAsFactors = FALSE)
test_expect <- data.frame(gss_code=c("a","b"), component = 50, stringsAsFactors = FALSE)

test_that("apply_rate_to_population test 1", {
  expect_equivalent(test_expect, apply_rate_to_population(test_popn,
                                                          test_rate,
                                                          col_aggregation = "gss_code"))
})

#test 2: test column name parameters work as expected
test_popn <- data.frame(gss_code=c("a","b"), population = 100, stringsAsFactors = FALSE)
test_rate <- data.frame(gss_code=c("a","b"), rate_of_thing = 0.5, stringsAsFactors = FALSE)
test_expect <- data.frame(gss_code=c("a","b"), thing_output = 50, stringsAsFactors = FALSE)

test_that("apply_rate_to_population test 2", {
  expect_equivalent(test_expect, apply_rate_to_population(test_popn,
                                                          test_rate,
                                                          col_aggregation = "gss_code",
                                                          col_popn = "population",
                                                          col_rate = "rate_of_thing",
                                                          col_out = "thing_output"))
})


#test 3: output column name can be the same as an input column name
test_popn <- data.frame(gss_code=c("a","b"), popn = 100, stringsAsFactors = FALSE)
test_rate <- data.frame(gss_code=c("a","b"), rate = 0.5, stringsAsFactors = FALSE)
test_expect <- data.frame(gss_code=c("a","b"), popn = 50, stringsAsFactors = FALSE)

test_that("apply_rate_to_population test 3", {
  
  test_expect <- data.frame(gss_code=c("a","b"), popn = 50, stringsAsFactors = FALSE)
  expect_equivalent(apply_rate_to_population(test_popn,
                                             test_rate,
                                             col_aggregation = "gss_code",
                                             col_out = "popn"),
                    test_expect)
  
  test_expect <- data.frame(gss_code=c("a","b"), rate = 50, stringsAsFactors = FALSE)
  expect_equivalent(apply_rate_to_population(test_popn,
                                             test_rate,
                                             col_aggregation = "gss_code",
                                             col_out = "rate"),
                    test_expect)
})


#test 4: col_aggregation handles mapping
test_popn <- expand.grid(xgss_code=c("a","b"), xsex = c("female","male"), year = 2001:2003, popn = 100, stringsAsFactors = FALSE)
test_rate <- expand.grid(gss_code=c("a","b"), sex = c("female","male"), year = 2001:2003, rate = 0.5, stringsAsFactors = FALSE)
test_expect <- expand.grid(xgss_code=c("a","b"), xsex = c("female","male"), year = 2001:2003, component = 50, stringsAsFactors = FALSE)

test_that("apply_rate_to_population test 4", {
  
  expect_equivalent(apply_rate_to_population(test_popn,
                                             test_rate,
                                             col_aggregation = c("xgss_code"="gss_code", "xsex"="sex", "year")),
                    test_expect)
  
})

#test 5: rate columns are dropped
test_popn <- data.frame(gss_code=c("a","b"), popn = 100, stringsAsFactors = FALSE)
test_rate <- data.frame(gss_code=c("a","b"), rate = 0.5, y = "nothing", stringsAsFactors = FALSE)
test_expect <- data.frame(gss_code=c("a","b"), popn = 50, stringsAsFactors = FALSE)

test_that("apply_rate_to_population test 5", {
  
  expect_equivalent(apply_rate_to_population(test_popn,
                                             test_rate,
                                             col_aggregation = "gss_code"),
                    test_expect)
  
})


#test 6: popn columns are dropped
test_popn <- data.frame(gss_code=c("a","b"), popn = 100, x = "nothing", stringsAsFactors = FALSE)
test_rate <- data.frame(gss_code=c("a","b"), rate = 0.5, stringsAsFactors = FALSE)
test_expect <- data.frame(gss_code=c("a","b"), popn = 50, stringsAsFactors = FALSE)

test_that("apply_rate_to_population test 6", {
  
  expect_equivalent(apply_rate_to_population(test_popn,
                                             test_rate,
                                             col_aggregation = "gss_code"),
                    test_expect)
  
})


#test 7: specified columns are retained
test_popn <- data.frame(gss_code=c("a","b"), popn = 100, x = "nothing", stringsAsFactors = FALSE)
test_rate <- data.frame(gss_code=c("a","b"), rate = 0.5, y = "nothing", stringsAsFactors = FALSE)
test_expect <- data.frame(gss_code=c("a","b"), x = "nothing", y = "nothing", popn = 50, stringsAsFactors = FALSE)

test_that("apply_rate_to_population test 7", {
  
  expect_equivalent(apply_rate_to_population(test_popn,
                                             test_rate,
                                             col_aggregation = "gss_code",
                                             additional_popn_cols = "x",
                                             additional_rate_cols = "y"),
                    test_expect)
  
})

#test 8: check factors work
test_popn <- data.frame(gss_code = as.factor(c("a","b")), popn = 100, stringsAsFactors = FALSE)
test_rate <- data.frame(gss_code = as.factor(c("a","b")), rate = 0.5, stringsAsFactors = FALSE)
test_expect <- data.frame(gss_code = as.factor(c("a","b")), popn = 50, stringsAsFactors = FALSE)

test_that("apply_rate_to_population test 8", {
  
  expect_equivalent(apply_rate_to_population(test_popn,
                                             test_rate,
                                             col_aggregation = "gss_code"),
                    test_expect)
  
})

#--------------------------------

#### Expect ERRORs ####

#test error 1: columns with same names in input dataframes
test_popn <- data.frame(gss_code=c("a","b"), popn = 100, stringsAsFactors = FALSE)
test_rate <- data.frame(gss_code=c("a","b"), popn = 0.5, stringsAsFactors = FALSE)

test_that("apply_rate_to_population test error 1", {
  expect_error(apply_rate_to_population(test_popn,
                                        test_rate,
                                        col_rate = "popn",
                                        col_aggregation = "gss_code"))
})


#test error 2: col name not in dataframe
test_that("apply_rate_to_population test error 2", {
  expect_error(apply_rate_to_population(test_popn,
                                        test_rate,
                                        col_popn = "yellow",
                                        col_aggregation = "gss_code"))
  
  expect_error(apply_rate_to_population(test_popn,
                                        test_rate,
                                        col_rate = "yellow",
                                        col_aggregation = "gss_code"))
  
})


#test error 3: type errors
test_popn <- data.frame(gss_code=c("a","b"), popn = 100, stringsAsFactors = FALSE)
test_rate <- data.frame(gss_code=c("a","b"), rate = 0.5, stringsAsFactors = FALSE)

test_that("apply_rate_to_population test error 3", {
  
  expect_error(apply_rate_to_population("a",
                                        test_rate,
                                        col_aggregation = "gss_code"))
  
  expect_error(apply_rate_to_population(test_popn,
                                        "a",
                                        col_aggregation = "gss_code"))
  
  
  expect_error(apply_rate_to_population(test_popn,
                                        test_rate,
                                        col_aggregation = 6))
  
  expect_error(apply_rate_to_population(test_popn,
                                        test_rate,
                                        col_aggregation = "gss_code",
                                        col_popn = 14))
  
  expect_error(apply_rate_to_population(test_popn,
                                        test_rate,
                                        col_aggregation = "gss_code",
                                        col_rate = NA))
  
  expect_error(apply_rate_to_population(test_popn,
                                        test_rate,
                                        col_aggregation = "gss_code",
                                        col_out = FALSE))
})

#test error 4: col_aggregation
test_that("apply_rate_to_population test error 4", {
  expect_error(apply_rate_to_population(test_popn,
                                        test_rate,
                                        col_aggregation = "year"))
  
  expect_error(apply_rate_to_population(test_popn,
                                        test_rate,
                                        col_aggregation = "gss_code",
                                        col_out = "gss_code"))
})

#test error 5: col_geog NULL when validate_geog TRUE
test_that("apply_rate_to_population test error 5", {
  expect_error(apply_rate_to_population(test_popn,
                                        test_rate,
                                        col_aggregation = "gss_code",
                                        validate_geog = TRUE,
                                        col_geog = NULL))
})