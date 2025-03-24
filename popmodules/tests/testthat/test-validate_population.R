library(testthat)
library(dplyr)
library(tidyr)

# Set up valid population data frame with default column names
pop_test1 <- data.frame( area=c("a","b","c","d"), stringsAsFactors = FALSE)

pop_test2 <- expand.grid( area=c("a","b","c","d"), age = 0:3, stringsAsFactors = FALSE)

pop_test3 <- mutate(pop_test2, popn = 10000)

set.seed(101)

mort_test4 <- pop_test2 %>%
  expand(area, age, year = 2002) %>%
  mutate(mort = rnorm(n(), mean = 0.1, sd = 0.001)) %>%
  as.data.frame()

pop <- mutate(pop_test3, popn2 = "fill")


# Run tests

test_that("validate_population validates correct data frames and tibbles without modifying them", {
  expect_identical(
    validate_population(pop, col_aggregation = c("area","age"), col_data = "popn"),
    pop)
  
  pop_in <- mutate(pop, age=as.factor(age), area=as.factor(area))
  expect_identical(
    validate_population(pop_in, col_aggregation = c("area","age"), col_data = "popn"),
    pop_in)
  
  pop_in <- as_tibble(pop_in)
  expect_identical(
    validate_population(pop_in, col_aggregation = c("area","age"), col_data = "popn"),
    pop_in)
  
  pop_in <- group_by(pop_in, area, age)
  expect_identical(
    temp <- validate_population(pop_in, col_aggregation = c("area","age"), col_data = "popn"),
    pop_in)
})

test_that("validate_population warns with an empty data frame", {
  expect_warning(
    temp <- validate_population(pop[NULL,], col_aggregation = c("area","age"), col_data = "popn"))
  expect_equal(temp, pop[NULL,])
})

# validate_pop returns invisibly. Now that we trust it's returning the input
# ok, we don't need to check the output every time and use expect_invisible()

test_that("validate_population can deal with different numbers of data and aggregation columns", {
  expect_invisible(
    validate_population(pop_test1, col_aggregation = "area", col_data = NA))
  
  expect_invisible(
    validate_population(pop_test2, col_aggregation = c("area","age"), col_data = NA))
  
  expect_invisible(
    validate_population(pop_test3, col_aggregation = c("area","age"), col_data = "popn"))
  
  expect_invisible(
    validate_population(pop, col_aggregation = c("area","age"), col_data = "popn"))
  
  expect_invisible(
    validate_population(pop, col_aggregation = c("area","age"), col_data = c("popn","popn2")))
})


test_that("validate_population preserves non-aggregation and non-data columns in output", {
  expect_equal(
    validate_population(pop, col_aggregation = c("area","age"), col_data = "popn"),
    pop)
})


test_that("validate_population warns when expected columns aren't present", {
  expect_warning(
    validate_population(pop_test1, col_aggregation = c("area","ABSENT"), col_data = NA),
    "Function validate_population expected column ABSENT - not found")
  
  expect_warning(
    validate_population(pop_test1, col_aggregation = "area", col_data = "ABSENT"),
    "Function validate_population expected column ABSENT - not found")
})


test_that("validate_population throws an error when no valid aggregation columns are provided", {
  expect_error(
    validate_population(pop_test1, col_aggregation = NA, col_data = NA),
    "validate_population requires a vector of column names for input parameter col_aggregation")
})


test_that("validate_population warns when 'data' and 'aggregation' columns overlap", {
  expect_warning(
    validate_population(pop_test1, col_aggregation = c("area"), col_data = c("area")),
    "Column area in both aggregation and data columns provided to validate_population")
  
  expect_warning(
    validate_population(pop_test2, col_aggregation = c("area","age"), col_data = c("age")),
    "Column age in both aggregation and data columns provided to validate_population")
  
  expect_warning(
    validate_population(pop_test3, col_aggregation = c("area","age"), col_data = c("age","popn")),
    "Column age in both aggregation and data columns provided to validate_population")
  
  expect_warning(
    validate_population(pop_test3, col_aggregation = c("area","age", "popn"), col_data = c("age","popn")),
    "Column age in both aggregation and data columns provided to validate_population\nColumn popn in both aggregation and data columns provided to validate_population")
})


test_that("validate_population spots missing aggregation levels", {
  expect_invisible(
    validate_population(pop_test2[-1,], col_aggregation = c("area","age"), col_data = NA, test_complete = FALSE))
  
  expect_error(
    validate_population(pop_test2[-1,], col_aggregation = c("area","age"), col_data = NA, test_complete = TRUE))
})


test_that("validate_population spots duplicate aggregation levels", {
  pop_in <- rbind(pop_test2, pop_test2)
  
  expect_invisible(
    validate_population(pop_in, col_aggregation = c("area","age"), col_data = NA, test_unique = FALSE))
  
  expect_error(
    validate_population(pop_in, col_aggregation = c("area","age"), col_data = NA, test_unique = TRUE))
})


test_that("validate_population ignores (but warns for) unused factor levels", {
  pop_in <- data.frame( area=factor(c("a","b"), levels=c("a","b","c")))
  
  expect_warning(
    temp <- validate_population(pop_in, col_aggregation = "area", col_data = NA))
  expect_equal(temp, pop_in)
})

test_that("validate_population spots negative counts", {
  pop_in <- mutate(pop, popn=-1)
  expect_invisible(
    validate_population(pop_in, col_aggregation = c("area","age"), col_data = "popn", check_negative_values = FALSE))
  
  expect_error(
    validate_population(pop_in, col_aggregation = c("area","age"), col_data = "popn", check_negative_values = TRUE),
    "validate_population found negative values in column popn")
})


# tests for comparison populations

test_that("validate_population validates against comparison data frames and tibbles", {
  expect_invisible(
    validate_population(pop, col_aggregation = c("area","age"), col_data = "popn", comparison_pop = pop))
  
  pop_in <- mutate(pop, age=as.factor(age), area=as.factor(area))
  expect_invisible(
    validate_population(pop_in, col_aggregation = c("area","age"), col_data = "popn", comparison_pop = pop_in))
  
  pop_in <- as_tibble(pop_in)
  expect_invisible(
    validate_population(pop_in, col_aggregation = c("area","age"), col_data = "popn", comparison_pop = pop_in))
})

test_that("validate_population validates against comparison populations with different numbers of data and aggregation columns", {
  expect_invisible(
    validate_population(pop_test1, col_aggregation = "area", col_data = NA, comparison_pop = pop_test1))
  
  expect_invisible(
    validate_population(pop_test2, col_aggregation = c("area","age"), col_data = NA, comparison_pop = pop_test2))
})

test_that("validate_population throws an error when the comparison population is invalid", {
  expect_error(
    validate_population(pop, col_aggregation = "area", col_data = NA, comparison_pop = 1:5))
  
  magnitude <- rbind(pop, pop)
  expect_error(
    validate_population(pop, col_aggregation = "area", col_data = NA, comparison_pop = magnitude))
})

test_that("validate_population throws an error when the comparison population has a different number of aggregation levels", {
  
  # fewer levels than pop_test1
  pop_in <- data.frame( area=c("a","b","c"), stringsAsFactors = FALSE)
  expect_error(evaluate_promise(  # only using evaluate_promise to suppress warnings
    validate_population(pop_test1, col_aggregation = "area", col_data = NA, comparison_pop = pop_in)))
  
  # more levels than pop_test1
  pop_in <- data.frame( area=c("a","b","c","d","e"), stringsAsFactors = FALSE)
  expect_error(evaluate_promise(
    validate_population(pop_test1, col_aggregation = "area", col_data = NA, comparison_pop = pop_in)))
})

test_that("validate_population can handle comparison populations with the same information in different formats", {
  pop_in <- mutate(pop, age=as.factor(age), area=as.factor(area))
  
  expect_invisible(
    validate_population(pop, col_aggregation = c("area","age"), col_data = "popn", comparison_pop = pop_in))
  
  expect_invisible(
    validate_population(pop_in, col_aggregation = c("area","age"), col_data = "popn", comparison_pop = pop))
  
  pop_in <- as_tibble(pop_in)
  
  expect_invisible(
    validate_population(pop, col_aggregation = c("area","age"), col_data = "popn", comparison_pop = pop_in))
  
  expect_invisible(
    validate_population(pop_in, col_aggregation = c("area","age"), col_data = "popn", comparison_pop = pop))
  
  pop_in <- pop[ sample(1:nrow(pop), nrow(pop), replace=FALSE), ]
  
  expect_invisible(
    validate_population(pop, col_aggregation = c("area","age"), col_data = "popn", comparison_pop = pop_in))
  
  pop_in <- pop[, sample(1:ncol(pop), ncol(pop), replace=FALSE)]
  
  expect_invisible(
    validate_population(pop, col_aggregation = c("area","age"), col_data = "popn", comparison_pop = pop_in))
  
  pop_in <- group_by(pop, area, age)
  
  expect_invisible(
    validate_population(pop, col_aggregation = c("area","age"), col_data = "popn", comparison_pop = pop_in))
  
  expect_invisible(
    validate_population(pop_in, col_aggregation = c("area","age"), col_data = "popn", comparison_pop = pop))
})

test_that("validate_population can handle comparison populations with the same information in different formats", {
  
  #validate_population can handle comparison populations with different column names to the input"
  pop_in <- rename(pop, new_age=age)
  expect_invisible(
    validate_population(pop, col_aggregation = c("area","age"), col_data = "popn",
                        comparison_pop = pop, col_comparison = c("age","area")))
  expect_invisible(
    validate_population(pop, col_aggregation = c("area","age"), col_data = "popn",
                        comparison_pop = pop_in, col_comparison = c("age"="new_age","area")))
  
  
  #validate_population allows comparison columns to be a subset of aggregation columns"
  expect_silent(validate_population(mort_test4, col_aggregation = c("area", "age", "year"),
                                    col_data = "mort",
                                    comparison_pop = pop_test3, col_comparison = c("area", "age")))
  
  #validate_population allows comparison columns to be a subset of aggregation columns"
  expect_silent(validate_population(mort_test4, col_aggregation = c("area", "age", "year"),
                                    col_data = "mort",
                                    comparison_pop = pop_test3, col_comparison = c("area", "age")))
  
  #validate_population allows matching in col_comparison parameter

  comp_pop <- rename(pop_test3, zone = area, Age = age)
  expect_silent(validate_population(mort_test4, col_aggregation = c("area", "age"),
                                    col_data = "mort",
                                    comparison_pop = comp_pop, col_comparison = c("area" = "zone", "age" = "Age")))
  
  #validate_population allows matching in col_comparison parameter AND col_comparison to be a subset of aggregation cols
  expect_silent(validate_population(mort_test4, col_aggregation = c("area", "age", "year"),
                      col_data = "mort",
                      comparison_pop = comp_pop, col_comparison = c("area" = "zone", "age" = "Age")))
  
  #validate_population comparison fails when comparison columns don't match and are a subset of aggregation columns
  expect_error(validate_population(filter(mort_test4, age != 3), col_aggregation = c("area", "age", "year"),
                                   col_data = "mort",
                                   comparison_pop = pop_test3, col_comparison = c("area", "age")))
  
  #passes when the missing aggregation levels aren't in columns being checked on
  expect_silent(validate_population(filter(mort_test4, year != 2001), col_aggregation = c("area", "age", "year"),
                                    col_data = "mort",
                                    comparison_pop = pop_test3, col_comparison = c("area", "age")))
})

test_that("validate_population can handle difficult column names", {
  pop_in <- rename(pop_test1, " ":="area")
  expect_invisible(
    validate_population(pop_in, col_aggregation = " ", col_data = NA))
  
  pop_in <- rename(pop_test1, "name with spaces":="area")
  expect_invisible(
    validate_population(pop_in, col_aggregation = "name with spaces", col_data = NA))
})
