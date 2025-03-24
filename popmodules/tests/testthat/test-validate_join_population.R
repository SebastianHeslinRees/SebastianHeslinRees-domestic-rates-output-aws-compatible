
context("validate_join_population")
library(testthat)
library(popmodules)

# Set up valid population data frame with default column names
pop_test1 <- data.frame( gss_code=c("a","b","c","d"), stringsAsFactors = FALSE)
pop_test1_partial <- dplyr::filter(pop_test1, gss_code!="a")

pop_test2 <- expand.grid( gss_code=c("a","b","c","d"), age = 0:3, stringsAsFactors = FALSE)

pop_test2_partial <- dplyr::filter(pop_test2, gss_code!="a")

pop_test3 <- dplyr::mutate(pop_test2, popn = 10000)

pop_full <- expand.grid( year = 2000:2001, gss_code=c("a","b","c","d"), age = 0:3, sex=c("male","female"),
                         popn=1000, popn2="fill", stringsAsFactors = FALSE)




# Run tests
test_that("validate_join_population validates good joins" , {
  expect_invisible(
    validate_join_population(pop_test1, pop_test1, cols_common_aggregation="gss_code"))

  expect_invisible(
    validate_join_population(pop_test2, pop_test1, cols_common_aggregation="gss_code"))

  expect_invisible(
    validate_join_population(pop_test2, pop_test2, cols_common_aggregation=c("gss_code","age")))

  expect_invisible(
    validate_join_population(pop_full, pop_test1, cols_common_aggregation="gss_code"))
})

test_that("validate_join_population can spot default column names" , {
  pop_in <- dplyr::rename(pop_full, x1=popn, x2=popn2)
  expect_invisible(
    validate_join_population(pop_in, pop_full))
})

test_that("validate_join_population fails when pop1 isn't a subset", {
  expect_error(
    validate_join_population(pop_test1, pop_test1_partial, cols_common_aggregation="gss_code"))
})

test_that("validate_join_population can require complete coverage" , {
  expect_invisible(
    validate_join_population(pop_test1_partial, pop_full, cols_common_aggregation="gss_code", aggregation_levels_match=TRUE))

  expect_error(
    validate_join_population(pop_test1_partial, pop_full, cols_common_aggregation="gss_code", aggregation_levels_match=FALSE))
})

test_that("validate_join_population can require many-to-one and one-to-many mapping" , {
  expect_invisible(
    validate_join_population(pop_test2_partial, pop_test1, cols_common_aggregation="gss_code", many2one = TRUE))

  expect_error(
    validate_join_population(pop_test2_partial, pop_test1, cols_common_aggregation="gss_code", many2one = FALSE))

  expect_invisible(
    validate_join_population(pop_test1_partial, pop_test2, cols_common_aggregation="gss_code", one2many = TRUE))

  expect_error(
    validate_join_population(pop_test1_partial, pop_test2, cols_common_aggregation="gss_code", one2many = FALSE))

  expect_invisible(
    validate_join_population(pop_test2_partial, pop_test2, cols_common_aggregation=c("gss_code","age"), one2many = TRUE, many2one=TRUE))
})


test_that("validate_join_population handles empty data frames", {
  expect_warning(
    temp <- validate_join_population(pop_test2[NULL,], pop_test1, cols_common_aggregation="gss_code"))
  expect_equal(pop_test2[NULL,], temp)

})

test_that("validate_join_population can map column names to each other", {
  pop_in <- dplyr::rename(pop_test2, new_age=age)
  expect_invisible(
    validate_join_population(pop_in, pop_test2, cols_common_aggregation=c("gss_code", "new_age"="age")))
})


test_that("validate_join_population handles (potentially differing) factors and tibbles", {
  pop_in <- dplyr::mutate(pop_test2, age=as.factor(age), gss_code=as.factor(gss_code))
  expect_invisible(
    validate_join_population(pop_in, pop_in, cols_common_aggregation = c("gss_code","age")))
  expect_invisible(
    validate_join_population(pop_in, pop_test2, cols_common_aggregation = c("gss_code","age")))
  expect_invisible(
    validate_join_population(pop_test2, pop_in, cols_common_aggregation = c("gss_code","age")))

  pop_in <- dplyr::as_tibble(pop_in)
  expect_invisible(
    validate_join_population(pop_in, pop_in, cols_common_aggregation = c("gss_code","age")))
  expect_invisible(
    validate_join_population(pop_in, pop_test2, cols_common_aggregation = c("gss_code","age")))
  expect_invisible(
    validate_join_population(pop_test2, pop_in, cols_common_aggregation = c("gss_code","age")))

  pop_in <- dplyr::group_by(pop_in, gss_code, age)
  expect_invisible(
    validate_join_population(pop_in, pop_in, cols_common_aggregation = c("gss_code","age")))
  expect_invisible(
    validate_join_population(pop_in, pop_test2, cols_common_aggregation = c("gss_code","age")))
  expect_invisible(
    validate_join_population(pop_test2, pop_in, cols_common_aggregation = c("gss_code","age")))
})


test_that("validate_join_population handles unused factor levels, with appropriate warnings", {
  pop_in <- data.frame(gss_code = factor(c("a","b","c","d"), levels=c("a","b","c","d","e")))
  expect_warning(
    validate_join_population(pop_in, pop_test1, cols_common_aggregation="gss_code"))
  expect_invisible(
    validate_join_population(pop_test1, pop_in, cols_common_aggregation="gss_code"))
})


test_that("validate_join_population throws an error when it can't find common column names", {
  expect_error(
    validate_join_population(pop_test2, pop_test2))

  expect_error(
    validate_join_population(pop_test2, pop_test2, cols_common_aggregation=c("gss_code","ABSENT")))
})

test_that("validate_join_population warns when there are common columns it's *not* joining on", {
  expect_warning(
    validate_join_population(pop_test2, pop_test2, cols_common_aggregation="gss_code"))

  pop_in <- dplyr::rename(pop_test2, "xgss_code" = "gss_code")
  pop_in$gss_code <- "fill"
  expect_warning(
    validate_join_population(pop_test2, pop_in, cols_common_aggregation = c("gss_code"="xgss_code", "age"))
  )
})
