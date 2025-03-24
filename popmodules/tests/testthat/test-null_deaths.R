context("null_deaths")
library(popmodules)
library(testthat)

pop <- data.frame( area=c("a","b","c"), popn = 100, stringsAsFactors = FALSE)
pop2 <- expand.grid( age=20:23, area=c("a","b","c"), sex=c("f","m"), popn = 100, stringsAsFactors = FALSE)

deaths  <- data.frame( area=c("a","b", "c"),  deaths = 0, stringsAsFactors = FALSE)
deaths2 <- expand.grid( age=20:23, area=c("a","b","c"), sex=c("f","m"), deaths = 0, stringsAsFactors = FALSE)



#--------------------------------------------------------------
# TODO find out whether data frame attributes matter, and whether it matters that the function changes them

test_that("null_deaths creates the expected output", {
  expect_equal(null_deaths(pop, col_aggregation = "area", col_popn = "popn"), deaths, check.attributes=FALSE)
  expect_equal(null_deaths(pop2, col_aggregation = c("age","area","sex"), col_popn = "popn"), deaths2, check.attributes=FALSE)
})

test_that("null_deaths doesn't care about the order of aggregation columns and warns if there are duplicates", {
  expect_equal(null_deaths(pop2, col_aggregation = c("area","age","sex"), col_popn = "popn"), deaths2, check.attributes=FALSE)

  expect_warning(temp <- null_deaths(pop2, col_aggregation = c("area","age","sex","sex"), col_popn = "popn"))
  expect_equal(temp, deaths2, check.attributes=FALSE)
})

test_that("null_deaths handles additional, unused input columns", {
  pop_in <- dplyr::mutate(pop, filler = "fill")  # fillers gonna fill
  expect_equal(null_deaths(pop_in, col_aggregation = "area", col_popn = "popn"), deaths, check.attributes=FALSE)
})

test_that("null_deaths handles factors, tibbles and groups", {
  pop_in <- dplyr::mutate(pop, area=as.factor(area))
  deaths_out <- dplyr::mutate(deaths, area=as.factor(area))
  expect_equal(null_deaths(pop_in, col_aggregation = "area", col_popn = "popn"), deaths_out, check.attributes=FALSE)

  pop_in <-  dplyr::as_tibble(pop_in)
  deaths_out <- dplyr::as_tibble(deaths_out)
  expect_equal(null_deaths(pop_in, col_aggregation = "area", col_popn = "popn"), deaths_out, check.attributes=FALSE)

  pop_in <-  dplyr::group_by(pop_in, area)
  deaths_out <- dplyr::group_by(deaths_out, area)
  expect_equal(null_deaths(pop_in, col_aggregation = "area", col_popn = "popn"), deaths_out, check.attributes=FALSE)
})

test_that("null_deaths warns when factor levels don't match the input", {
  pop_in  <- dplyr::mutate(pop, area=factor(area, levels = c("a","b","c","d")))
  deaths_out <- dplyr::mutate(deaths, area=factor(area, levels = c("a","b","c","d")))
  expect_warning( temp <- null_deaths(pop_in, col_aggregation = "area", col_popn = "popn") )
  expect_equal(temp, deaths_out, check.attributes=FALSE)
})

test_that("null_deaths warns with an empty input", {
  pop_in <- pop[NULL,]
  deaths_out <- deaths[NULL,]
  expect_warning( temp <- null_deaths(pop_in, col_aggregation = "area", col_popn = "popn") )
  expect_equal(temp, deaths_out, check.attributes=FALSE)
})

test_that("null_deaths deals with non-standard column names", {
  pop_in     <- dplyr::rename(pop2,    xage = age, xarea = area, xsex = sex)
  deaths_out <- dplyr::rename(deaths2, xage = age, xarea = area, xsex = sex)
  # TODO find out whether data frame attributes matter and whether it matters that they're different here
  expect_equal(null_deaths(pop_in, col_aggregation = c("xage", "xarea", "xsex"), col_popn = "popn"), deaths_out, check.attributes = FALSE)
})

test_that("null_deaths warns when the input already has a deaths column and throws an error when it's an aggregation level", {
  pop_in <- dplyr::mutate(pop, deaths = 20)
  expect_warning( temp <- null_deaths(pop_in, col_aggregation = "area", col_popn = "popn") )
  expect_equal(temp, deaths, check.attributes=FALSE)
  expect_error(null_deaths(pop_in, col_aggregation = c("area","deaths"), col_popn = "popn") )
})

test_that("null_deaths throws an error with missing aggregation values", {
  pop_in <- pop
  pop_in$area[1] <- NA
  expect_error(null_deaths(pop_in, col_aggregation = "area", col_popn = "popn"))
})

test_that("null_deaths throws an error with duplicate aggregation values", {
  expect_error(null_deaths(pop2, col_aggregation = c("area","sex"), col_popn = "popn"))
})


test_that("null_deaths throws an error or (requested) warning when the result would create a negative population", {
  expect_error(null_deaths(pop, col_aggregation = "area", col_popn = "popn", const = 1000))

  deaths_out <- dplyr::mutate(deaths, deaths=100)
  expect_warning( temp <- null_deaths(pop, col_aggregation = "area",
                                      col_popn = "popn", const = 1000,
                                      error_negative_pop = FALSE))
  expect_equal(temp, deaths_out, check.attributes=FALSE)
})



# These tests are specific to null_deaths
test_that("null_deaths can give custom numbers per aggregation level", {
  deaths_out <- dplyr::mutate(deaths, deaths = 100)
  expect_equal(null_deaths(pop, col_aggregation = "area", col_popn = "popn", const = 0), deaths)
  expect_equal(null_deaths(pop, col_aggregation = "area", col_popn = "popn", const = 100), deaths_out)
})

test_that("null_deaths hates negative death rates", {
  expect_error(null_deaths(pop, col_aggregation = "area", col_popn = "popn", const = -1))
})

