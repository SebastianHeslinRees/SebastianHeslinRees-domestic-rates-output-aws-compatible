context("null_births")
library(popmodules)
library(testthat)

pop <- data.frame(area = c("a","b","c"), popn = 100, stringsAsFactors = FALSE)
pop2 <- expand.grid( age=20:23, area=c("a","b","c"), sex=c("f","m"), popn = 100, stringsAsFactors = FALSE)

births <- data.frame(area = c("a","b","c"), age = 0, births = 0, stringsAsFactors = FALSE)
births2 <- expand.grid( area=c("a","b","c"), sex=c("f","m"), age = 0, births = 0, stringsAsFactors = FALSE) %>% 
  arrange(area,sex,age) %>% data.frame()


#--------------------------------------------------------------

test_that("null_births creates the expected output", {
  expect_equal(null_births(pop, col_aggregation = "area"), births)
  expect_equal(null_births(pop2, col_aggregation = c("area","sex","age")), births2)
})

test_that("null_births doesn't care about the order of input aggregation columns and warns for duplicates", {
  expect_equal(null_births(pop2, col_aggregation = c("sex","age","area")), births2)

  expect_warning(temp <- null_births(pop2, col_aggregation = c("sex", "age", "area", "area")))
  expect_equal(temp, births2)
})

test_that("null_births handles factors, tibbles and groups", {
  pop_in <- dplyr::mutate(pop, area=as.factor(area))
  births_out <- dplyr::mutate(births, area=as.factor(area)) %>% data.frame()
  expect_equal(null_births(pop_in, col_aggregation = "area"), births_out)

  pop_in <-  dplyr::as_tibble(pop_in)
  expect_equal(null_births(pop_in, col_aggregation = "area"), births_out)

  pop_in <-  dplyr::group_by(pop_in, area)
  births_out <- dplyr::group_by(births_out, area, age) %>% summarise(births = sum(births)) %>% data.frame()
  expect_equal(null_births(pop_in, col_aggregation = "area"), births_out)
})

test_that("null_births warns when factor levels don't match the input", {
  skip_if(!requireNamespace("validatepop", quietly=TRUE), message = "validatepop package is not installed")
  pop_in  <- dplyr::mutate(pop, area=factor(area, levels = c("a","b","c","d")))
  births_out <- dplyr::mutate(births, area=factor(area, levels = c("a","b","c","d")))
  expect_warning( temp <- null_births(pop_in, col_aggregation = "area") )
  expect_equal(temp, births_out)
})

test_that("null_births warns with an empty input", {
  pop_in <- pop[NULL,]
  births_out <- births[NULL,]
  expect_warning( temp <- null_births(pop_in, col_aggregation = "area") )
  expect_equal(temp, births_out)
})

test_that("null_births throws an error with missing aggregation values", {
  pop_in <- pop2
  pop_in$area[1] <- NA
  expect_error(null_births(pop_in, col_aggregation = "area"))
})

test_that("null_births deals with non-standard column names", {
  pop_in  <- dplyr::rename(pop2,    xage = age, xarea = area, xsex = sex)
  pop_out <- dplyr::rename(births2, xage = age, xarea = area, xsex = sex)
  expect_equal(null_births(pop_in, col_aggregation = c("xarea", "xage", "xsex"), col_age = "xage"), pop_out)
})

test_that("null_births warns when the input already has a births column and throws an error when it's an aggregation level", {
  pop_in <- dplyr::mutate(pop, births = 20)
  expect_warning( temp <- null_births(pop_in, col_aggregation = "area") )
  expect_equal(temp, births)
  expect_error(null_births(pop_in, col_aggregation = c("area","births")) )
})


# These tests are specific to null_births
test_that("null_births can give custom birth numbers per geography", {
  births_out <- dplyr::mutate(births, births = 1000)
  expect_equal(null_births(pop, col_aggregation = "area", const = 0), births)
  expect_equal(null_births(pop, col_aggregation = "area", const = 1000), births_out)
})

test_that("null_births hates negative birth rates", {
  expect_error(null_births(pop, col_aggregation = "area", const = -1))
})


# Other tests in more complex functions might include
# - trying out different fertility rates (zero, negative...)
# - checking the function can hand the presence (or lack of) a 'sex' column
