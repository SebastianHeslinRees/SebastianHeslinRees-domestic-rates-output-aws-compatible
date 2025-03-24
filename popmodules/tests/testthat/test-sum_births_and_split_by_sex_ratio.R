library(popmodules)
library(testthat)

popn1 <- expand.grid(year = 2000, gss_code = c("a","b"), sex = c("female","male"), age = 20:21, stringsAsFactors=FALSE)
popn1$births <- ifelse(popn1$sex == "female", 205, 0)

popn2 <- expand.grid(year = 2000, gss_code = c("a","b"), age = 20:21, stringsAsFactors=FALSE)
popn2$births <- 205

output <- expand.grid(year = 2000, gss_code = c("a","b"), sex = c("female","male"), age = 0, stringsAsFactors=FALSE)
output <- mutate(output, births = ifelse(output$sex == "female", 200, 210)) %>%
  dplyr::arrange(year, gss_code, sex)

test_that("sum_births_and_split_by_sex_ratio produces the expected output", {
  expect_equivalent( sum_births_and_split_by_sex_ratio(popn1, 1.05), output)
  expect_equivalent( sum_births_and_split_by_sex_ratio(popn2, 1.05), output)
})

