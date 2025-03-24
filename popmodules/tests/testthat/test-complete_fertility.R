context("complete_fertility")
library(testthat)
library(popmodules)
library(dplyr)

set.seed(101)

# complete_fertility <- function(fertility, population,
#                                col_sex_fert = "sex",
#                                col_sex_popn = col_sex_fert,
#                                col_age_fert = "age",
#                                col_age_popn = col_age_fert,
#                                col_rate = "rate")

popn <- expand.grid(year = c(2017, 2018),
                    gss_code=c("E09000001","E09000002"),
                    sex=c("female","male"),
                    age=20:30, stringsAsFactors = FALSE)

popn$count <- sample(1:nrow(popn))

fert <- expand.grid(year = c(2017, 2018),
                    gss_code=c("E09000001","E09000002"),
                    sex=c("female"),
                    age=22:27, stringsAsFactors = FALSE)

fert$rate <- sample((1:nrow(fert))/(2*nrow(fert)))

fert_complete <- expand.grid(year = c(2017, 2018),
                             gss_code = c("E09000001", "E09000002"),
                             sex = c("male", "female"),
                             age = 20:30, stringsAsFactors = FALSE) %>%
  left_join(fert, by = c("year", "gss_code", "age", "sex")) %>%
  mutate(rate = ifelse(is.na(rate), 0, rate))


pop_null_sex <- popn %>% filter(sex == "female") %>% select(-sex)
pop_null_age <- popn %>% filter(age == 25) %>% select(-age)
pop_null_age_sex <- popn %>% filter(sex == "female", age == 25) %>% select(-age, -sex)
fert_null_sex <- fert %>% select(-sex)
fert_null_age <- fert %>% filter(age == 25) %>% select(-age)
fert_null_age_sex <- fert %>% filter(sex == "female", age == 25) %>% select(-age, -sex)
pop_diff_age <- popn %>% rename(aage = age)
pop_diff_sex <- popn %>% rename(ssex = sex)
fert_diff_sex_encoding <- mutate(fert, sex = "f")
fert_no_rate <- fert %>% select(-rate)

# Test some expected results
## standard
test_that("complete_fertility produces expected result", {
  expect_equivalent(arrange(complete_fertility(fert, popn), age, desc(sex), gss_code, year),
                    fert_complete)

})

## different column names
test_that("complete_fertility produces expected result with different age column names", {
  expect_equivalent(arrange(complete_fertility(fert, pop_diff_age, col_age_popn = "aage"), age, desc(sex), gss_code, year),
                    fert_complete)

})
test_that("complete_fertility produces expected result with different sex column names", {
  expect_equivalent(arrange(complete_fertility(fert, pop_diff_sex, col_sex_popn = "ssex"), age, desc(sex), gss_code, year),
                    fert_complete)

})

## no age and sex columns
test_that("complete_fertility produces expected result with no age or sex columns", {
  expect_equivalent(complete_fertility(fert_null_age_sex, pop_null_age_sex, col_sex_fert = NULL, col_age_fert = NULL),
                    fert_null_age_sex)
})

# Error when fertility doesn't encode sex the same as population
test_that("complete_fertility checks sex is encoded the same in fertility and population", {
  expect_error(complete_fertility(fert_diff_sex_encoding, popn), "fertility and population dataframes appear to encode sex differently")
})

# Errors if population or fertility don't exist
test_that("error if population doesn't exist", {
  expect_error(complete_fertility(fert, poppp), "object 'poppp' not found")
})

test_that("error if fertility doesn't exist", {
  expect_error(complete_fertility(fertdfa, popn), "object 'fertdfa' not found")
})

# Errors if population or fertility are not dataframes
test_that("error if population isn't dataframe", {
  expect_error(complete_fertility(fert, "hi"), "population must be a dataframe")
})

test_that("error if fertility isn't dataframe", {
  expect_error(complete_fertility("hi", popn), "fertility must be a dataframe")
})

# Errors if sex and age aren't either both null or both present
test_that("error if age is missing from population but not fertility", {
  expect_error(complete_fertility(fert, pop_null_age, col_age_popn = NULL), "col_age must either be NULL in both population and fertility or NULL in neither")
})
test_that("error if age is missing from fertility but not population", {
  expect_error(complete_fertility(fert_null_age, popn, col_age_fert = NULL, col_age_popn = "age"), "col_age must either be NULL in both population and fertility or NULL in neither")
})
test_that("error if sex is missing from population but not fertility", {
  expect_error(complete_fertility(fert, pop_null_sex, col_sex_popn = NULL), "col_sex must either be NULL in both population and fertility or NULL in neither")
})
test_that("error if sex is missing from fertility but not population", {
  expect_error(complete_fertility(fert_null_sex, popn, col_sex_fert = NULL, col_sex_popn = "sex"), "col_sex must either be NULL in both population and fertility or NULL in neither")
})

# Errors if specified columns don't exist
test_that("error if specified fertility age column is missing", {
  expect_error(complete_fertility(fert_null_age, popn), "fertility dataframe does not contain 'age' column")
})
test_that("error if specified fertility sex column is missing", {
  expect_error(complete_fertility(fert_null_sex, popn), "fertility dataframe does not contain 'sex' column")
})
test_that("error if specified population age column is missing", {
  expect_error(complete_fertility(fert, pop_null_age), "population dataframe does not contain 'age' column")
})
test_that("error if specified population sex column is missing", {
  expect_error(complete_fertility(fert, pop_null_sex), "population dataframe does not contain 'sex' column")
})
test_that("error if specified rate column is missing from fertility", {
  expect_error(complete_fertility(fert_no_rate, popn), "fertility dataframe does not contain 'rate' column")
})








