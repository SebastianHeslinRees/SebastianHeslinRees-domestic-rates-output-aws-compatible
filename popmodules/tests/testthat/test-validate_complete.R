library(testthat)

x <- expand.grid(gss_code_ward = letters[15:23],
                 year = 2000:2020,
                 sex = "female", "male",
                 age = 0:90,
                 stringsAsFactors = FALSE) %>% 
  left_join(data.frame(gss_code = c(rep("a",3), rep("b",3), rep("c",3)),
                       gss_code_ward = letters[15:23]),
            by="gss_code_ward")


test_that("returns invisible", {
  expect_silent(validate_complete(x,
                                  test_cols = c("year","gss_code","gss_code_ward","sex","age"),
                                  nested_cols = c("gss_code", "gss_code_ward"),
                                  warn_only = FALSE))
  
  expect_silent(validate_complete(x,
                                  test_cols = c("year","gss_code_ward","sex","age"),
                                  nested_cols = c("gss_code", "gss_code_ward"),
                                  warn_only = FALSE))
  
  expect_silent(validate_complete(x,
                                  test_cols = c("year","sex","age"),
                                  nested_cols = c("gss_code", "gss_code_ward"),
                                  warn_only = FALSE))
})

test_that("returns error", {
  expect_error(validate_complete(x,
                                 test_cols = c("year","gss_code","gss_code_ward","sex","age"),
                                 nested_cols = c("gss_code"),
                                 warn_only = FALSE))
})


test_that("returns warning", {
  expect_warning(validate_complete(x,
                                   test_cols = c("year","gss_code","gss_code_ward","sex","age"),
                                   nested_cols = c("gss_code"),
                                   warn_only = TRUE))
})
