library(popmodules)
library(testthat)

popn <- data.frame(gss_code = letters[1:5], popn = -2:2)
fixed <- data.frame(gss_code = letters[1:5], popn = c(0, 0, 0, 1, 2))
fixed_2 <- data.frame(gss_code = letters[1:5], popn = c(5, 5, 0, 1, 2))

test_that("check_negative_values", {
  expect_warning(out <- check_negative_values(popn, "popn", warn_only = TRUE))
  expect_equal(out, popn)

  expect_warning(out <- check_negative_values(popn, "popn", warn_only = FALSE))
  expect_equal(out, fixed)

  expect_equal(fixed, check_negative_values(fixed, "popn", warn_only = FALSE))
  
  expect_warning(out <- check_negative_values(popn, "popn", change_value = 5))
  expect_equal(out, fixed_2)
})
