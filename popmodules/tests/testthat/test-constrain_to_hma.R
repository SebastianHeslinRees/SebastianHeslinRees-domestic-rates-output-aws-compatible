library(popmodules)
library(testthat)

hma_list <- list(a = c(1:2), b=c(3:5))

constraint <- tidyr::crossing(gss_code = 1:5, data.frame(sex=c("a","b"), births = 60))

popn <- tidyr::crossing(gss_code = 1:8, data.frame(sex=c("a","b"), births = c(1,3)))

test_output1 <- data.frame(gss_code = rep(1:8, each=2), sex = c("a","b"), births = c(rep(60,10), rep(c(1,3),3)))
test_output2 <- data.frame(gss_code = rep(1:8, each=2), sex = c("a","b"), births = c(rep(c(30,90), 5), rep(c(1,3),3)))


#------------------------------------------

test_that("constraint_to_hma creates the expected output", {
  testthat::expect_equal(
    constrain_to_hma(popn, constraint, hma_list,
                   col_aggregation = c("hma", "sex"),
                   col_popn = "births", col_constraint = "births"),
    test_output1)

  testthat::expect_equal(
    constrain_to_hma(popn, constraint, hma_list,
                   col_aggregation = c("hma"),
                   col_popn = "births", col_constraint = "births"),
    test_output2)
})
