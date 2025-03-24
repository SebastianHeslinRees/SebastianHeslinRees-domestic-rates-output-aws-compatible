context("project_forward_flat")
library(popmodules)
library(testthat)
library(dplyr)

df_w_yr <- data.frame(year = c(2001, 2002),
                      gss_code = c("E0901", "E0902"),
                      age = c(2,3),
                      sex = c("male", "female"))

df_w_yr <- as.data.frame(tidyr::complete(df_w_yr, year, gss_code, age, sex))
df_w_yr$rate <- (1:nrow(df_w_yr))/(5 * nrow(df_w_yr))

df_no_yr <- df_w_yr[, !names(df_w_yr) %in% "year"]

proj_df <- df_w_yr %>% filter(year == 2002) %>%
  tidyr::expand(year = c(2002, 2002, 2003, 2004), tidyr::nesting(gss_code, age, sex, rate)) %>%
  rbind(filter(.data = df_w_yr, year == 2001), .) %>%
  as.data.frame()

last_proj_yr <- 2004


#THIS IS TESTED IN THE INTERNATIONAL_RATES_AND_FLOWS TEST

# project_forward_flat <- function(df, last_proj_yr)

test_that("project_forward_flat produces the expected output", {
   expect_equivalent(project_forward_flat(df_w_yr, last_proj_yr), proj_df)
})

