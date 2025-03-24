library(dplyr)
library(testthat)

df <- expand.grid(gss_code = c("A","B","C"),
                  year = 2001:2020,
                  sex = c("female","male"),
                  age = 0:90) %>% 
  mutate(popn = year-2000) %>% 
  data.frame()

out_1 <- expand.grid(gss_code = c("A","B","C"),
                     year = 2021,
                     sex = c("female","male")) %>% 
  mutate(popn = 21) %>% 
  data.frame() %>% 
  select(year, gss_code, sex, popn)

test_that("calculate_rate_by_regression fails", {
  
  #Basic operation
  expect_equal(calculate_rate_by_regression(data_backseries = df,
                                            n_years_regression = 5,
                                            last_data_year = 2020,
                                            data_col = 'popn',
                                            col_aggregation = c("gss_code","sex"),
                                            project_rate_from = 2021),
               out_1)
})


#trigger errors
test_that("calculate_rate_by_regression validation fails", {
  
  expect_error(calculate_rate_by_regression(data_backseries = "not a dataframe",
                                              n_years_regression = 5,
                                              last_data_year = 2020,
                                              data_col = "popn",
                                              col_aggregation = c("gss_code","sex"),
                                              project_rate_from = 2021))
  
  expect_error(calculate_rate_by_regression(data_backseries = df,
                                              n_years_regression = "not a number",
                                              last_data_year = 2020,
                                              data_col = "popn",
                                              col_aggregation = c("gss_code","sex"),
                                              project_rate_from = 2021))
  
  expect_error(calculate_rate_by_regression(data_backseries = df,
                                              n_years_regression = 5,
                                              last_data_year = "not a number",
                                              data_col = "popn",
                                              col_aggregation = c("gss_code","sex"),
                                              project_rate_from = 2021))
  
  expect_error(calculate_rate_by_regression(data_backseries = df,
                                              n_years_regression = 5,
                                              last_data_year = 2020,
                                              data_col = "not a column",
                                              col_aggregation = c("gss_code","sex"),
                                              project_rate_from = 2021))
  
  
  expect_error(calculate_rate_by_regression(data_backseries = df,
                                              n_years_regression = 5,
                                              last_data_year = 2030,
                                              data_col = "popn",
                                              col_aggregation = c("gss_code","sex"),
                                              project_rate_from = 2021))
  
})
