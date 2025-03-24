library(dplyr)
library(testthat)

df <- expand.grid(gss_code = c("A","B","C"),
                  year = 2001:2020,
                  sex = c("female","male"),
                  age = 0:90) %>% 
  mutate(popn = runif(10920, 100, 150)) %>% 
  data.frame()

out_1 <- df %>%
  filter(year %in% 2016:2020) %>%
  group_by(gss_code, sex) %>%
  summarise(popn = sum(popn)/5,
            .groups = 'drop_last') %>%
  data.frame() %>%
  mutate(year = 2021) %>% 
  select(year, gss_code, sex, popn) 

test_that("calculate_mean_from_backseries fails", {
  
  #basic operation
  expect_equal(calculate_mean_from_backseries(data_backseries = df,
                                              n_years_to_avg = 5,
                                              last_data_year = 2020,
                                              data_col = "popn",
                                              col_aggregation = c("gss_code","sex"),
                                              project_rate_from = 2021),
               out_1)
})

#trigger errors
test_that("calculate_mean_from_backseries validation fails", {

  expect_error(calculate_mean_from_backseries(data_backseries = "not a dataframe",
                                 n_years_to_avg = 5,
                                 last_data_year = 2020,
                                 data_col = "popn",
                                 col_aggregation = c("gss_code","sex"),
                                 project_rate_from = 2021))
  
  expect_error(calculate_mean_from_backseries(data_backseries = df,
                                              n_years_to_avg = "not a number",
                                              last_data_year = 2020,
                                              data_col = "popn",
                                              col_aggregation = c("gss_code","sex"),
                                              project_rate_from = 2021))

  expect_error(calculate_mean_from_backseries(data_backseries = df,
                                              n_years_to_avg = 5,
                                              last_data_year = "not a number",
                                              data_col = "popn",
                                              col_aggregation = c("gss_code","sex"),
                                              project_rate_from = 2021))
  
  expect_error(calculate_mean_from_backseries(data_backseries = df,
                                              n_years_to_avg = 5,
                                              last_data_year = 2020,
                                              data_col = "not a column",
                                              col_aggregation = c("gss_code","sex"),
                                              project_rate_from = 2021))
  
  
  expect_error(calculate_mean_from_backseries(data_backseries = df,
                                              n_years_to_avg = 5,
                                              last_data_year = 2030,
                                              data_col = "popn",
                                              col_aggregation = c("gss_code","sex"),
                                              project_rate_from = 2021))
  
})