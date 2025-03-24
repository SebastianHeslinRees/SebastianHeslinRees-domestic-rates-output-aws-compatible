library(popmodules)
library(testthat)
library(dplyr)

rates_traj <- expand.grid(sex = c("female","male"),
                          age = 21:22,
                          year = 2010:2020,
                          variant = "2018_principal",
                          stringsAsFactors = FALSE) %>%
  arrange(sex, age, variant) %>%
  mutate(change = 0.1 * (year - 2009))


jump_off <- expand.grid(year = 2012,
                        gss_code = c("E01","E02"),
                        sex = c("female","male"),
                        age = 21:22,
                        test_rate = 0.1,
                        stringsAsFactors = FALSE)

expected_output <- filter(rates_traj, year > 2012) %>%
  arrange(year) %>%
  group_by(sex, age) %>%
  mutate(change = 1+change,
         cumprod = cumprod(change)) %>%
  ungroup() %>%
  left_join(select(jump_off, -year), by=c("sex","age")) %>%
  mutate(test_rate = test_rate * cumprod) %>%
  select(names(jump_off)) %>%
  rbind(jump_off) %>%
  arrange(year, gss_code, sex, age) %>%
  data.frame()


test_that("project_rates_npp produces the expected output",
          expect_equal(project_rates_npp(jump_off, rate_col="test_rate", rates_traj,
                                         first_proj_yr=2012, n_proj_yr=9,
                                         npp_var="2018_principal"),
                       expected_output))


#Validation checks

test_that("project_rates npp fails to throw expected errors", {
  
  expect_error(project_rates_npp("i am the walrus", rate_col="test_rate", rates_traj,
                                 first_proj_yr=2012, n_proj_yr=9,
                                 npp_var="2018_principal"))
  
  expect_error(project_rates_npp(jump_off, rate_col=7, rates_traj,
                                 first_proj_yr=2012, n_proj_yr=9,
                                 npp_var="2018_principal"))
  
  expect_error(project_rates_npp(jump_off, rate_col="test_rate", rates_traj,
                                 first_proj_yr="mr banks", n_proj_yr=9,
                                 npp_var="2018_principal"))
  
  expect_error(project_rates_npp(jump_off, rate_col="test_rate", rates_traj,
                                 first_proj_yr=2012, n_proj_yr="mary poppins",
                                 npp_var="2018_principal"))
  
  expect_error(project_rates_npp(jump_off, rate_col="test_rate", rates_traj,
                                 first_proj_yr=2012, n_proj_yr=9,
                                 npp_var=data.frame(a = 3, b = 1)))
  
  #check required columns are present
  
  expect_error(project_rates_npp(jump_off, rate_col="test_rate",
                                 rates_trajectory = rename(rates_traj, wrong = year),
                                 first_proj_yr=2012, n_proj_yr=9,
                                 npp_var="2018_principal"))
  
  expect_error(project_rates_npp(jump_off, rate_col="test_rate",
                                 rates_trajectory = rename(rates_traj, wrong = sex),
                                 first_proj_yr=2012, n_proj_yr=9,
                                 npp_var="2018_principal"))
  
  expect_error(project_rates_npp(jump_off, rate_col="test_rate",
                                 rates_trajectory = rename(rates_traj, wrong = age),
                                 first_proj_yr=2012, n_proj_yr=9,
                                 npp_var="2018_principal"))
  
  expect_error(project_rates_npp(rename(jump_off, wrong = sex),
                                 rate_col="test_rate", rates_traj,
                                 first_proj_yr=2012, n_proj_yr=9,
                                 npp_var="2018_principal"))
  
  expect_error(project_rates_npp(rename(jump_off, wrong = age),
                                 rate_col="test_rate", rates_traj,
                                 first_proj_yr=2012, n_proj_yr=9,
                                 npp_var="2018_principal"))
  
})





