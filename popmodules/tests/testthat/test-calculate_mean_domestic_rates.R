library(testthat)
library(popmodules)
library(dplyr)

codes <- codes <- c("E09000001","E09000002","E09000003","E06000001","E06000002","W06000001","W06000002","S92000003","N92000002")

x <- expand.grid(gss_in = codes,
                 gss_out = codes,
                 age = 0:3, sex = c("male","female"), year = 2010:2019,
                 stringsAsFactors = FALSE) %>%
  mutate(rate = runif(6480,0,0.1)) %>%
  filter(gss_in != gss_out) %>% 
  data.frame()

y <- filter(x, gss_out == "E09000001", sex == "female", age == 2)

z <- setdiff(x,y)

a <- filter(y, year %in% 2015:2019)
a <- sum(a$rate)/5
a <- 1.5/a
b <- 1.5/0.8

y <- y %>% 
  mutate(rate = rate*a)

s <- rename(x, steve = rate)

expect_1 <- filter(x, year %in% 2015:2019) %>% 
  group_by(gss_out, gss_in, age, sex) %>% 
  summarise(rate = sum(rate)/5, .groups = 'drop_last') %>% 
  data.frame() %>% 
  arrange(gss_out, gss_in, sex, age)

expect_2 <- y %>% 
  mutate(rate = rate/b) %>%
  rbind(z) %>% 
  filter(year %in% 2015:2019) %>% 
  group_by(gss_out, gss_in, age, sex) %>% 
  summarise(rate = sum(rate)/5, .groups = 'drop_last') %>% 
  data.frame() %>% 
  arrange(gss_out, gss_in, sex, age)

expect_3 <- filter(z, year %in% 2015:2019) %>% 
  group_by(gss_out, gss_in, age, sex) %>% 
  summarise(rate = sum(rate)/5, .groups = 'drop_last') %>% 
  data.frame() %>% 
  arrange(gss_out, gss_in, sex, age)

expect_4 <- rename(expect_1, steve = rate)

test_that("calculate_mean_domestic_rates: basic operation", {
  #test function does what it should with no cap
  expect_equal(calculate_mean_domestic_rates(x,
                                              last_data_year = 2019,
                                              n_years_to_avg = 5,
                                              rate_cap = 100),
               expect_1)
})

test_that("calculate_mean_domestic_rates: cap warning", {
  #test >cap warning
  expect_warning(calculate_mean_domestic_rates(x,
                                                last_data_year = 2019,
                                                n_years_to_avg = 5,
                                                rate_cap = 0.0001))
})
  #test cap output
  expect_warning(expect_equal(calculate_mean_domestic_rates(rbind(y,z),
                                                             last_data_year = 2019,
                                                             n_years_to_avg = 5,
                                                             rate_cap = 0.8),
                              expect_2))

test_that("calculate_mean_domestic_rates: missing levels", {
  #test missing aggregation levels
  expect_warning(expect_equal(calculate_mean_domestic_rates(z,
                                                             last_data_year = 2019,
                                                             n_years_to_avg = 5,
                                                             rate_cap = 100),
                              expect_3))
  
})

test_that("calculate_mean_domestic_rates: non-standard column name", {
  expect_equal(calculate_mean_domestic_rates(s,
                                             last_data_year = 2019,
                                             n_years_to_avg = 5,
                                             col_rate = "steve",
                                             rate_cap = 100),
               expect_4)
})
