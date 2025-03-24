library(popmodules)
library(dplyr)
library(testthat)

data_list <- list('2020' = list(path = "test_data/test-get_rates_or_flows_a.rds",
                                transition = F),
                  '2021' = list(path = "test_data/test-get_rates_or_flows_b.rds",
                                transition = T),
                  '2030' = list(path = "test_data/test-get_rates_or_flows_c.rds",
                                transition = F))


df_info <- get_rates_flows_info(data_list, 2020, 2050)

x <- NULL
y <- list()

projection_year <- 2020

for(projection_year in 2020:2050){
  
  x <- get_rates_or_flows(x, df_info, projection_year, 2020,
                          col_aggregation=c("gss_code","sex","age"), 
                          data_col="int_in",
                          geog_code_col = NULL)
  
  y[[projection_year]] <- x %>%
    mutate(year = projection_year) %>%
    select(year, gss_code, sex, age, int_in)
}

y <- data.table::rbindlist(y) %>%
  as.data.frame()

expect <- expand_grid(year = 2020:2050,
                      gss_code = c("E06000001","E06000002"),
                      sex = c("female","male"),
                      age = 10:20) %>%
  mutate(int_in = case_when(year == 2020 ~ 1,
                            year %in% 2021:2030 ~ (year-2020)*10,
                            year > 2030 ~ 100)) %>%
  data.frame()

testthat::test_that("get_rates_or_flows unexpected output",
                    expect_equivalent(y,expect))
