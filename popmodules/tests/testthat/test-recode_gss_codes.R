context("recode_gss_codes")
library(popmodules)
library(testthat)
library(dplyr)

code_changes_path <- "test_data/district_changes_clean.rds"
code_changes <- readRDS(code_changes_path)

df_1 <- code_changes %>%
  select(gss_code = changed_from_code) %>%
  unique() %>%
  mutate(value = 5)

#
code_changes_2012 <- filter(code_changes, year == 2012) %>%
  select(changed_from_code, changed_to_code)

code_changes_2013 <- filter(code_changes, year == 2013) %>%
  select(changed_from_code, changed_to_code)

code_changes_2018 <- filter(code_changes, year == 2018) %>%
  select(changed_from_code, changed_to_code)

code_changes_2019 <- filter(code_changes, year == 2019) %>%
  select(changed_from_code, changed_to_code)

code_changes_2020 <- filter(code_changes, year == 2020) %>%
  select(changed_from_code, changed_to_code)

#
expect_2009 <- code_changes %>% 
  filter(!(split == T & merge == T)) %>% 
  mutate(gss_code = ifelse(year == 2009,
                           changed_to_code,
                           changed_from_code),
         value = 5) %>%
  group_by(gss_code) %>%
  summarise(value = sum(value), .groups = 'drop_last') %>%
  data.frame()

expect_2012 <- expect_2009 %>% 
  left_join(code_changes_2012, by=c("gss_code"="changed_from_code")) %>%
  mutate(gss_code = ifelse(is.na(changed_to_code), gss_code, changed_to_code)) %>% 
  group_by(gss_code) %>%
  summarise(value = sum(value), .groups = 'drop_last') %>%
  data.frame()

expect_2013 <- expect_2012 %>% 
  left_join(code_changes_2013, by=c("gss_code"="changed_from_code")) %>%
  mutate(gss_code = ifelse(is.na(changed_to_code), gss_code, changed_to_code)) %>% 
  group_by(gss_code) %>%
  summarise(value = sum(value), .groups = 'drop_last') %>%
  data.frame() %>%
  mutate(value=ifelse(gss_code == "E06000057", 35, value),
         value=ifelse(gss_code == "E07000243", 5, value))

expect_2018 <- expect_2013 %>% 
  left_join(code_changes_2018, by=c("gss_code"="changed_from_code")) %>%
  mutate(gss_code = ifelse(is.na(changed_to_code), gss_code, changed_to_code)) %>% 
  group_by(gss_code) %>%
  summarise(value = sum(value), .groups = 'drop_last') %>%
  data.frame()

expect_2019 <- expect_2018 %>% 
  left_join(code_changes_2019, by=c("gss_code"="changed_from_code")) %>%
  mutate(gss_code = ifelse(is.na(changed_to_code), gss_code, changed_to_code)) %>% 
  group_by(gss_code) %>%
  summarise(value = sum(value), .groups = 'drop_last') %>%
  data.frame()

expect_2020 <- expect_2019 %>% 
  left_join(code_changes_2020, by=c("gss_code"="changed_from_code")) %>%
  mutate(gss_code = ifelse(is.na(changed_to_code), gss_code, changed_to_code)) %>% 
  group_by(gss_code) %>%
  summarise(value = sum(value), .groups = 'drop_last') %>%
  data.frame()

output_2009 <- recode_gss_codes(df_1, data_cols = "value", recode_to_year = 2009, code_changes_path = code_changes_path)
output_2012 <- recode_gss_codes(df_1, data_cols = "value", recode_to_year = 2012, code_changes_path = code_changes_path)
output_2013 <- recode_gss_codes(df_1, data_cols = "value", recode_to_year = 2013, code_changes_path = code_changes_path)
output_2018 <- recode_gss_codes(df_1, data_cols = "value", recode_to_year = 2018, code_changes_path = code_changes_path)
output_2019 <- recode_gss_codes(df_1, data_cols = "value", recode_to_year = 2019, code_changes_path = code_changes_path)
output_2020 <- recode_gss_codes(df_1, data_cols = "value", recode_to_year = 2020, code_changes_path = code_changes_path)
output_2020_hc1 <- recode_gss_codes(df_1, data_cols = "value", recode_to_year = 2020, code_changes_path = NULL)
output_2020_hc2 <- recode_gss_codes(df_1, data_cols = "value", recode_to_year = 2020)

expect_2_vars <- mutate(expect_2020, value_2=value)
output_2_vars <- mutate(df_1, value_2 = value) %>% 
  recode_gss_codes(data_cols = c("value","value_2"), recode_to_year = 2020, code_changes_path = code_changes_path)

expect_mean <- expect_2020 %>%
  mutate(value = 5) %>% 
  arrange(gss_code)

output_mean <- recode_gss_codes(df_1, data_cols = "value",
                                recode_to_year = 2020,
                                fun = "mean",
                                code_changes_path = code_changes_path) %>% 
  arrange(gss_code)

#test with 1 var
test_that("recode_gss_codes produces the expected output", {
  expect_equal(expect_2009, output_2009)
  expect_equal(expect_2012, output_2012)
  expect_equal(expect_2013, output_2013)
  expect_equal(expect_2018, output_2018)
  expect_equal(expect_2019, output_2019)
  expect_equal(expect_2020, output_2020)
})

#test 2 variables
test_that("recode_gss_codes produces the expected output", {
  expect_equivalent(expect_2_vars, output_2_vars)
})

#test with mean not sum
test_that("recode_gss_codes produces the expected output", {
  expect_equivalent(expect_mean, output_mean)
})

#test hard-coded dataframe works
test_that("recode_gss_codes produces the expected output", {
  expect_equal(expect_2020, output_2020_hc1)
  expect_equal(expect_2020, output_2020_hc2)
})
