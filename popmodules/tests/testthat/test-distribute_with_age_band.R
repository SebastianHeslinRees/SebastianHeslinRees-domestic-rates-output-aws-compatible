library(testthat)
library(dplyr)

df <- expand.grid(gss_code = c("E111","E222"),
                    age_group = "0_5",
                    sex = c("male","female"),
                    persons = 3600) %>% 
  mutate(gss_code = as.character(gss_code),
         age_group = as.character(age_group)) %>% 
  as.data.frame()

df_1 <- expand_grid(gss_code = c("E111","E222"),
                    sex = c("male","female"),
                    age = 0:5) %>% 
  mutate(popn = runif(24,100,200)) %>% 
  as.data.frame()

out_1 <- df_1 %>% 
  group_by(gss_code, sex) %>% 
  mutate(persons = (popn / sum(popn))*3600) %>% 
  as.data.frame() %>% 
  mutate(age_group = "0_5") %>% 
  select("gss_code","age_group","sex","persons","age") %>% 
  arrange(gss_code, sex, age)

df_2 <- df_1 %>% 
  mutate(gss_name = ifelse(gss_code == "E111", "A", "B"))

out_2 <- out_1 %>% 
  mutate(gss_name = ifelse(gss_code == "E111", "A", "B")) %>% 
  select(gss_code,age_group,sex,persons,gss_name,age)

test_that("distribute_with_age_band failed", {
  
  #basic operation
  expect_equal(distribute_within_age_band(popn_1=df, popn_2=df_1,
                                          popn_1_col="persons", popn_2_col="popn",
                                          min_age=0, max_age=5,
                                          col_aggregation=c("gss_code","sex"),
                                          additional_dist_cols = NULL) %>% 
                 arrange(gss_code, sex, age),
               out_1)
  
  #correctly drop a column
  expect_equal(distribute_within_age_band(popn_1=df, popn_2=df_2,
                                          popn_1_col="persons", popn_2_col="popn",
                                          min_age=0, max_age=5,
                                          col_aggregation=c("gss_code","sex"),
                                          additional_dist_cols = NULL) %>% 
                 arrange(gss_code, sex, age),
               out_1)
  
  #correctly retain a column
  expect_equal(distribute_within_age_band(popn_1=df, popn_2=df_2,
                                          popn_1_col="persons", popn_2_col="popn",
                                          min_age=0, max_age=5,
                                          col_aggregation=c("gss_code","sex"),
                                          additional_dist_cols = "gss_name") %>% 
                 arrange(gss_code, sex, age),
               out_2)
  
})
