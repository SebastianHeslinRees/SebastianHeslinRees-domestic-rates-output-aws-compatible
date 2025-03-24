library(tidyr)
library(dplyr)
library(testthat)

#one geog
df_1 <- expand_grid(gss_code = c("a","b","c"),
                    year = 2006,
                    sex = "female",
                    age = 15:49,
                    rate = 1)

out_1 <- expand_grid(gss_code = c("a","b","c"),
                     year = 2006,
                     sex = c("female","male"),
                     age = 0:90) %>% 
  mutate(rate = case_when(sex == "female" & age %in% 15:49 ~ 1,
                          TRUE ~ 0))

#two geogs
df_2 <- df_1 %>% 
  filter(gss_code != "c") %>% 
  right_join(data.frame(gss_code = c(rep("a",5), rep("b",5)),
                        gss_code_ward = letters[10:19]), by="gss_code") %>% 
  select(gss_code, gss_code_ward, year, sex, age, rate) 

out_2 <- out_1 %>% 
  filter(gss_code != "c") %>% 
  right_join(data.frame(gss_code = c(rep("a",5), rep("b",5)),
                        gss_code_ward = letters[10:19]), by="gss_code") %>% 
  select(gss_code, gss_code_ward, year, sex, age, rate) %>% 
  arrange(gss_code, gss_code_ward, year, sex, age)



test_that("basic operation", {
  expect_equivalent(complete_popn_dataframe(df_1),
                    out_1)
  
  expect_equivalent(complete_popn_dataframe(df_2) %>% 
                      arrange(gss_code, gss_code_ward, year, sex, age),
                    out_2)
})

#no age

df_3 <- df_2 %>% 
  select(-age) %>% 
  unique()

out_3 <- out_2 %>% 
  select(-age) %>%
  mutate(rate = ifelse(sex == "female", 1, 0)) %>% 
  unique()


#no age, no sex

df_4 <- df_2 %>% 
  select(-sex, -age) %>% 
  unique()

test_that("missing cols", {
  expect_equivalent(complete_popn_dataframe(df_3, ages = NULL),
                    out_3)
  
  expect_equivalent(complete_popn_dataframe(df_4, sexes = NULL, ages = NULL),
                    df_4)
})


#complete dataframe

test_that("complete dataframe", {
  expect_equivalent(complete_popn_dataframe(out_2, ages = NULL),
                    out_2)
})


