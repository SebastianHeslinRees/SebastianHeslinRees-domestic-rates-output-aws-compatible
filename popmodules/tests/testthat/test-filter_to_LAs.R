library(testthat)
library(dplyr)

df_1 <- expand.grid(gss_code = c(paste0("E06",1:4),
                               paste0("E07",1:4),
                               paste0("E08",1:4),
                               paste0("E09",1:4),
                               paste0("W06",1:4),
                               paste0("N92",1:4),
                               paste0("S92",1:4)),
                  persons = 1) %>% 
  data.frame() %>% 
  mutate(gss_code = as.character(gss_code))

df_2 <- expand.grid(gss_code = c(paste0("E06",1:4),
                                 paste0("E07",1:4),
                                 paste0("E05",1:4)),
                    persons = 1) %>% 
  data.frame() %>% 
  mutate(gss_code = as.character(gss_code))

  
out_2 <- df_2 %>% filter(substr(gss_code, 1, 3) != "E05")

test_that( "filter_to_LAs fails",{
  
  #does nothing when it doesn't need to do anything
  expect_equal(filter_to_LAs(df_1), df_1)
  
  #filters out E05 codes
  expect_equal(filter_to_LAs(df_2), out_2)
  
  #error when passed a col name that doesn't exist
  expect_error(filter_to_LAsdf_1, gss_col = "bob")
  
  #error when passed something that isn't a dataframe
  expect_error(filter_to_LAs(c("once upon a time","you dressed so fine",
                               "threw the bums a dime","in your prime")))
})
