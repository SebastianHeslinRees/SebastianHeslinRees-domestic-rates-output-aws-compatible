library(testthat)
library(dplyr)

#tests run in the test/testthat folder so we need to go back up the tree
#to get to the lookup
lookup_path <-  "test_data/district_to_region.rds"

df <- readRDS(lookup_path) %>% 
  mutate(popn = 1,
         year = 2021) %>% 
  filter(substr(gss_code,1,1) %in% c("E","W"))

df_1 <- select(df, year, gss_code, popn)

expect_1 <- group_by(df, year, gss_code = region_gss_code) %>% 
  summarise(popn = sum(popn), .groups = 'drop_last') %>% 
  data.frame() %>% 
  rbind(df_1) %>% 
  arrange(gss_code) %>% 
  data.frame()

expect_2 <- df %>% 
  filter(substr(gss_code,1,1) %in% "E") %>% 
  mutate(gss_code = "E92000001") %>% 
  group_by(year, gss_code) %>% 
  summarise(popn = sum(popn), .groups = 'drop_last') %>% 
  data.frame() %>% 
  rbind(expect_1) %>% 
  arrange(gss_code) %>% 
  data.frame()

out_1 <- aggregate_regions(df_1,
                           gss_col = "gss_code",
                           col_aggregation = "year",
                           england = FALSE,
                           lookup = lookup_path) %>% 
  arrange(gss_code)

out_2 <- aggregate_regions(df_1,
                           gss_col = "gss_code",
                           col_aggregation = "year",
                           england = TRUE,
                           lookup = lookup_path) %>% 
  arrange(gss_code)

rownames(expect_1) <- c()
rownames(expect_2) <- c()
rownames(out_1) <- c()
rownames(out_2) <- c()

test_that("aggregate_regions fails",{
  
  #basic operation
  expect_equal(expect_1, out_1)
  
  #with England
  expect_equal(expect_2, out_2)
})

#trigger warnings
test_that("aggregate_regions fails",{
  
  #basic operation
  expect_warning(aggregate_regions(out_1,
                                   gss_col = "gss_code",
                                   col_aggregation = "year",
                                   england = TRUE,
                                   lookup = lookup_path))
  
  #with England
  expect_warning(aggregate_regions(out_2,
                                   gss_col = "gss_code",
                                   col_aggregation = "year",
                                   england = TRUE,
                                   lookup = lookup_path))
})