library(testthat)
library(dplyr)

#This function works with a lot of data
#The test takes around 10 seconds

lookup_path <-  "test_data/district_to_region.rds"
inner_outer_lookup <- "test_data/inner_and_outer_london.rds"

lookup <- readRDS(lookup_path)
inner_outer <- readRDS(inner_outer_lookup)

df_1 <- expand.grid(gss_in = unique(lookup$gss_code),
                    gss_out = unique(lookup$gss_code),
                    sex = c("female","male"),
                    age = 0:5,
                    year = 2021) %>% 
  dtplyr::lazy_dt() %>%
  filter(gss_in != gss_out) %>% 
  mutate(flow = 1,
         gss_in = as.character(gss_in),
         gss_out = as.character(gss_out)) 

out_1 <- aggregate_regional_flows(domestic_flow = data.frame(df_1),
                                  region_lookup = lookup,
                                  flow_col = "flow",
                                  inner_outer_lookup = inner_outer_lookup)

expect_regions <- df_1 %>% 
  left_join(lookup, by=c("gss_in"="gss_code")) %>% 
  select(-gss_in) %>% 
  rename(gss_in = region_gss_code) %>% 
  left_join(lookup, by=c("gss_out"="gss_code")) %>% 
  select(-gss_out) %>% 
  rename(gss_out = region_gss_code) %>% 
  filter(gss_in != gss_out) %>% 
  group_by(gss_in, gss_out, sex, age, year) %>% 
  summarise(flow = sum(flow), .groups = 'drop_last') %>% 
  data.frame() %>% 
  select(year, gss_in, gss_out, age, sex, flow) %>% 
  arrange(year, gss_in, gss_out, age, sex)

expect_national <- df_1 %>% 
  mutate(gss_in = case_when(substr(gss_in,1,1)=="E" ~ "E92000001",
                            substr(gss_in,1,1)=="W" ~ "W92000004",
                            TRUE ~ gss_in),
         gss_out = case_when(substr(gss_out,1,1)=="E" ~ "E92000001",
                             substr(gss_out,1,1)=="W" ~ "W92000004",
                             TRUE ~ gss_out)) %>% 
  filter(gss_in != gss_out) %>% 
  group_by(gss_in, gss_out, sex, age, year) %>% 
  summarise(flow = sum(flow), .groups = 'drop_last') %>% 
  data.frame() %>% 
  select(year, gss_in, gss_out, age, sex, flow) %>% 
  arrange(year, gss_in, gss_out, age, sex)

expect_sub_regions <- df_1 %>% 
  left_join(inner_outer, by=c("gss_in"="gss_code")) %>% 
  select(-gss_in) %>% 
  rename(gss_in = gss_code_region) %>% 
  left_join(inner_outer, by=c("gss_out"="gss_code")) %>% 
  select(-gss_out) %>% 
  rename(gss_out = gss_code_region) %>% 
  filter(gss_in != gss_out) %>% 
  group_by(gss_in, gss_out, sex, age, year) %>% 
  summarise(flow = sum(flow), .groups = 'drop_last') %>% 
  data.frame() %>% 
  select(year, gss_in, gss_out, age, sex, flow) %>% 
  arrange(year, gss_in, gss_out, age, sex)

outer_codes <- filter(inner_outer, outer == TRUE)$gss_code
inner_codes <- filter(inner_outer, inner == TRUE)$gss_code

expect_sub_regions <- df_1 %>%
  mutate(gss_out = case_when(gss_out %in% outer_codes ~ "E13000002",
                             gss_out %in% inner_codes ~ "E13000001",
                             TRUE ~ gss_out),
         gss_in = case_when(gss_in %in% outer_codes ~ "E13000002",
                            gss_in %in% inner_codes ~ "E13000001",
                            TRUE ~ gss_in)) %>% 
  filter(gss_in != gss_out) %>%
  group_by(year, gss_in, gss_out, age, sex) %>%
  summarise(flow = sum(flow)) %>%
  as.data.frame()

out_regions <- out_1[[1]] %>% 
  arrange(year, gss_in, gss_out, age, sex)

out_national <- out_1[[2]] %>% 
  arrange(year, gss_in, gss_out, age, sex)

out_sub_region <- out_1[[3]] %>% 
  arrange(year, gss_in, gss_out, age, sex)

test_that("aggregate_regional_flows", {
  
  #basic operation
  expect_equal(out_regions, expect_regions)
  expect_equal(out_national, expect_national)
  expect_equal(out_sub_region, expect_sub_regions)
  
})

