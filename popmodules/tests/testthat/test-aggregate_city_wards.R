library(dplyr)

#lookup
expand.grid(gss_code_ward = c(paste0("E0500000",1:9), paste0("E050000",10:25)),
            gss_name = "x",
            gss_code = "E09000001") %>% 
  data.frame() %>% 
  saveRDS("test_data/test_city_ward_lookup.rds")

df_1<- expand.grid(gss_code_ward = paste0("E0500000",1:9),
                   age = 0:90,
                   persons = 1) %>% 
  data.frame()

out_1 <- df_1 %>% 
  mutate(gss_code_ward = "E09000001") %>% 
  group_by(gss_code_ward, age) %>% 
  summarise(persons = sum(persons),
            .groups = 'drop_last') %>% 
  data.frame()

df_2 <- expand.grid(gss_code_ward = c(paste0("E0500000",1:9), paste0("E050000",10:20)),
                    persons = 1) %>% 
  data.frame()

out_2 <- df_2 %>% 
  mutate(gss_code_ward = "E09000001") %>% 
  group_by(gss_code_ward) %>% 
  summarise(persons = sum(persons),
            .groups = 'drop_last') %>% 
  data.frame()

df_3 <- expand.grid(gss_code_ward = c(paste0("E0500000",1:9), paste0("E050000",80:90)),
                    persons = 1) %>% 
  data.frame() %>% 
  mutate(gss_code_ward = as.character(gss_code_ward))

out_3 <- df_3 %>% 
  mutate(gss_code_ward = ifelse(substr(gss_code_ward,8,8)=="0",
                                "E09000001",
                                gss_code_ward)) %>% 
  group_by(gss_code_ward) %>% 
  summarise(persons = sum(persons),
            .groups = 'drop_last') %>% 
  data.frame()

df_4 <- data.frame(gss_code_ward = rep("E09000001",30),
                           persons = rep(1,30)) %>% 
  mutate(gss_code_ward = as.character(gss_code_ward))


test_that("aggreagte_city_wards fails", {
  
  #All data is city, all columns retained
  expect_equal(aggregate_city_wards(df_1, data_col="persons",
                                    "test_data/test_city_ward_lookup.rds"),
               out_1)
  
  #All data is city, columns collapsed
  expect_equal(aggregate_city_wards(df_2, data_col="persons",
                                    "test_data/test_city_ward_lookup.rds"),
               out_2)
  
  #Data for wards outside city
  expect_equal(aggregate_city_wards(df_3, data_col="persons",
                                    "test_data/test_city_ward_lookup.rds"),
               out_3)

})

test_that("aggreagte_city_wards fails", {

  #E09000001 is in the input dataframe
  expect_error(aggregate_city_wards(df_4, data_col="persons",
                                    "test_data/test_city_ward_lookup.rds"))
})

