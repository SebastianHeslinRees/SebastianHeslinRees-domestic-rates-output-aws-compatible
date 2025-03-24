library(dplyr)
library(testthat)

# popn <- expand.grid(year = c(2000:2004),
#                     gss_code = c("E0901", "E0902"),
#                     age = c(0:3),
#                     sex = c("male", "female"),
#                     stringsAsFactors = F) %>%
#   data.frame()
# popn$popn <- sample(c(1000:3000), size=80, replace=T)
#
# births <- expand.grid(year = c(2001:2005),
#                     gss_code = c("E0901", "E0902"),
#                     age = 0,
#                     sex = c("male", "female"),
#                     stringsAsFactors = F) %>%
#   data.frame()
# births$births <- sample(c(1000:3000), size=20, replace=T)
#
# components <- expand.grid(year = c(2001:2005),
#                           gss_code = c("E0901", "E0902"),
#                           age = c(0:3),
#                           sex = c("male", "female"),
#                           stringsAsFactors = F)%>%
#   data.frame()
#
# int_out <- components
# int_out$int_out <- sample(c(100:200), size=80, replace=T)
#
# int_in <- components
# int_in$int_in <- sample(c(100:200), size=80, replace=T)

# saveRDS(popn, "model_code/popmodules/tests/testthat/test_data/test-internationa_rates_and_flows_popn.rds")
# saveRDS(births, "model_code/popmodules/tests/testthat/test_data/test-internationa_rates_and_flows_births.rds")
# saveRDS(int_out, "model_code/popmodules/tests/testthat/test_data/test-internationa_rates_and_flows_int_out.rds")
# saveRDS(int_in, "model_code/popmodules/tests/testthat/test_data/test-internationa_rates_and_flows_int_in.rds")

#-----------------------------==

popn_path <- "test_data/test-internationa_rates_and_flows_popn.rds"
births_path <- "test_data/test-internationa_rates_and_flows_births.rds"
int_out_path <- "test_data/test-internationa_rates_and_flows_int_out.rds"
int_in_path <- "test_data/test-internationa_rates_and_flows_int_in.rds"
popn <- readRDS(popn_path)
births <- readRDS(births_path)
int_out <- readRDS(int_out_path)
int_in <- readRDS(int_in_path)

#----------------------------------

aged <- popn %>%
  mutate(year = year +1,
         age = ifelse(age == 3, 3, age +1))%>%
  group_by(year, gss_code, age, sex) %>%
  summarise(popn = sum(popn), .groups = 'drop_last') %>%
  ungroup() %>%
  #filter(year != max(year)) %>%
  rbind(rename(births, popn = births))

out_rates <- int_out %>%
  left_join(aged, by = c("year", "gss_code", "age", "sex")) %>%
  mutate(int_out = int_out/popn) %>%
  select(year, gss_code, sex, age, int_out)

out_flow <- out_rates %>%
  filter(year %in% c(2003:2005)) %>%
  group_by(gss_code, sex, age) %>%
  summarise(int_out = sum(int_out)/3, .groups = 'drop_last') %>%
  ungroup()

out_traj <- list()
for(i in 2006:2011){
  out_traj[[i]] <- mutate(out_flow, year = i)
}
out_traj <- data.table::rbindlist(out_traj) %>%
  data.frame() %>%
  select(year, gss_code, sex, age, int_out) %>%
  mutate(year = as.numeric(year), age=as.numeric(age))


#--------------------------------

in_flow <- filter(int_in, year %in% c(2003:2005)) %>%
  group_by(gss_code, sex, age) %>%
  summarise(int_in = sum(int_in)/3, .groups = 'drop_last') %>%
  ungroup()

in_traj <- list()
for(i in 2006:2011){
  in_traj[[i]] <- mutate(in_flow, year = i)
}
in_traj <- data.table::rbindlist(in_traj) %>%
  data.frame() %>%
  select(year, gss_code, sex, age, int_in) %>%
  mutate(year = as.numeric(year))

#-----------------------------------

x <-  calculate_mean_international_rates_or_flows(popn_mye_path=popn_path,
                                   births_mye_path=births_path,
                                   flow_or_rate="flow",
                                   component_path=int_in_path,
                                   last_data_year=2005,
                                   n_years_to_avg=3,
                                   data_col="int_in",
                                   first_proj_yr=2006,
                                   n_proj_yr=6,
                                   rate_cap = NULL)
test_that(" calculate_international_rates_and_flows error in flow calc", {
  expect_equivalent(x, in_traj)
})


#------------------------------------

x <-  calculate_mean_international_rates_or_flows(popn_mye_path=popn_path,
                                   births_mye_path=births_path,
                                   flow_or_rate="rate",
                                   component_path=int_out_path,
                                   last_data_year=2005,
                                   n_years_to_avg=3,
                                   data_col="int_out",
                                   first_proj_yr=2006,
                                   n_proj_yr=6,
                                   rate_cap = 0.8)
test_that(" calculate_international_rates_and_flows: error in rate calc", {
  expect_equivalent(x, out_traj)
})


test_that(" calculate_international_rates_and_flows: error in rate calc", {
  expect_warning( calculate_mean_international_rates_or_flows(popn_mye_path=popn_path,
                                               births_mye_path=births_path,
                                               flow_or_rate="rate",
                                               component_path=int_out_path,
                                               last_data_year=2005,
                                               n_years_to_avg=3,
                                               data_col="int_out",
                                               first_proj_yr=2006,
                                               n_proj_yr=6,
                                               rate_cap = 0.01))
})

#x <- international_flows(component=int_in, last_data_year=2005, years_to_avg=3, flow_col="int_in", n_proj_years=5)
#y <- left_join(x, in_flow, by = c("year", "gss_code", "age", "sex")) %>% mutate(diff = int_in.x - int_in.y)
