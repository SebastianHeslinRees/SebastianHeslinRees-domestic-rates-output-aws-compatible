library(popmodules)
library(testthat)
library(dplyr)
library(tidyr)
library(purrr)

pop_data <- expand.grid(year = c(2001:2005),
                        gss_code = c("E0901", "E0902"),
                        age = c(0:90),
                        sex = c("male", "female"),
                        stringsAsFactors = F)

pop_data$popn <- sample(c(20:100),nrow(pop_data), replace=T)

births <- expand.grid(year = c(2001:2005),
                      gss_code = c("E0901", "E0902"),
                      age = c(0:90),
                      sex = c("male", "female"),
                      stringsAsFactors = F)

births$births <- sample(c(20:100),nrow(births), replace=T)
births <- mutate(births, births = ifelse(age==0, births, 0))

curves <- expand.grid(gss_code = c("E0901", "E0902"),
                      age = c(15:49),
                      sex = "female",
                      year = 2005,
                      stringsAsFactors = F)
curves$rate <- sample(c(1:100),nrow(curves),replace=T)
curves$rate <- ifelse(curves$age %in% c(0,90), 0, curves$rate)

curves_no_year <- curves %>% select(-year)

aged_popn <- pop_data %>% 
  filter(sex == "female", age %in% unique(curves$age)) %>% 
  popn_age_on2(births = NULL)

total_births <- group_by(births, year, gss_code) %>%
  summarise(births = sum(births), .groups = 'drop_last')

last_data_year <- 2005
years_to_avg <- 3

#-------------------------------------------------------------------------------

a <- function(df){lm(scaling ~ year, data=df)}
b <- function(df){coef(df)}

#-------------------------------------------------------------------------------

scaling_backseries <- aged_popn %>% 
  left_join(curves_no_year, by = c("gss_code", "age", "sex")) %>%
  mutate(curve_births = rate * popn) %>%
  group_by(gss_code, year) %>%
  summarise(curve_births = sum(curve_births), .groups = 'drop_last') %>%
  ungroup() %>%
  left_join(total_births, by = c("gss_code", "year")) %>%
  mutate(scaling = births / curve_births) %>%
  select(gss_code, year, scaling) %>%
  data.frame()

scaled_backseries <- scaling_backseries %>% 
  left_join(curves_no_year, by = c("gss_code")) %>% 
  mutate(rate = rate * scaling) %>% 
  select(year, gss_code, sex, age, rate) %>% 
  filter(year != 2006)

back_years <- c((last_data_year - years_to_avg + 1):last_data_year)

#-------------------------------------------------------------------------------

mean <- scaling_backseries %>% 
  filter(year %in% back_years) %>%
  group_by(gss_code) %>%
  summarise(scaling = sum(scaling)/years_to_avg, .groups = 'drop_last') %>%
  data.frame() %>%
  mutate(year = last_data_year+1) %>%
  left_join(curves_no_year, by = c("gss_code")) %>%
  mutate(rate = scaling * rate) %>% 
  select(year, gss_code, sex, age, rate) %>%
  rbind(scaled_backseries) %>% 
  arrange(gss_code, year, sex, age)

#-------------------------------------------------------------------------------

trend <- scaling_backseries %>%
  filter(year %in% back_years) %>%
  mutate(year = years_to_avg - last_data_year + year)%>%
  group_by(gss_code)%>%
  tidyr::nest() %>%
  mutate(
    Model = map(data, a),
    Coef = Model %>% map(b),
    Intercept = Coef %>% map_dbl("(Intercept)"),
    Slope = Coef %>% map_dbl("year"),
    scaling = Slope * (years_to_avg+1) + Intercept,
    scaling = ifelse(scaling < 0, 0, scaling)) %>%
  as.data.frame()  %>%
  mutate(year = last_data_year + 1) %>%
  left_join(curves_no_year, by = c("gss_code")) %>%
  mutate(rate = scaling * rate) %>% 
  select(year, gss_code, sex, age, rate) %>% 
  rbind(scaled_backseries) %>% 
  arrange(gss_code, year, sex, age)

#-----------------------------------------------------------

test_that("scaled_fertility_curve function: test with mean", {
  expect_equivalent(
    scaled_fertility_curve(pop_data, births,
                           curves, last_data_year,
                           years_to_avg, avg_or_trend="average",
                           data_col = "births", output_col = "rate") %>%
      arrange(gss_code, year, sex, age),
    mean)
})


test_that("scaled_fertility_curve function: test with regression", {
  expect_equivalent(
    scaled_fertility_curve(pop_data, births,
                           curves, last_data_year,
                           years_to_avg, avg_or_trend="trend",
                           data_col = "births", output_col = "rate") %>%
      arrange(gss_code, year, sex, age),
    trend)
})

