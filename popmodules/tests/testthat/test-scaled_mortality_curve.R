library(popmodules)
library(testthat)
library(dplyr)
library(tidyr)
library(purrr)


last_data_year <- 2005
years_to_avg <- 3

# #------------------------------------------------

a <- function(df){
  lm(scaling ~ year, data=df)
}

b <- function(df){
  coef(df)
}

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

deaths <- expand.grid(year = c(2001:2005),
                      gss_code = c("E0901", "E0902"),
                      age = c(0:90),
                      sex = c("male", "female"),
                      stringsAsFactors = F)
deaths$deaths <- sample(c(5:15),nrow(deaths),replace=T)

curves <- expand.grid(gss_code = c("E0901", "E0902"),
                      age = c(0:90),
                      sex = c("male", "female"),
                      year = 2005,
                      stringsAsFactors = F)
curves$rate <- sample(seq(0.001:0.01, by=0.01),nrow(curves),replace=T)

#-------------------------------------------------------------------------------

aged <- pop_data %>%
  popn_age_on2(births = births)

curves_no_year <- select(curves, -year)

#-------------------------------------------------------------------------------

scaling_backseries <- left_join(aged, curves_no_year, by = c("gss_code", "age", "sex")) %>%
  mutate(curve_deaths = rate * popn) %>%
  left_join(deaths, by = c("gss_code", "age", "sex", "year")) %>%
  group_by(gss_code, year, sex) %>%
  summarise(actual_deaths = sum(deaths),
            curve_deaths = sum(curve_deaths),
            .groups = 'drop_last') %>%
  mutate(scaling = actual_deaths / curve_deaths) %>%
  select(gss_code, year, sex, scaling) %>%
  data.frame()

scaled_backseries <- scaling_backseries %>% 
  left_join(curves_no_year, by = c("gss_code", "sex")) %>% 
  mutate(rate = rate * scaling) %>% 
  select(year, gss_code, sex, age, rate) %>% 
  filter(year != 2006)

back_years <- c((last_data_year - years_to_avg + 1):last_data_year)

#-------------------------------------------------------------------------------

mean <- filter(scaling_backseries, year %in% back_years) %>%
  group_by(gss_code, sex) %>%
  summarise(scaling = sum(scaling)/years_to_avg, .groups = 'drop_last') %>%
  data.frame() %>%
  mutate(year = last_data_year+1) %>%
  left_join(curves_no_year, by = c("gss_code","sex")) %>% 
  mutate(rate = rate*scaling) %>% 
  select(gss_code, year, sex, age, rate) %>%
  rbind(scaled_backseries) %>%
  arrange(gss_code, year, sex, age)

trend <- scaling_backseries %>%
  filter(year %in% back_years) %>%
  mutate(year = years_to_avg - last_data_year + year) %>%
  group_by(gss_code, sex)%>%
  tidyr::nest() %>%
  mutate(
    Model = map(data, a),
    Coef = Model %>% map(b),
    Intercept = Coef %>% map_dbl("(Intercept)"),
    Slope = Coef %>% map_dbl("year"),
    scaling = Slope * (years_to_avg+1) + Intercept,
    scaling = ifelse(scaling < 0, 0, scaling)) %>%
  as.data.frame()  %>%
  mutate(year = last_data_year+1) %>%
  left_join(curves_no_year, by = c("gss_code","sex")) %>%
  mutate(rate = scaling * rate) %>%
  select(gss_code, year, sex, age, rate) %>%
  rbind(scaled_backseries) %>%
  arrange(gss_code, year, sex, age)

x <- scaled_mortality_curve(pop_data, births, deaths,
                            curves, last_data_year,
                            years_to_avg, avg_or_trend="trend",
                            data_col = "deaths", output_col = "rate") %>% 
  arrange(gss_code, year, sex, age) %>% 
  filter(year > 2001)

#-----------------------------------------------------------
#devtools::load_all('model_code/popmodules')
test_that("scaled_mortality_curve function: test with mean", {
  expect_equivalent(
    scaled_mortality_curve(pop_data, births, deaths,
                           curves, last_data_year,
                           years_to_avg, avg_or_trend="average",
                           data_col = "deaths", output_col = "rate") %>% 
      arrange(gss_code, year, sex, age),
    mean)
})


test_that("scaled_mortality_curve function: test with regression", {
  expect_equivalent(
    scaled_mortality_curve(pop_data, births, deaths,
                           curves, last_data_year,
                           years_to_avg, avg_or_trend="trend",
                           data_col = "deaths", output_col = "rate") %>%
      arrange(gss_code, year, sex, age),
    trend)
})

