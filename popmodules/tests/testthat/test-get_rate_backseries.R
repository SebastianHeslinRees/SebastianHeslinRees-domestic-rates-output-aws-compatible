context("get_rate_backseries")
library(popmodules)
library(testthat)
library(dplyr)

# get_rate_backseries <- function(component_mye_path,
#                                 popn_mye_path,
#                                 births_mye_path,
#                                 col_partial_match = NULL)

set.seed(101)

popn <- expand.grid(year = c(2012, 2013, 2014),
                    gss_code=c("E06000001","E07000001", "E09000002"),
                    age=0:3,
                    sex=c("female","male"), stringsAsFactors = FALSE)
popn$popn <- sample(((nrow(popn)/2)+1):nrow(popn), size = nrow(popn), replace = TRUE)
births <- expand.grid(year = c(2012, 2013, 2014),
                      gss_code=c("E06000001","E07000001", "E09000002"),
                      age=0:3,
                      sex=c("female","male"), stringsAsFactors = FALSE)
births$births <- sample(((nrow(popn)/2)+1):nrow(popn), size = nrow(popn), replace = TRUE)
births$births[births$age > 0] <- 0

deaths <- expand.grid(year = c(2013, 2014),
                      gss_code=c("E06000001","E07000001", "E09000002"),
                      age=0:3,
                      sex=c("female","male"), stringsAsFactors = FALSE)
deaths$deaths <- sample(1:(nrow(deaths)/2), size = nrow(deaths), replace = TRUE)

migration <- expand.grid(year = c(2013, 2014),
                         gss_code=c("E06000001","E07000001", "E09000002"),
                         age=0:3,
                         sex=c("female","male"), stringsAsFactors = FALSE)
migration$dom_out <- sample(1:(nrow(migration)/2), size = nrow(migration), replace = TRUE)

saveRDS(popn, file = "test_data/test-get_rates_backseries_popn.rds")
saveRDS(births, file = "test_data/test-get_rates_backseries_births.rds")
saveRDS(deaths, file = "test_data/test-get_rates_backseries_deaths.rds")
saveRDS(migration, file = "test_data/test-get_rates_backseries_migration.rds")

popn_aged <- mutate(popn, age = age + 1, year = year + 1, age = ifelse(age == 4, 3, age)) %>%
  group_by(year, gss_code, age, sex) %>%
  summarise(popn = sum(popn), .groups = 'drop_last') %>%
  as.data.frame() %>%
  rbind(filter(births, age == 0) %>%
          rename(popn = births)) %>%
  filter(year %in% c(2013, 2014))

mort_answer <- left_join(popn_aged, deaths, by = c("year", "gss_code", "age", "sex")) %>%
  mutate(rate = deaths/popn) %>%
  arrange(year, gss_code, age, sex) %>%
  select(-popn, -deaths)

mig_rate_answer <- left_join(popn_aged, migration, by = c("year", "gss_code", "age", "sex")) %>%
  mutate(rate = dom_out/popn) %>%
  arrange(year, gss_code, age, sex) %>%
  select(-popn, -dom_out)

mortality <- get_rate_backseries(component_mye_path = "test_data/test-get_rates_backseries_deaths.rds",
                                 popn_mye_path = "test_data/test-get_rates_backseries_popn.rds",
                                 births_mye_path = "test_data/test-get_rates_backseries_births.rds",
                                 years_backseries = c(2013,2014)) %>%
  arrange(year, gss_code, age, sex)

migration_rate <- get_rate_backseries(component_mye_path = "test_data/test-get_rates_backseries_migration.rds",
                                      popn_mye_path = "test_data/test-get_rates_backseries_popn.rds",
                                      births_mye_path = "test_data/test-get_rates_backseries_births.rds",
                                      years_backseries = c(2013, 2014)) %>%
  arrange(year, gss_code, age, sex)

test_that("get_rate_backseries creates expected output for mortality test case", {
  expect_equivalent(mortality, mort_answer)
})

test_that("get_rate_backseries creates expected output for migration test case", {
  expect_equivalent(migration_rate, mig_rate_answer)
})

file.remove(file = "test_data/test-get_rates_backseries_popn.rds")
file.remove(file = "test_data/test-get_rates_backseries_births.rds")
file.remove(file = "test_data/test-get_rates_backseries_deaths.rds")
file.remove(file = "test_data/test-get_rates_backseries_migration.rds")

