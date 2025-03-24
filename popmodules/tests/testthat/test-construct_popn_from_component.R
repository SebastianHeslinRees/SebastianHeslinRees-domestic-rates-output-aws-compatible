library(testthat)
library(dplyr)

start_population <- expand.grid(gss_code = c("E01","E02","E03"),
                                sex = c("female","male"),
                                age = 0:5,
                                stringsAsFactors = F)
start_population <- mutate(start_population, popn = sample(100:200, nrow(start_population)))


births <- expand.grid(gss_code = c("E01","E02","E03"),
                      sex = c("female","male"),
                      age = 0:5,
                      stringsAsFactors = F)
births <- mutate(births, births = sample(30:50, nrow(start_population), replace= T)) %>%
  mutate(births = ifelse(age == 0, births, 0))

deaths <- expand.grid(gss_code = c("E01","E02","E03"),
                      sex = c("female","male"),
                      age = 0:5,
                      stringsAsFactors = F)
deaths <- mutate(deaths, deaths = sample(30:50, nrow(start_population), replace= T))

int_in <- expand.grid(gss_code = c("E01","E02","E03"),
                      sex = c("female","male"),
                      age = 0:5,
                      stringsAsFactors = F)
int_in <- mutate(int_in, int_in = sample(30:50, nrow(start_population), replace= T))

int_out <- expand.grid(gss_code = c("E01","E02","E03"),
                       sex = c("female","male"),
                       age = 0:5,
                       stringsAsFactors = F)
int_out <- mutate(int_out, int_out = sample(30:50, nrow(start_population), replace= T))

dom_in <- expand.grid(gss_code = c("E01","E02","E03"),
                      sex = c("female","male"),
                      age = 0:5,
                      stringsAsFactors = F)
dom_in <- mutate(dom_in, dom_in = sample(30:50, nrow(start_population), replace= T))

dom_out <- expand.grid(gss_code = c("E01","E02","E03"),
                       sex = c("female","male"),
                       age = 0:5,
                       stringsAsFactors = F)
dom_out <- mutate(dom_out, dom_out = sample(30:50, nrow(start_population), replace= T))


output <- left_join(start_population, births, by = c("gss_code","sex","age")) %>%
  left_join(deaths, c("gss_code","sex","age")) %>%
  left_join(int_in, by = c("gss_code","sex","age")) %>%
  left_join(int_out, by = c("gss_code","sex","age")) %>%
  left_join(dom_in, by = c("gss_code","sex","age")) %>%
  left_join(dom_out, by = c("gss_code","sex","age")) %>%
  mutate(popn = popn + births - deaths + int_in - int_out + dom_in - dom_out) %>%
  select(gss_code, sex, age, popn) %>%
  arrange(gss_code, sex, age) %>% 
  data.frame()


constructed <- construct_popn_from_components(start_population = start_population,
                                              addition_data = list(births, dom_in, int_in),
                                              subtraction_data = list(deaths, dom_out, int_out),
                                              col_aggregation = c("gss_code","sex","age"))

test_that("construct_popn_from_component works", {
  expect_equal(constructed, output)
})

expect_error(construct_popn_from_components(start_population = start_population,
                                            addition_data = list(select(births, births, gss_code, sex, age),
                                                                 dom_in, int_in),
                                            subtraction_data = list(deaths, dom_out, int_out),
                                            col_aggregation = c("gss_code","sex","age")))

expect_error(construct_popn_from_components(start_population = start_population,
                                            addition_data = list(births, dom_in, int_in),
                                            subtraction_data = list(select(deaths, deaths, gss_code, sex, age),
                                                                    dom_out, int_out),
                                            col_aggregation = c("gss_code","sex","age")))
