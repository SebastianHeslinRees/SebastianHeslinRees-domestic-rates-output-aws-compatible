library(dplyr)

pop <- expand.grid(gss_code = c("E01","E02","E03","E04"), age = 2, sex = "female", year = c(1066:1068))
pop$popn <- sample(seq(100:200), size=nrow(pop), replace=T)
                   

rates <- expand.grid(gss_out = c("E01","E02","E03","E04"), gss_in = c("E01","E02","E03","E04"),
                     age=2, sex = "female", rate = 0.1) %>%
  filter(gss_in != gss_out)
rates$rate <- sample(seq(0.1:0.3,by=0.01), size=nrow(rates), replace=T)

rates_out <- filter(rates, gss_out %in% c("E01","E02")) %>%
  filter(!gss_in %in% c("E01","E02"))

rates_in <- filter(rates, gss_in %in% c("E01","E02")) %>%
  filter(!gss_out %in% c("E01","E02"))

answer_in <- filter(pop, !gss_code %in% c("E01","E02")) %>%
  left_join(rates_in, by=c("gss_code"="gss_out","sex","age")) %>%
  mutate(flow = rate*popn) %>%
  group_by(year) %>%
  summarise(dom_in = sum(flow), .groups = 'drop_last')

answer_out <- filter(pop, gss_code %in% c("E01","E02")) %>%
  left_join(rates_out, by=c("gss_code"="gss_out","sex","age")) %>%
  mutate(flow = rate*popn) %>%
  group_by(year) %>%
  summarise(dom_out = sum(flow), .groups = 'drop_last')

answer_net <- left_join(answer_in, answer_out, by="year") %>%
  mutate(dom_net = dom_in-dom_out,
         area = "test_area") %>%
  select(area, year, dom_in, dom_out, dom_net) %>%
  as.data.frame()

x <- calculate_region_domestic_flows(domestic_rates = rates,
                                     nat_chng_popn = pop,
                                     gss_code_list = c("E01","E02"),
                                     region_name = "test_area")

testthat::expect_equivalent(answer_net, x)

