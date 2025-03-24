library(testthat)
library(popmodules)
library(dplyr)

in_constraint_1 <- expand.grid(year=2000, age=c(20:21), sex=c("female","male"), cross_in=400, country = "E", stringsAsFactors = FALSE)
out_constraint_1 <- expand.grid(year=2000, age=c(20:21), sex=c("female","male"), cross_out=400, country = "E", stringsAsFactors = FALSE)

in_constraint_2 <- expand.grid(year=2000, age=c(20:21), sex=c("female","male"), cross_in=100, country = "E", stringsAsFactors = FALSE)
out_constraint_2 <- expand.grid(year=2000, age=c(20:21), sex=c("female","male"), cross_out=100, country = "E", stringsAsFactors = FALSE)


domestic_flow <- expand.grid(year=2000, age=c(20:21), sex=c("female","male"),
                             gss_out = c("E01", "E02", "S01"),
                             gss_in = c("E01", "E02", "S01"),
                             flow = 100,
                             stringsAsFactors = FALSE) %>%
  filter(gss_out!=gss_in)

x_1 <- constrain_cross_border(domestic_flow, in_constraint_1, out_constraint_1, col_flow = "flow")
x_2 <- constrain_cross_border(domestic_flow, in_constraint_2, out_constraint_2, col_flow = "flow")

output_1 <- domestic_flow %>%
  mutate(cb = ifelse(substr(gss_out,1,1)=="E" & substr(gss_in,1,1)=="E",TRUE,FALSE)) %>%
  mutate(flow = ifelse(cb==TRUE,100,200))

output_2 <- output_1 %>%
  mutate(flow = ifelse(cb==TRUE,100,50)) %>%
  select(names(domestic_flow))%>%
  arrange(gss_in, gss_out, sex, age)

output_1 <- output_1 %>%
  select(names(domestic_flow))%>%
  arrange(gss_in, gss_out, sex, age)

test_that("constrain_cross_border can scale up and down",{
  expect_equivalent(x_1, output_1)
  expect_equivalent(x_2, output_2)
})
