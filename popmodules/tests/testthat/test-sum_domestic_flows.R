library(testthat)
library(dplyr)

df <- expand.grid(gss_in = c("A","B","C"),
                  gss_out = c("A","B","C"),
                  sex = c("female","male"),
                  age = 0:90,
                  year = 2011:2020) %>% 
  filter(gss_in != gss_out) %>% 
  mutate(flow = runif(10920, 5, 10))

out_1 <- df %>%
  group_by(year, gss_code = gss_in, sex, age) %>%
  summarise(dom_in = sum(flow),
            .groups = 'drop_last') %>%
  data.frame() %>%
  tidyr::complete(year, gss_code, age=0:90, sex, fill=list(flow=0)) %>%
  data.frame()

test_that("sum_domestic_flows failed", {
  
  #basic operation
  expect_equal(sum_domestic_flows(df, in_or_out="in", flow_col = "flow"),
               out_1)
  
  expect_error(sum_domestic_flows(df, in_or_out="shake it all about", flow_col = "flow"))
  
  expect_error(sum_domestic_flows(df, in_or_out="in", flow_col = "wrong"))

})

