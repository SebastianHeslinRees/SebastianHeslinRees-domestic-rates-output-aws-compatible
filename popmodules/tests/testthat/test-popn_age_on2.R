# library(popmodules)
# library(testthat)
# library(dplyr)
# 
# # Simple test population
# popn <- expand.grid( year = 2000, gss_code=c("a","b","c"), age=20:22, sex=c("f","m"), popn = 100, stringsAsFactors = FALSE)
# # Simple test population with an ordered factor for age
# popn_banded <- expand.grid( year = 2000, gss_code=c("a","b","c"), age=factor(c("x","y","z"), ordered=TRUE), sex=c("f","m"), popn = 100, stringsAsFactors = FALSE)
# 
# # The same populations aged on
# aged <- expand.grid( year = 2001, gss_code=c("a","b","c"), age=21:22, sex=c("f","m"), stringsAsFactors = FALSE)
# aged$popn <- ifelse(aged$age == 22, 200, 100)
# aged_banded <- expand.grid( year = 2001, gss_code=c("a","b","c"), age=factor(c("y","z"), ordered=TRUE), sex=c("f","m"), stringsAsFactors = FALSE)
# aged_banded$popn <- ifelse(aged_banded$age == "z", 200, 100)
# 
# arrange_popn <- function(popn) arrange(popn, year, gss_code, age, sex)
# popn <- arrange_popn(popn)
# aged <- arrange_popn(aged)
# popn_banded <- arrange_popn(popn_banded)
# aged_banded <- arrange_popn(aged_banded)
# 
# births <- expand.grid(year = 2001, gss_code = c("a","b","c"), age = 0, sex = c("f","m"), births = 100, stringsAsFactors = F)
# 
# aged_w_births <- rbind(rename(births, popn = births),aged)
# aged_w_births <- arrange_popn(aged_w_births)
# 
# no_births <- births
# no_births$births <- 0
# 
# aged_w_no_births <- rbind(rename(no_births, popn = births),aged)
# aged_w_no_births <- arrange_popn(aged_w_no_births)
# 
# # The function being tested is:
# #
# #popn_age_on2 <- function(popn,
# #                        col_aggregation=c("year","gss_code","age","sex"),
# #                        col_age = "age",
# #                        col_year = "year",
# #                        timestep = 1,
# #                        template_age_levels = NULL)
# #
# # These default values will be used int eh test unless otherwise specified
# 
# 
# #--------------------------------------------------------------
# 
# test_that("popn_age_on2 works on a simple population", {
#   expect_equivalent(popn_age_on2(popn),
#                     aged)
# })
# 
# test_that("popn_age_on2 works on a simple population with births specified", {
#   expect_equivalent(popn_age_on2(popn, births=births),
#                     aged_w_births)
#   
#   expect_equivalent(popn_age_on2(popn, births=0),
#                     aged_w_no_births)
# })
# 
# 
# test_that("popn_age_on2 works with multiple data columns, including text data", {
#   
#   popn_in  <- mutate(popn, popn2 = popn)
#   aged_out <- mutate(aged, popn2 = popn)
#   expect_equivalent(popn_age_on2(popn_in,
#                                  col_data = c("popn","popn2")),
#                     aged_out)
# })
# 
# test_that("popn_age_on2 handles custom column names", {
#   popn_in  <- rename(popn, xyear=year, xgss_code=gss_code, xage=age, xsex=sex, xpopn=popn)
#   aged_out <- rename(aged, xyear=year, xgss_code=gss_code, xage=age, xsex=sex, xpopn=popn)
#   expect_equivalent(popn_age_on2(popn_in,
#                                  col_aggregation = c("xyear","xgss_code","xage","xsex"),
#                                  col_age = "xage",
#                                  col_year = "xyear",
#                                  col_data = "xpopn",
#                                  col_geog = "xgss_code"),
#                     aged_out)
# })
# 
# test_that("popn_age_on2 doesn't care about the order of col_aggregation", {
#   expect_equivalent(popn_age_on2(popn, col_aggregation = c("sex","age","gss_code","year")),
#                     aged)
# })
# 
# test_that("popn_age_on2 throws an error with explicit missing aggregation values", {
#   popn_in <- popn
#   popn_in$gss_code[1] <- NA
#   expect_error(popn_age_on2(popn_in))
# })
# 
# test_that("popn_age_on2 throws an error with implicit missing aggregation values", {
#   popn_in <- popn[-1,]
#   expect_error(popn_age_on2(popn_in))
# })
# 
# test_that("popn_age_on2 throws an error when there's only one age level in the input", {
#   popn_in <- expand.grid( year = 2000, gss_code=c("a","b","c"), age=20, sex=c("f","m"), popn = 100, stringsAsFactors = FALSE)
#   expect_error(popn_age_on2(popn_in))
# })
# 
# test_that("popn_age_on2 throws an error with duplicate aggregation values", {
#   popn_in <- select(popn, -sex)
#   expect_error(popn_age_on2(popn_in, col_aggregation = c("year","gss_code","age")))
# })
# 
# test_that("popn_age_on2 works on data without a year column", {
#   popn_in  <- select(popn, -year)
#   aged_out <- select(aged, -year)
#   expect_equivalent(popn_age_on2(popn_in, col_aggregation = c("gss_code","age","sex"), col_year = NULL),
#                     aged_out)
# })
# 
# 
# test_that("popn_age_on2 can handle multiple years of data)", {
#   popn_in  <- popn %>% 
#     mutate(year = year + 1,
#            popn = popn * 2) %>% 
#     rbind(popn)
#   aged_out <-  aged %>% 
#     mutate(year = year + 1,
#            popn = popn*2) %>% 
#     rbind(aged) %>%
#     arrange(year, gss_code, age, sex)
#   
#   expect_equivalent(popn_age_on2(popn_in),
#                     aged_out)
#   
#   #with births
#   births_in <- births %>% 
#     mutate(year = year + 1,
#            births = births * 2) %>%
#     rbind(births)
#   
#   aged_out <- rbind(aged_out, rename(births_in, popn = births)) %>%
#     arrange(year, gss_code, age, sex)
#   
#   expect_equivalent(popn_age_on2(popn_in, births=births_in),
#                     aged_out)
#   
# })
# 
# #-------------------------------------------------------------------------------
# 
# #small area data
# 
# small_area_popn <- data.frame(gss_code = c(rep("a",3),rep("b",3),rep("c",4)),
#                               gss_code_ward = c("l","m","o","p","q","r","s","t","u","v")) %>% 
#   left_join(popn, by = "gss_code")
# 
# small_area_births <- small_area_popn %>% 
#   mutate(age = 0) %>% 
#   mutate(births = 15,
#          year = 2001) %>% 
#   select(-popn) %>% 
#   unique() %>% 
#   select(gss_code, gss_code_ward, year, age, sex, births)
# 
# small_area_out <- small_area_popn %>% 
#   mutate(age = ifelse(age == 22, 22, age + 1),
#          year = year + 1) %>% 
#   group_by(gss_code, gss_code_ward, year, age, sex) %>% 
#   summarise(popn = sum(popn), .groups = 'drop_last') %>% 
#   data.frame()
# 
# small_area_out_births <- rename(small_area_births, popn = births) %>% 
#   rbind(small_area_out) %>% 
#   arrange(gss_code_ward, age, sex)
# 
# small_area_wrong <- small_area_popn %>% 
#   mutate(gss_code_ward = ifelse(gss_code_ward == "l", "z", gss_code_ward))
# 
# test_that("popn_age_on2 can handle small area data - errors)", {
#   
#   expect_error(popn_age_on2(small_area_popn))
#   
#   expect_error(popn_age_on2(small_area_popn,
#                             col_aggregation = c("year","gss_code","sex","age")))
#   
#   expect_error(popn_age_on2(small_area_popn,
#                             col_aggregation = c("year","gss_code","gss_code_ward","sex","age")))
#   
# })
# 
# 
# test_that("popn_age_on2 can handle small area data - errors)", {
#   
#   expect_error(popn_age_on2(small_area_wrong,
#                             col_aggregation = c("year","gss_code_ward","sex","age"),
#                             births = small_area_births,
#                             col_geog = "gss_code_ward"))
# })
# 
# 
# test_that("popn_age_on2 can handle small area data - normal operation", {
#   
#   expect_equivalent(popn_age_on2(small_area_popn,
#                                  col_aggregation = c("year","gss_code_ward","sex","age")),
#                     select(small_area_out, -gss_code))
#   
#   expect_equivalent(popn_age_on2(small_area_popn,
#                                  col_aggregation = c("year","gss_code_ward","sex","age"),
#                                  births = small_area_births),
#                     select(small_area_out_births, -gss_code))
#   
#   expect_equivalent(popn_age_on2(small_area_popn,
#                                  col_aggregation = c("year","gss_code_ward","sex","age"),
#                                  births = small_area_births,
#                                  col_geog = "gss_code_ward"),
#                     select(small_area_out_births, -gss_code))
#   
#   
# })
