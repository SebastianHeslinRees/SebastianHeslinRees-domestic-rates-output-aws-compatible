library(popmodules)
library(testthat)
library(dplyr)

# Simple test population
popn <- expand.grid( year = 2000, gss_code=c("a","b","c"), age=20:22, sex=c("f","m"), popn = 100, stringsAsFactors = FALSE)
# Simple test population with an ordered factor for age
popn_banded <- expand.grid( year = 2000, gss_code=c("a","b","c"), age=factor(c("x","y","z"), ordered=TRUE), sex=c("f","m"), popn = 100, stringsAsFactors = FALSE)

# The same populations aged on
aged <- expand.grid( year = 2001, gss_code=c("a","b","c"), age=21:22, sex=c("f","m"), stringsAsFactors = FALSE)
aged$popn <- ifelse(aged$age == 22, 200, 100)
aged_banded <- expand.grid( year = 2001, gss_code=c("a","b","c"), age=factor(c("y","z"), ordered=TRUE), sex=c("f","m"), stringsAsFactors = FALSE)
aged_banded$popn <- ifelse(aged_banded$age == "z", 200, 100)

arrange_popn <- function(popn) arrange(popn, year, gss_code, age, sex)
popn <- arrange_popn(popn)
aged <- arrange_popn(aged)
popn_banded <- arrange_popn(popn_banded)
aged_banded <- arrange_popn(aged_banded)

births <- expand.grid(year = 2001, gss_code = c("a","b","c"), age = 0, sex = c("f","m"), births = 100, stringsAsFactors = F)

aged_w_births <- rbind(rename(births, popn = births),aged)
aged_w_births <- arrange_popn(aged_w_births)

no_births <- births
no_births$births <- 0

aged_w_no_births <- rbind(rename(no_births, popn = births),aged)
aged_w_no_births <- arrange_popn(aged_w_no_births)

# The function being tested is:
#
#popn_age_on <- function(popn,
#                        col_aggregation=c("year","gss_code","age","sex"),
#                        col_age = "age",
#                        col_year = "year",
#                        timestep = 1,
#                        template_age_levels = NULL)
#
# These default values will be used int eh test unless otherwise specified


#--------------------------------------------------------------

test_that("popn_age_on works on a simple population", {
  expect_equivalent(popn_age_on(popn),
                    aged)
})

test_that("popn_age_on works on a simple population with births specified", {
  expect_equivalent(popn_age_on(popn, births=births),
                    aged_w_births)
  
  expect_equivalent(popn_age_on(popn, births=0),
                    aged_w_no_births)
})

test_that("popn_age_on handles age when it's an ordered factor", {
  expect_equivalent(popn_age_on(popn_banded),
                    aged_banded)
  # ... and fails when it's not ordered
  popn_in <- mutate(popn_banded, age = factor(age, levels = c("x","y","z"), ordered = FALSE))
  expect_error(popn_age_on(popn_in))
})

test_that("popn_age_on is happy to validate against template age data", {
  expect_equivalent(popn_age_on(popn, template_age_levels = 20:22),
                    aged)
  expect_equivalent(popn_age_on(popn_banded, template_age_levels = c("x","y","z")),
                    aged_banded)
  expect_equivalent(popn_age_on(popn_banded, template_age_levels = factor(c("x","y","z"))),
                    aged_banded)
})

test_that("popn_age_on handles factors, tibbles and groups", {
  # CASE: age is numeric
  # Factors
  popn_in  <- mutate(popn, gss_code=as.factor(gss_code), sex=as.factor(sex))
  aged_out <- mutate(aged, gss_code=as.factor(gss_code), sex=as.factor(sex))
  expect_equivalent(popn_age_on(popn_in),
                    aged_out)
  
  # Tibbles
  popn_in  <- tibble::as_tibble(popn_in)
  aged_out <- tibble::as_tibble(aged_out)
  expect_equivalent(popn_age_on(popn_in),
                    aged_out)
  
  # Groups
  popn_in  <- group_by(popn_in,  year, gss_code, age, sex)
  aged_out <- group_by(aged_out, year, gss_code, age, sex)
  expect_equivalent(popn_age_on(popn_in),
                    aged_out)
  
  # CASE: age is factor
  # Factors
  popn_in  <- mutate(popn_banded, gss_code=as.factor(gss_code), sex=as.factor(sex))
  aged_out <- mutate(aged_banded, gss_code=as.factor(gss_code), sex=as.factor(sex))
  expect_equivalent(popn_age_on(popn_in),
                    aged_out)
  # Tibbles
  popn_in  <- tibble::as_tibble(popn_in)
  aged_out <- tibble::as_tibble(aged_out)
  expect_equivalent(popn_age_on(popn_in),
                    aged_out)
  # Groups
  popn_in  <- group_by(popn_in,  year, gss_code, age, sex)
  aged_out <- group_by(aged_out, year, gss_code, age, sex)
  expect_equivalent(popn_age_on(popn_in),
                    aged_out)
})

test_that("popn_age_on warns when factor levels don't match the input, and throws an error when it's the age column", {
  popn_in  <- mutate(popn, gss_code=factor(gss_code, levels = c("a","b","c","d")))
  aged_out <- mutate(aged, gss_code=factor(gss_code, levels = c("a","b","c","d")))
  expect_equivalent(popn_age_on(popn_in), aged_out)
  
  popn_in  <- mutate(popn_banded, age=factor(age, levels = c("w","x","y","z")))
  expect_error(popn_age_on(popn_in))
  
  template_age_in <- factor(c("x","y","z"), levels = c("w","x","y","z"))
  expect_error(popn_age_on(popn, template_age_levels = template_age_in))
})

test_that("popn_age_on throws an error with an empty input", {
  popn_in <- popn[NULL,]
  expect_error(popn_age_on(popn_in))
})

test_that("popn_age_on works with multiple data columns, including text data", {
  
  popn_in  <- mutate(popn, popn2 = popn)
  aged_out <- mutate(aged, popn2 = popn)
  expect_equivalent(popn_age_on(popn_in,
                                col_data = c("popn","popn2")),
                    aged_out)
})

test_that("popn_age_on handles custom column names", {
  popn_in  <- rename(popn, xyear=year, xgss_code=gss_code, xage=age, xsex=sex, xpopn=popn)
  aged_out <- rename(aged, xyear=year, xgss_code=gss_code, xage=age, xsex=sex, xpopn=popn)
  expect_equivalent(popn_age_on(popn_in,
                                col_aggregation = c("xyear","xgss_code","xage","xsex"),
                                col_age = "xage",
                                col_year = "xyear",
                                col_data = "xpopn",
                                col_geog = "xgss_code"),
                    aged_out)
})

test_that("popn_age_on doesn't care about the order of col_aggregation", {
  expect_equivalent(popn_age_on(popn, col_aggregation = c("sex","age","gss_code","year")),
                    aged)
})

test_that("popn_age_on throws an error with explicit missing aggregation values", {
  popn_in <- popn
  popn_in$gss_code[1] <- NA
  expect_error(popn_age_on(popn_in))
})

test_that("popn_age_on throws an error with implicit missing aggregation values", {
  popn_in <- popn[-1,]
  expect_error(popn_age_on(popn_in))
})

test_that("popn_age_on throws an error when there's only one age level in the input", {
  popn_in <- expand.grid( year = 2000, gss_code=c("a","b","c"), age=20, sex=c("f","m"), popn = 100, stringsAsFactors = FALSE)
  expect_error(popn_age_on(popn_in))
})

test_that("popn_age_on throws an error with duplicate aggregation values", {
  popn_in <- select(popn, -sex)
  expect_error(popn_age_on(popn_in, col_aggregation = c("year","gss_code","age")))
})

test_that("popn_age_on works on data without a year column", {
  popn_in  <- select(popn, -year)
  aged_out <- select(aged, -year)
  expect_equivalent(popn_age_on(popn_in, col_aggregation = c("gss_code","age","sex"), col_year = NULL),
                    aged_out)
})

test_that("popn_age_on doesn't modify the year when col_year is NULL", {
  aged_out <- mutate(aged, year = 2000)
  expect_equivalent(popn_age_on(popn, col_aggregation = c("year","gss_code","age","sex"), col_year = NULL),
                    aged_out)
})

test_that("popn_age_on can handle custom timesteps when the age column is a factor)", {
  aged_out <- mutate(aged_banded, year = 2010)
  expect_equivalent(popn_age_on(popn_banded, timestep = 10),
                    aged_out)
  
  aged_out <- mutate(aged_banded, year = 2010.5)
  expect_warning(temp <- popn_age_on(popn_banded, timestep = 10.5))
  expect_equivalent(temp, aged_out)
  
  expect_error(popn_age_on(popn_banded, timestep = 0))
  expect_error(popn_age_on(popn, timestep = 10))
})

test_that("popn_age_on can handle custom timesteps when the age column isn't a factor)", {
  popn_in  <- mutate(popn, age = age * 2)
  aged_out <- mutate(aged, age = age * 2, year = 2002)
  expect_equivalent(popn_age_on(popn_in, timestep = 2),
                    aged_out)
  
  # Throws an error when age input doesn't match the timestep
  expect_error(popn_age_on(popn, timestep = 2))
})

test_that("popn_age_on can handle multiple years of data)", {
  popn_in  <- popn %>% 
    mutate(year = year + 1,
           popn = popn * 2) %>% 
    rbind(popn)
  aged_out <-  aged %>% 
    mutate(year = year + 1,
           popn = popn*2) %>% 
    rbind(aged) %>%
    arrange(year, gss_code, age, sex)
  
  expect_equivalent(popn_age_on(popn_in),
                    aged_out)
  
  #with births
  births_in <- births %>% 
    mutate(year = year + 1,
           births = births * 2) %>%
    rbind(births)
  
  aged_out <- rbind(aged_out, rename(births_in, popn = births)) %>%
    arrange(year, gss_code, age, sex)
  
  expect_equivalent(popn_age_on(popn_in, births=births_in),
                    aged_out)
  
})

#-------------------------------------------------------------------------------

#small area data

small_area_popn <- data.frame(gss_code = c(rep("a",3),rep("b",3),rep("c",4)),
                              gss_code_ward = c("l","m","o","p","q","r","s","t","u","v")) %>% 
  left_join(popn, by = "gss_code")

small_area_births <- small_area_popn %>% 
  mutate(age = 0) %>% 
  mutate(births = 15,
         year = 2001) %>% 
  select(-popn) %>% 
  unique() %>% 
  select(gss_code, gss_code_ward, year, age, sex, births)

small_area_out <- small_area_popn %>% 
  mutate(age = ifelse(age == 22, 22, age + 1),
         year = year + 1) %>% 
  group_by(gss_code, gss_code_ward, year, age, sex) %>% 
  summarise(popn = sum(popn), .groups = 'drop_last') %>% 
  data.frame()

small_area_out_births <- rename(small_area_births, popn = births) %>% 
  rbind(small_area_out) %>% 
  arrange(gss_code_ward, age, sex)

small_area_wrong <- small_area_popn %>% 
  mutate(gss_code_ward = ifelse(gss_code_ward == "l", "z", gss_code_ward))

test_that("popn_age_on can handle small area data - errors)", {
  
  expect_error(popn_age_on(small_area_popn))
  
  expect_error(popn_age_on(small_area_popn,
                           col_aggregation = c("year","gss_code","sex","age")))
  
})


test_that("popn_age_on can handle small area data - errors)", {
  
  expect_error(popn_age_on(small_area_wrong,
                             col_aggregation = c("year","gss_code_ward","sex","age"),
                             births = small_area_births,
                             col_geog = "gss_code_ward"))
})


test_that("popn_age_on can handle small area data - normal operation", {
  
  expect_equivalent(popn_age_on(small_area_popn,
                                col_aggregation = c("year","gss_code_ward","sex","age")),
                    select(small_area_out, -gss_code))
  
  expect_equivalent(popn_age_on(small_area_popn,
                                col_aggregation = c("year","gss_code_ward","sex","age"),
                                births = small_area_births),
                    select(small_area_out_births, -gss_code))
  
  expect_equivalent(popn_age_on(small_area_popn,
                                col_aggregation = c("year","gss_code_ward","sex","age"),
                                births = small_area_births,
                                col_geog = "gss_code_ward"),
                    select(small_area_out_births, -gss_code))
  
  
})
