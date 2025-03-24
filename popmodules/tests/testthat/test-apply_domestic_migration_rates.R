context("apply_domestic_migration_rates")
library(popmodules)
library(testthat)
library(dplyr)


popn        <- expand.grid( year = 2000, gss_code=c("a","b"), sex=c("female","male"), age=20:21, popn = 100, stringsAsFactors = FALSE)
popn_no_sex <- expand.grid( year = 2000, gss_code=c("a","b"),                         age=20:21, popn = 100, stringsAsFactors = FALSE)

mign_rate <- expand.grid( year = 2000, gss_out=c("a","b"), gss_in=c("a","b"), sex=c("female","male"), age=20:21, stringsAsFactors = FALSE) %>%
  dplyr::filter( gss_out != gss_in) %>%
  dplyr::mutate( rate = ifelse(gss_out == "a", 0.2, 0.1))

mign_rate_no_sex <- unique(dplyr::select(mign_rate, -sex))

# outputs to match inputs above
mign_out <- expand.grid( year = 2000, gss_out=c("a","b"), sex=c("female","male"), age=20:21, gss_in=c("a","b"), stringsAsFactors = FALSE) %>%
  dplyr::mutate(flow = ifelse(gss_out == "a", 20, 10)) %>%
  dplyr::filter(gss_out != gss_in) %>%
  dplyr::arrange(age, sex, gss_out)
mign_out_no_sex <- unique(dplyr::select(mign_out, -sex))



#--------------------------------------------------------------
# The tests here use expect_equivalent. This is expect_equal (i.e. objects must be the same) but doesn't compare object attributes
# TODO find out whether the attributes matter, and whether it matters that they don't match

# Function's default parameters are:
#
#apply_domestic_migration_rates <- function(popn,
#                             mign_rate,
#                             col_aggregation = c("year", "gss_code"="gss_out", "sex", "age"),
#                             col_gss_destination = "gss_in",
#                             col_popn = "popn",
#                             col_rate = "rate",
#                             col_flow = "flow",
#                             one2many = TRUE,
#                             many2one = FALSE,
#                             col_origin_destination = NA) {

test_that("apply_domestic_migration_rates creates the expected output", {
  expect_equivalent(apply_domestic_migration_rates(popn,
                                                   mign_rate,
                                                   col_aggregation = c("year", "gss_code"="gss_out", "sex", "age"),
                                                   col_gss_destination = "gss_in",
                                                   col_popn = "popn",
                                                   col_rate = "rate",
                                                   col_flow = "flow",
                                                   one2many = FALSE,
                                                   many2one = FALSE,
                                                   col_origin_destination = NA),
                    mign_out)
  
  # Same as above but with default parameters
  expect_equivalent(apply_domestic_migration_rates(popn, mign_rate),
                    mign_out)
  
  expect_equivalent(apply_domestic_migration_rates(popn, mign_rate, col_origin_destination = c("gss_out","gss_in")),
                    mign_out)
  
  expect_equivalent(apply_domestic_migration_rates(popn_no_sex, mign_rate_no_sex, col_aggregation = c("year", "gss_code"="gss_out", "age")),
                    mign_out_no_sex)
})

test_that("apply_domestic_migration_rates lets you specify output column order", {
  expect_equivalent(apply_domestic_migration_rates(popn,
                                                   mign_rate,
                                                   col_aggregation = c("year", "sex", "age", "gss_code"="gss_out"),
                                                   col_gss_destination = "gss_in"),
                    mign_out[c("year", "sex", "age", "gss_out", "gss_in", "flow")])
})

test_that("apply_domestic_migration_rates lets you migrate to places that aren't in the source data", {
  popn_in <- dplyr::filter(popn, gss_code != "a")
  output_out <- dplyr::filter(mign_out, gss_out != "a")
  expect_equivalent(apply_domestic_migration_rates(popn_in, mign_rate, one2many = FALSE),
                    output_out)
})

test_that("apply_domestic_migration_rates throws an error when col_aggregation names columns not present in input dataframe", {
  #TODO Look into whether these should be errors or whether we want to code in the ability to pass data at differing aggregation levels
  expect_error(apply_domestic_migration_rates(popn, mign_rate_no_sex, col_aggregation = c("year", "gss_code"="gss_out", "sex", "age")))
  expect_error(apply_domestic_migration_rates(popn, mign_rate_no_sex, col_aggregation = c("year", "gss_out", "age")))
})

test_that("apply_domestic_migration_rates doesn't care about the order of aggregation columns and throws errors if you name a column twice", {
  expect_equivalent(apply_domestic_migration_rates(popn, mign_rate, col_aggregation = c("age","sex","gss_code"="gss_out","year")),
                    mign_out[c("age","sex","gss_code"="gss_out","year","gss_in","flow")])
  
  expect_error(apply_domestic_migration_rates(popn, mign_rate, col_aggregation = c("year", "gss_code"="gss_out", "age", "sex", "sex")))
  expect_error(apply_domestic_migration_rates(popn, mign_rate, col_aggregation = c("year", "gss_code"="gss_out", "gss_code"="sex", "age")))
  expect_error(apply_domestic_migration_rates(popn, mign_rate, col_aggregation = c("year", "gss_code"="age", "sex", "age"))) # does this make sense?
})

test_that("apply_domestic_migration_rates handles additional, unused input columns", {
  popn_in <- dplyr::mutate(popn, fillpopn = "fill")  # fillers gonna fill
  mign_in <- dplyr::mutate(mign_rate, fillmign = "fill")
  expect_equivalent(apply_domestic_migration_rates(popn_in, mign_in),
                    mign_out)
})

test_that("apply_domestic_migration_rates handles factors, tibbles and groups", {
  # factors
  popn_in <- dplyr::mutate(popn, gss_code=as.factor(gss_code))
  mign_in <- dplyr::mutate(mign_rate, gss_out=as.factor(gss_out), gss_in=as.factor(gss_in))
  
  output_out <- dplyr::mutate(mign_out, gss_out=as.factor(gss_out))
  expect_equivalent(apply_domestic_migration_rates(popn_in, mign_rate),
                    output_out)
  
  output_out <- dplyr::mutate(mign_out, gss_in=as.factor(gss_in))
  expect_equivalent(apply_domestic_migration_rates(popn, mign_in),
                    output_out)
  
  # tibbles
  popn_in <- dplyr::mutate(popn, gss_code=as.factor(gss_code)) %>% dplyr::as_tibble()
  mign_in <- dplyr::as_tibble(mign_in)
  output_out <- dplyr::mutate(mign_out, gss_out=as.factor(gss_out)) %>% dplyr::as_tibble()
  
  expect_equivalent(apply_domestic_migration_rates(popn_in, mign_rate),
                    output_out)
  
  output_out <- dplyr::mutate(mign_out, gss_in=as.factor(gss_in)) %>% dplyr::as_tibble()
  expect_equivalent(apply_domestic_migration_rates(popn, mign_in),
                    output_out)
  
  output_out <- dplyr::mutate(mign_out, gss_out=as.factor(gss_out), gss_in=as.factor(gss_in)) %>% dplyr::as_tibble()
  expect_equivalent(apply_domestic_migration_rates(popn_in, mign_in),
                    output_out)
  
  # groups
  popn_in <-  dplyr::group_by(popn_in, gss_code)
  mign_in <- dplyr::mutate(mign_rate, gss_out=as.factor(gss_out), gss_in=as.factor(gss_in)) %>%
    dplyr::as_tibble() %>%
    dplyr::group_by(gss_in)
  
  output_out <- dplyr::mutate(mign_out, gss_in=as.factor(gss_in)) %>%
    dplyr::as_tibble() %>%
    dplyr::group_by(gss_in)
  expect_equivalent(apply_domestic_migration_rates(popn, mign_in),
                    output_out)
  
  output_out <- dplyr::mutate(mign_out, gss_out=as.factor(gss_out)) %>%
    dplyr::as_tibble() %>%
    dplyr::group_by(gss_out)
  expect_equivalent(apply_domestic_migration_rates(popn_in, mign_rate),
                    output_out)
  
  output_out <- dplyr::mutate(mign_out, gss_out=as.factor(gss_out), gss_in=as.factor(gss_in)) %>%
    dplyr::as_tibble() %>%
    dplyr::group_by(gss_out, gss_in)
  expect_equivalent(apply_domestic_migration_rates(popn_in, mign_in),
                    output_out)
})

test_that("apply_domestic_migration_rates warns when factor levels don't match the input", {
  popn_in    <- dplyr::mutate(popn,      gss_code=factor(gss_code, levels = c("a","b","c","d")))
  
  expect_warning( temp <- apply_domestic_migration_rates(popn_in, mign_rate))
  expect_equivalent(temp, mign_out) # due to differing factor levels, the output won't have a factor in the gss_code column
  
  mign_in    <- dplyr::mutate(mign_rate, gss_out =factor(gss_out,  levels = c("a","b","c","d")), gss_in=factor(gss_in, levels = c("a","b","c","d")))
  output_out <- dplyr::mutate(mign_out,  gss_in=factor(gss_in, levels = c("a","b","c","d")))
  expect_warning( temp <- apply_domestic_migration_rates(popn, mign_in))
  expect_equivalent(temp, output_out)
})

test_that("apply_domestic_migration_rates warns with an empty input", {
  popn_in <- popn[NULL,]
  mign_in <- mign_rate[NULL,]
  expect_error(apply_domestic_migration_rates(popn_in, mign_rate))
  expect_error(migrate_deomestic(popn, mign_in))
})

test_that("apply_domestic_migration_rates handles arbitrary column names", {
  popn_in <- dplyr::rename(popn, xyear=year, xage=age, xsex=sex, xgss_code=gss_code, xpopn=popn)
  output_out <- dplyr::rename(mign_out, xyear=year, xage=age, xsex=sex)
  expect_equivalent(
    apply_domestic_migration_rates(popn_in, mign_rate, col_aggregation = c("xyear"="year","xgss_code"="gss_out","xsex"="sex","xage"="age"), col_popn = "xpopn"),
    output_out)
  
  mign_in <-  dplyr::rename(mign_rate, xyear=year, xage=age, xsex=sex, xgss_out=gss_out, xgss_in=gss_in, xrate=rate)
  output_out <- dplyr::rename(mign_out, xgss_out=gss_out, xgss_in=gss_in)
  expect_equivalent(
    apply_domestic_migration_rates(popn, mign_in, col_aggregation = c("year"="xyear","gss_code"="xgss_out","sex"="xsex","age"="xage"), col_gss_destination = "xgss_in",  col_rate = "xrate"),
    output_out)
  
  output_out <- dplyr::rename(mign_out, xflow=flow)
  expect_equivalent(
    apply_domestic_migration_rates(popn, mign_rate, col_flow = "xflow"),
    output_out)
})

test_that("apply_domestic_migration_rates warns when the input already has a flow/col_flow column and throws an error when it's an aggregation level", {
  popn_in <- dplyr::mutate(popn, flow = 50)
  expect_error(apply_domestic_migration_rates(popn_in, mign_rate))
  expect_error(apply_domestic_migration_rates(popn, mign_rate, col_flow = "gss_code")) # yes, even though this column won't actually be in the output
})

test_that("apply_domestic_migration_rates handles important data column names duplicated between the population and migration data", {
  popn_in  <- dplyr::rename(popn,      value = popn)
  mign_in  <- dplyr::rename(mign_rate, value = rate)
  output_out <- dplyr::rename(mign_out,  value = flow)
  
  expect_error(apply_domestic_migration_rates(popn_in, mign_rate, col_popn = "value", col_flow = "value"))
  
  expect_equivalent(apply_domestic_migration_rates(popn, mign_in, col_rate = "value", col_flow = "value"),
                    output_out)
  
  expect_error(apply_domestic_migration_rates(popn_in, mign_in, col_popn = "value", col_rate = "value", col_flow = "value"))
  
  popn_in <- dplyr::rename(popn, value = gss_code)
  expect_error(apply_domestic_migration_rates(popn_in, mign_in, col_aggregation = c("year", "value"="gss_out", "sex", "age"), col_rate = "value")) # this needn't be an error but ¯\_(ツ)_/¯
  expect_error(apply_domestic_migration_rates(popn_in, mign_in, col_aggregation = c("year", "value"="gss_out", "sex", "age"), col_flow = "value"))
})

test_that("apply_domestic_migration_rates handles unused columns in the inputs that share names with the other inputs", {
  popn_in <- dplyr::mutate(popn, rate = 0.1)
  mign_in <- dplyr::mutate(mign_rate, popn = 20)
  
  expect_equivalent(apply_domestic_migration_rates(popn_in, mign_in),
                    mign_out)
  
  mign_in <- dplyr::mutate(mign_rate, gss_code = gss_out) # creates identical gss_out, gss_code columns
  expect_equivalent(apply_domestic_migration_rates(popn, mign_in, col_aggregation = c("year", "gss_code"="gss_out", "sex", "age")),
                    mign_out)
})

test_that("apply_domestic_migration_rates throws an error with explicit missing aggregation values", {
  popn_in <- popn
  popn_in$gss_code[1] <- NA
  
  mign_in <- mign_rate
  mign_in$gss_out[1] <- NA
  
  expect_error(apply_domestic_migration_rates(popn_in, mign_rate))
  expect_error(apply_domestic_migration_rates(popn, mign_in))
  
  mign_in <- mign_rate
  mign_in$gss_in[1] <- NA
  
  expect_error(apply_domestic_migration_rates(popn, mign_in))
})

# test_that("apply_domestic_migration_rates throws an error/warning with implicit missing aggregation values", {
#   skip()
#   mign_in <- mign_rate[-1,]
#   output_out <- mign_out[-2,]
#   expect_warning(temp <- apply_domestic_migration_rates(popn, mign_in))
#   expect_equivalent(temp, output_out)
#
#   popn_in <- popn[-1,]
#   expect_error(apply_domestic_migration_rates(popn_in, mign_rate))
# })


test_that("apply_domestic_migration_rates throws an error with duplicate aggregation values", {
  mign_in <- unique(dplyr::select(mign_rate, -age))
  expect_error(apply_domestic_migration_rates(popn, mign_in, col_aggregation = c("year", "gss_code"="gss_out","sex")))
})

test_that("apply_domestic_migration_rates throws an error when the rates are negative or would create a negative population", {
  mign_in <- dplyr::mutate(mign_rate, rate=2)
  expect_error(apply_domestic_migration_rates(popn, mign_in))
  
  mign_in <- dplyr::mutate(mign_rate, rate=-1)
  expect_error(apply_domestic_migration_rates(popn, mign_in))
})

test_that("apply_domestic_migration_rates can join to rate data with more than one extra aggregation level", {
  popn_in <- unique(dplyr::select(popn, -sex))
  output_out <- dplyr::select(mign_out, year, gss_out, age, gss_in, sex, flow) %>%
    dplyr::arrange(age, gss_out, sex)
  expect_equivalent(apply_domestic_migration_rates(popn_in,
                                                   mign_rate,
                                                   col_aggregation = c("year", "gss_code"="gss_out", "age"),
                                                   col_gss_destination = c("gss_in", "sex"),
                                                   col_origin_destination = c("gss_out", "gss_in"),
                                                   one2many = TRUE),
                    output_out)
  
  # but fails when col_origin_destination isn't supplied
  expect_error(apply_domestic_migration_rates(popn_in,
                                              mign_rate,
                                              col_aggregation = c("year", "gss_code"="gss_out", "age"),
                                              col_gss_destination = c("gss_in", "sex"),
                                              col_origin_destination = NA,
                                              one2many = TRUE))
})
  
  test_that("apply_domestic_migration_rates can join to *coarser* rate data with more than one extra aggregation level", {
    skip("I don't think we need the function to do this. The data can be wrangled before it gets passed to the function.")
    popn_in <- unique(dplyr::select(popn, -sex))
    mign_in <- unique(dplyr::select(mign_rate, -age))
    output_out <- dplyr::select(mign_out, year, gss_out, age, gss_in, sex, flow) %>%
      dplyr::arrange(age, gss_out, sex)
    expect_equivalent(apply_domestic_migration_rates(popn_in,
                                                     mign_in,
                                                     col_aggregation = c("year", "gss_code"="gss_out"),
                                                     col_gss_destination = c("gss_in", "sex"),
                                                     col_origin_destination = c("gss_out", "gss_in"),
                                                     one2many = TRUE,
                                                     many2one = TRUE),
                      output_out)
  })
  
  
  test_that("apply_domestic_migration_rates can spot aggregation levels missing entirely from the destination data", {
    mign_in <- dplyr::filter(mign_rate, gss_in == "a")
    expect_error(apply_domestic_migration_rates(popn, mign_in))
    expect_error(apply_domestic_migration_rates(popn, mign_in, col_origin_destination = c("gss_out","gss_in")))
    
    mign_in <- dplyr::mutate(mign_in, gss_extra = "d")
    expect_error(apply_domestic_migration_rates(popn, mign_in, col_gss_destination = c("gss_in", "gss_extra"), col_origin_destination = c("gss_out", "gss_in")))
  })
  
  test_that("apply_domestic_migration_rates can deal with missing levels in the migration data when requested", {
    # We need a bigger migration dataset to test this, since the code fails when
    # there's *no* outmigration for an aggregation level, rather than a missing
    # level
    
    mign_in <- expand.grid( year = 2000, gss_out=c("a","b"), gss_in=c("a","b","c"), sex=c("female","male"), age=20:21, stringsAsFactors = FALSE) %>%
      dplyr::filter( gss_out != gss_in) %>%
      dplyr::mutate( rate = ifelse(gss_out == "a", 0.2, 0.1))
    mign_in <- mign_in[-1, ]
    
    output_out <- expand.grid( year = 2000, gss_out=c("a","b"), sex=c("female","male"), age=20:21, gss_in=c("a","b","c"), stringsAsFactors = FALSE) %>%
      dplyr::mutate(flow = ifelse(gss_out == "a", 20, 10)) %>%
      dplyr::filter(gss_out != gss_in)
    output_out <- output_out[-1,]
    output_out <- dplyr::arrange(output_out, age, sex, gss_out)
    
    expect_equivalent(apply_domestic_migration_rates(popn, mign_in, one2many = TRUE),
                      output_out)
    
    mign_in <- tidyr::crossing(mign_in, fill=c("d","e"))
    output_out <- tidyr::crossing(output_out, fill=c("d","e")) %>%
      dplyr::arrange(age, sex, gss_out, gss_in, fill) %>%
      dplyr::select(year, gss_out, sex, age, gss_in, fill, flow)
    
    
    expect_equivalent(apply_domestic_migration_rates(popn,
                                                     mign_in,
                                                     col_gss_destination = c("gss_in","fill"),
                                                     col_origin_destination = c("gss_out", "gss_in"),
                                                     one2many = TRUE),
                      output_out)
  })
  
  
  test_that("migrate domestic can handle nested geographic data, applying rates to a coarser resolution", {
    skip("TODO implement nesting in the validate and apply rate methods")
    popn_in <- tidyr::crossing(popn, gss_finer = c("d","e"))
    output_out <- tidyr::crossing(output, gss_finer = c("d","e"))
    expect_error(apply_domestic_migration_rates(popn_in, mign_rate, col_aggregation = c("year","gss_code"="gss_out","gss_finer","age","sex")))
    expect_equivalent(apply_domestic_migration_rates(popn_in,
                                                     mign_rate,
                                                     col_aggregation = c("year","gss_code"="gss_out","gss_finer","age","sex"),
                                                     nesting = c("gss_code", "gss_finer")),
                      output_out) 
  })
  