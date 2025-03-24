#' Check one population is contained within another
#'
#' The function checks that every row of one population can be mapped to a row
#' in another, given common aggregation columns (e.g. age, geography). It is
#' designed to confirm that joins between population datasets, are going to
#' behave as you expect, and will throw errors if the mapping between datasets
#' is ambiguous or incomplete, or doesn't perform according to other optional
#' restrictions.
#'
#' By default the function just checks that one population's aggregation levels
#' are contained within another. The option \code{aggregation_levels_match} requires the
#' levels to match exactly (ignoring duplicates). Turning off \code{one2many}
#' requires each row in \code{pop1} to map to exactly one level in \code{pop2} -
#' use this when you expect a left join to return the same number of rows as the
#' input. Turning off \code{many2one} requires levels in \code{pop2} to map to
#' at most one level in \code{pop1}, use this when you want to confirm a unique
#' mapping from \code{pop1} into code{pop2}.
#'
#' The function does not do a full validation of the population data: use
#' \code{validate_population} for that.
#'
#' @param pop1,pop2 Data frames.
#' @param cols_common_aggregation A character vector. Names of columns for data
#'   aggregation (e.g. age, ward). If columns don't have the same name, use a
#'   named character vector (e.g. \code{"WARD"="WARD11CD"}). All elements must give
#'   columns in \code{popn} but not all need to be in \code{popn_rate}, that is,
#'   it can be at a lower resolution. Defaults to
#'   \code{c("year","gss_code","age","sex")}.
#' @param aggregation_levels_match Logical. When TRUE \code{pop1} is allowed to match to
#'   subset of \code{pop2}.When FALSE the aggregation levels should be the same
#'   (ignoring duplicates) and the mapping will be subjective, i.e. all
#'   aggregation levels must be present in both data frames. Default TRUE.
#' @param many2one Logical. Can multiple rows in \code{pop1} map to a single row
#'   in \code{pop2}? Set to FALSE when matching should be unique (i.e.
#'   injective), and leave TRUE when, e.g. \code{pop2} has fewer aggregation
#'   levels than \code{pop2}, for example when validating a join from LSOA data
#'   to MSOA data on MSOA11CD.
#' @param one2many Logical. Can one row in the first population map to several
#'   in the second? Set to FALSE when testing a left join that expects the same
#'   number of rows out. Default TRUE.
#' @param warn_unused_shared_cols Logical. When TRUE a warning will be thrown
#'   when the two input data frames share column names not in
#'   \code{cols_common_aggregation}. Default TRUE.
#'
#' @return \code{pop1}, invisibly (stopping on failure).
#'
#' @importFrom assertthat assert_that
#' @export

validate_join_population <- function(pop1,
                                     pop2,
                                     cols_common_aggregation = c("year","gss_code","age","sex"),
                                     aggregation_levels_match = TRUE,
                                     many2one = TRUE,
                                     one2many = TRUE,
                                     warn_unused_shared_cols = TRUE) {

  validate_join_population_inputs(pop1, pop2, cols_common_aggregation,
                                  aggregation_levels_match, many2one, one2many,
                                  warn_unused_shared_cols)

  cols_common_aggregation <- .convert_to_named_vector(cols_common_aggregation)

  # swap names and values for joining from pop2 to pop1
  cols_common_aggregation_reverse <- names(cols_common_aggregation)
  names(cols_common_aggregation_reverse) <- cols_common_aggregation

  # warn if there are duplicated column names that *aren't* being joined on
  unjoined_pop1 <- setdiff(names(pop1), names(cols_common_aggregation))
  unjoined_pop2 <- setdiff(names(pop2), cols_common_aggregation)

  shared_unjoined_intersection <- intersect(unjoined_pop1, unjoined_pop2)
  if(warn_unused_shared_cols & length(shared_unjoined_intersection) > 0) {
    warning(paste(c("Inputs to validate_join_population both contained", shared_unjoined_intersection,
                  "column(s), but they were not specified as part of the join.",
                  "A join output will contain these columns with .x and .y suffixes to the column"), collapse=" "))
  }
  shared_unjoined_pop2  <- intersect(unjoined_pop2, names(cols_common_aggregation))
  if(length(shared_unjoined_pop2) > 0) {
    warning(paste("Inputs to validate_join_population are joining from column", shared_unjoined_pop2,
                  "but this is (separately) a column in the pop2 input. Output will include this column with a .y suffix"))
  }

  # cut down to only the columns we're interested in
  test_pop1 <- dplyr::select(as.data.frame(pop1), names(cols_common_aggregation))
  test_pop2 <- dplyr::select(as.data.frame(pop2), unname(cols_common_aggregation))

  # convert aggregation columns to factors with common levels
  # warn when factor levels don't match
  for(i in 1:length(cols_common_aggregation)) {
    var1 <- names(cols_common_aggregation)[i]
    var2 <- cols_common_aggregation[i]
    if(is.factor(test_pop1[[var1]]) && is.factor(test_pop1[[var2]])) {
      if(!setequal(levels(test_pop1[[var1]]), test_pop2[[var2]])) {
        warning(paste("validate_join_population was given populations with different factor levels in the",
                      var1,"-",var2,"join. The data will be converted to a character vector."))
      }
    }
    agg_levels <- sort(union(test_pop1[[var1]], test_pop2[[var2]]))
    test_pop1[[var1]] <- factor(test_pop1[[var1]], levels=agg_levels)
    test_pop2[[var2]] <- factor(test_pop2[[var2]], levels=agg_levels)
  }

  assert_that(ncol(test_pop1) == ncol(test_pop2))
  #test_pop2 <- match_factors(test_pop1, test_pop2, cols_common_aggregation)

  # CHECK every level in pop1 maps to a level in pop2
  n_missing_levels <- nrow(test_pop1) - nrow(dplyr::semi_join(test_pop1, test_pop2, by=cols_common_aggregation))
  assert_that(n_missing_levels == 0,
             msg = paste("validate_join_population couldn't match", n_missing_levels,
                         "levels from the source to the target data frame"))

  # CHECK (optional) every level in pop2 also maps to a level in pop1
  if(!aggregation_levels_match) {
    n_missing_levels <- nrow(test_pop2) - nrow(dplyr::semi_join(test_pop2, test_pop1, by=cols_common_aggregation_reverse))
    assert_that(n_missing_levels == 0,
               msg = paste("validate_join_population couldn't match all levels between inputs.",
                           n_missing_levels,"are present in the second data frame that aren't mapped to from the first.",
                           "\nIf it's ok that the first is a subset of the second, set aggregation_levels_match = TRUE"))
  }

  # CHECK (optional) every level in pop2 matches at most one level in pop1
  if(!many2one) {
    pop2_trim_to_pop1 <- dplyr::semi_join(test_pop2, test_pop1, by=cols_common_aggregation_reverse )
    assert_that(nrow(pop2_trim_to_pop1) == nrow(dplyr::left_join(pop2_trim_to_pop1, test_pop1, by=cols_common_aggregation_reverse)),
                msg = "validate_join_population detected several levels in the first population dataset matching to a single level in the second. If this is ok set many2one = TRUE")
  }

  # CHECK (optional) every level in pop1 matches to exactly one level in pop2
  if(!one2many) {
    assert_that(nrow(test_pop1) == nrow(dplyr::left_join(test_pop1, test_pop2, by=cols_common_aggregation)),
                msg = "validate_join_population detected levels in the first population dataset with several matches in the second. If this is ok set one2many = TRUE")
  }

  invisible(pop1)
}



#--------------------------------------------------------------------------------

# Type match the inputs to validate_join_population and check named columns are present
validate_join_population_inputs <-function(pop1,
                                           pop2,
                                           cols_common_aggregation,
                                           aggregation_levels_match,
                                           many2one,
                                           one2many,
                                           warn_unused_shared_cols) {


  assert_that(is.data.frame(pop1),
              is.data.frame(pop2),
              msg="validate_join_population needs data frames as inputs for pop1 and pop2")
  assert_that(is.character(cols_common_aggregation),
              msg="validate_join_population needs a character vector of common aggregation column names as input")
  assert_that(length(cols_common_aggregation) > 0,
              msg="validate_join_population was given a zero-length vector of common column names to compare")
  assert_that(!any(is.na(cols_common_aggregation)),
              msg="validate_join_population can't handle missing in common aggreagation column names")
  assert_that(is.logical(aggregation_levels_match),
              msg="validate_join_population needs a logical value for the aggregation_levels_match input parameter")
  assert_that(is.logical(many2one),
              msg="validate_join_population needs a logical value for the many2one input parameter")
  assert_that(is.logical(one2many),
              msg="validate_join_population needs a logical value for the one2many input parameter")
  assert_that(is.logical(warn_unused_shared_cols),
              msg="validate_join_population needs a logical value for the warn_unused_shared_cols input parameter")

  if(nrow(pop1) == 0) {
    warning("An empty data frame was passed to validate_join_population as parameter pop1")
  }
  if(nrow(pop2) == 0) {
    warning("An empty data frame was passed to validate_join_population as parameter pop2")
  }

  cols_common_aggregation <- .convert_to_named_vector(cols_common_aggregation)

  # Check all expected columns are present
  assert_that(all(names(cols_common_aggregation) %in% names(pop1)),
              msg=paste(c("validate_join_population couldn't find all expected column names in the input for the pop1 parameter.",
                          "\nExpected:",names(cols_common_aggregation),
                          "\nNames of input data frame:",names(pop1)),
                        collapse = " "))
  assert_that(all(cols_common_aggregation %in% names(pop2)),
              msg=paste(c("validate_join_population couldn't find all expected column names in the input for the pop2 parameter.",
                          "\nExpected:",cols_common_aggregation,
                          "\nNames of input data frame:",names(pop2)),
                        collapse = " "))

  # Check no missing values
  if(any(!complete.cases(dplyr::select(as.data.frame(pop1), names(cols_common_aggregation))))) {
    warning("validate_join_population was given NA values in the aggregation levels of pop1")
  }
  if(any(!complete.cases(dplyr::select(as.data.frame(pop2), unname(cols_common_aggregation))))) {
    warning("validate_join_population was given NA values in the aggregation levels of pop2")
  }


  invisible(TRUE)
}

