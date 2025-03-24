#' Validate a population dataset
#'
#' Performs common checks to validate a population dataset. The dataset is
#' expected to contain at least one 'aggregation' column (e.g. geography, age,
#' sex), and optionally 'data' columns, (e.g. popn, rate).
#'
#' The function checks for missing values, duplicated aggregation levels, unused
#' aggregation levels (optional), negative data values (optional), and validates
#' the input against a provided 'comparison' input (optional).
#'
#' Currently the function can only process a single geography column - give it
#' the highest resolution contained in the data frame. Any columns not named are
#' ignored.
#'
#' The function will stop code execution with an error message if it finds a
#' problem.
#'
#' @param population A data frame containing population data.
#' @param col_aggregation A string or character vector giving column names for
#'   levels of data aggregations: e.g. geographic area, age/age band, sex. It
#'   will be checked that these behave as a primary key. If there is more than
#'   one geography column (e.g. ward code, ward name, or ward code borough code)
#'   include only the highest resolution and only one column. Default value is
#'   \code{"year", "gss_code", "age", "sex"}.
#' @param col_data A string of character vector giving column names for data
#'   (e.g. popn, birth rate). Columns given will be checked for missing and
#'   negative values. Any columns not named in the input are currently ignored.
#'   Default value is NA.
#' @param test_complete Logical. Check all combinations of aggregation variables
#'   are present. The test will fail when lower geographic resolutions are
#'   present (e.g. borough and LSOA), so make sure these are not passed to
#'   \code{col_aggregation}. Default FALSE.
#' @param test_unique Logical. Check there are no duplicate combinations of any
#'   of the given aggregation levels. Default FALSE.
#' @param check_negative_values Logical value requesting data are checked for
#'   negative values. Defaults to FALSE.
#' @param comparison_pop Optional. A data frame containing all aggregation
#'   levels of the population being tested (e.g. an initial population), so that
#'   they can be checked to match. Default to NA.
#' @param col_comparison A character vector giving the names of the aggregation
#'   levels in the comparison population. If names are different between frames,
#'   provide a named character vector, as you would with by=c() in a join.
#'   Defaults to the value of \code{col_aggregation}, accepts NA.
#' @return The input population, unchanged. Return is invisible, so the function
#'   can be called within a dplyr pipe or as a one-line validity test
#'
#' @importFrom assertthat assert_that
#' @import dplyr
#' @export

validate_population <- function(population,
                                col_aggregation = c("year", "gss_code", "age", "sex"),
                                col_data = NA,
                                test_complete = FALSE,
                                test_unique = FALSE,
                                check_negative_values = FALSE,
                                comparison_pop = NA,
                                col_comparison = col_aggregation) {
  
  #make sure col_aggregation is not a named vector
  col_aggregation <- unname(col_aggregation)
  
  # Check input parameters are legal and make sense
  check_validate_pop_input(population, col_aggregation, col_data, test_complete,
                           test_unique, check_negative_values, comparison_pop, col_comparison)
  
  # drop requested column names that don't exist (the above checks threw the necessary warnings)
  col_aggregation <- col_aggregation[ col_aggregation %in% names(population) ]
  col_data <- col_data[ col_data %in% names(population) ]  # works with col_data = NA
  
  # For some reason things took ages with tibbles, so convert to data frame
  # and cut down to the columns we care about
  test_population <- as.data.frame(population) %>% 
    select(all_of(c(col_aggregation, col_data)))
  
  # CHECK: warn if there are any missing factor levels,
  # for(col in col_aggregation) {
  #   if(is.factor(population[[col]])) {
  #     n_missing_levels <- sum( !levels(population[[col]]) %in% population[[col]])
  #     if(n_missing_levels != 0) {
  #       warning(paste("Column", col, "is a factor with", n_missing_levels,
  #                     "level(s) not present in the input data"))
  #     }
  #   }
  # }
  # 
  col_aggregation %>% 
    lapply(function(col){
      if(is.factor(population[[col]])) {
        n_missing_levels <- sum( !levels(population[[col]]) %in% population[[col]])
        if(n_missing_levels != 0) {
          warning(paste("Column", col, "is a factor with", n_missing_levels,
                        "level(s) not present in the input data"))
        }
      }
    })
  
  # drop unused factor levels
  test_population <- droplevels(test_population)
  
  # CHECK: aggregation levels have no missing values
  col_aggregation %>% 
    lapply(function(col){
      assert_that( !any(is.na(test_population[[col]])),
                   msg=paste("Aggregation column", col, "contains missing values"))
    })
  
  
  # CHECK: warn if data columns have missing values
  if(length(col_data)!=0) {
    
    col_data %>% 
      lapply(function(col){
        if(any(is.na(test_population[[col]]))) {
          warning(paste("validate_population found missing values in the",col,"column"))
        }
      })
  }
  
  # CHECK: no duplicates in input aggregation levels
  n_duplicates <- sum(duplicated(data.table::as.data.table(test_population[col_aggregation])))
  if(test_unique) {
    assert_that(n_duplicates == 0,
                msg=paste("validate_population found", n_duplicates,
                          "duplicated aggregation levels.",
                          "Call with test_unique = FALSE if this is permitted"))
  }
  
  # CHECK: all combinations of aggregation levels are present (optional)
  if(test_complete) {
    aggregation_level_counts <- sapply(col_aggregation, function(x) length(unique(test_population[[x]])))
    n_combinations <- prod(aggregation_level_counts)
    n_missing_levels <- n_combinations - nrow(test_population) + n_duplicates
    assert_that(n_missing_levels == 0,
                msg=paste("validate_population found", n_missing_levels, "missing aggregation levels.",
                          "\nIf this is a large number, check that the col_aggregation parameter only includes one geographic variable, as the test checks for all permutations of all varaibles.",
                          "\nCall with test_complete = FALSE if this is permitted"))
  }
  
  # CHECK: no negative counts in the data (optional)
  if(check_negative_values & length(col_data)!=0) {
    for(col in col_data) {
      if(is.factor(test_population[[col]])) {
        test_col <- as.character(test_population[[col]])
      } else {
        test_col <- test_population[[col]]
      }
      
      test_col <- test_col[!is.na(test_col)]
      assert_that( all( test_col >= 0),
                   msg=paste("validate_population found negative values in column",col,
                             "- call with check_negative_values = FALSE if this is permitted."))
    }
  }
  
  # CHECK: if there's a comparison data frame, make sure the aggregation levels match
  if(!identical(comparison_pop, NA)) {
    
    # drop missing factor levels from the comparison population (we warned about them above)
    comparison_pop <- as.data.frame(droplevels(comparison_pop))
    
    # rename columns in comparison table if necessary
    if(!is.null(names(col_comparison))) {
      names(col_comparison) <- ifelse(names(col_comparison)=="",
                                      col_comparison,
                                      names(col_comparison))
      ix <- match(col_comparison, names(comparison_pop))
      names(comparison_pop)[ix] <- names(col_comparison)
      
      assert_that(!any(duplicated(names(comparison_pop))),
                  msg="Renaming columns in the comparison data frame in validate_population resulted in duplicate column names. This occurs when a column in pop2 isn't an aggregation level but has the same name as an aggregation level in pop1") # check no duplicate names
      
      col_comparison <- names(col_comparison)
      
    }
    
    # after renaming the dataframes, the col_comparison values are redundant.
    
    # convert when necessary to match factored columns
    comparison_pop <- .match_factors(test_population, comparison_pop, col_mapping = col_aggregation)
    
    # get only the unique values in the comparison columns. Unique() is faster on data tables
    comparison_pop <- dplyr::select_at(comparison_pop, col_comparison)
    test_population_comparison <- dplyr::select_at(test_population, col_comparison)
    data.table::setDT(comparison_pop)
    data.table::setDT(test_population_comparison)
    #comparison_pop <- unique(comparison_pop)
    test_population_comparison <- unique(test_population_comparison)
    data.table::setDF(comparison_pop)
    data.table::setDF(test_population_comparison)
    
    # validate the comparison population.
    tryCatch(validate_population(comparison_pop,
                                 col_aggregation = col_comparison,
                                 col_data = NA,
                                 comparison_pop = NA,
                                 test_unique = TRUE,
                                 test_complete = TRUE,
                                 check_negative_values = TRUE),
             error = function(e) stop(paste0("validate_population found an error in the comparison population it was given:\n",e,"\n")),
             warning = function(w) warning(paste0("validate_population threw a warning when checking the given comparison population:\n",w,"\n")))
    
    
    # compare populations. if we can assume completeness we have a shortcut
    if(test_complete) {
      comparison <- all(sapply(col_aggregation, function(x) setequal(test_population_comparison[[x]], comparison_pop[[x]])))
    } else {
      comparison <- dplyr::all_equal(test_population_comparison[col_comparison],
                                     comparison_pop[col_comparison],
                                     ignore_col_order = TRUE, ignore_row_order = TRUE, convert = TRUE)
    }
    if(!isTRUE(comparison)) {
      stop(paste(c(
        "validate_population couldn't match aggregation levels in the input data compared to the provided comparison population",
        "(aggregating over",col_comparison,")"),
        collapse=" "))
    }
    
  }
  
  invisible(population)
}


#--------------------------------------------------------------------

# Function to check that the input to validate_population is all legal

check_validate_pop_input <- function(population,
                                     col_aggregation,
                                     col_data,
                                     test_complete,
                                     test_unique,
                                     check_negative_values,
                                     comparison_pop,
                                     col_comparison) {
  
  # test input parameters are of the correct type
  assert_that(is.data.frame(population),
              msg="validate_population expects a data frame as input")
  assert_that(is.character(col_aggregation),
              msg="validate_population requires a vector of column names for input parameter col_aggregation")
  assert_that(identical(col_data, NA) | is.character(col_data),
              msg="validate_population needs NA or a vector of column names for input parameter col_data")
  assert_that(is.logical(test_complete),
              msg="validate_population requires a logical value for input parameter test_complete")
  assert_that(identical(comparison_pop, NA) | is.data.frame(comparison_pop),
              msg="validate_population requires NA or a data frame for input parameter comparison_pop")
  assert_that(is.logical(check_negative_values),
              msg="validate_population requires a logical value for input parameter check_negative_values")
  assert_that(identical(col_comparison, NA) | is.character(col_comparison),
              msg="validate_population requires NA or a vector of column names for input parameter col_comparison")
  assert_that(is.null(names(col_aggregation)),
              msg = "validate_population cannot take a named vector for col_aggregation")
  col_comparison_test_pop <- names(.convert_to_named_vector(col_comparison))
  assert_that(identical(col_comparison, NA) | all(col_comparison_test_pop %in% col_aggregation),
              msg = "validate_popualation requires that comparison columns are a subset of aggregation columns (or the same)")
  
  # test at least one aggregation variable is present
  missing_cols <- col_aggregation[ !col_aggregation %in% names(population)]
  if(length(missing_cols) > 0) {
    warning(paste("Function validate_population expected column", missing_cols,"- not found", collapse="\n"))
  }
  
  col_aggregation <- col_aggregation[ col_aggregation %in% names(population) ]
  assert_that(length(col_aggregation) > 0,
              msg="validate_population found no valid aggregation columns")
  
  # test data column(s) exist, if given
  if(!identical(col_data, NA)) {
    missing_cols <- col_data[ !col_data %in% names(population)]
    if(length(missing_cols) > 0) {
      warning(paste("Function validate_population expected column", missing_cols,"- not found", collapse="\n"))
    }
    col_data <- col_data[ col_data %in% names(population) ]
  }
  if(identical(col_data, NA) | length(col_data)==0) {
    col_data <- c()
  }
  
  # stop if any input column names are duplicated
  assert_that(!any(duplicated(names(population))),
              msg="validate_population found duplicate column names in the input data frame")
  
  # warn if there's overlap between data and aggregation columns
  if(any(col_aggregation %in% col_data)) {
    col_overlap <- col_aggregation[ col_aggregation %in% col_data ]
    warning(paste("Column",col_overlap, "in both aggregation and data columns provided to validate_population", collapse="\n"))
  }
  
  # warn if population is empty
  if(nrow(population) == 0) {
    warning("Empty data frame passed to validate_population")
  }
  
  invisible(TRUE)
}


