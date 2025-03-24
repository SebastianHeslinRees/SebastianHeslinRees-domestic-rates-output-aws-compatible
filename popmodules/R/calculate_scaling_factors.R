#' Produce scaling factors to convert one population to another
#'
#' Given a cohort population and a data frame of target marginal population
#' subtotals, return a set of scaling factors which, when applied, will scale
#' the input population to match the target at each grouping level.
#'
#' @param popn A data frame containing population data.
#' @param constraint A data frame containing population data at the same
#'   resolution or lower.
#' @param col_aggregation A string or character vector giving the join mapping between
#'   \code{popn} and \code{constraint}. Equivalent to \code{by} in \code{dplyr} joins.
#' @param col_popn String. Name of column in \code{popn} containing population
#'   counts.
#' @param col_constraint String. Name of column in \code{constraint} containing population
#'   counts. Defaults to the value of \code{col_popn}.
#' @param rows_to_constrain Which rows of the input population are to be
#'   constrained. This can be provided as a logical vector (e.g. \code{TRUE} for
#'   all rows or \code{c(TRUE, TRUE, FALSE)} or using dplyr's non-standard
#'   evaluation, e.g. for a population with a \code{gss_code} column,
#'   \code{gss_code == "E090000001"} for City of London, or \code{grepl("^E09",
#'   gss_code)} for all of London. Note this can't be applied to the population
#'   column. Default TRUE.
#'
#' @return A data frame of scaling factors calculated as target population/input population,
#'   with one row for each level of the input population for each distinct level of the
#'   input \code{col_aggregation} columns.
#'
#' @import dplyr
#' @import assertthat
#' @importFrom utils capture.output
#' @importFrom stats setNames
#' @importFrom dtplyr lazy_dt
#'
#' @export

calculate_scaling_factors <- function(popn,
                                      constraint,
                                      col_aggregation = c("year", "sex", "age", "country"),
                                      col_popn,
                                      col_constraint = col_popn,
                                      rows_to_constrain = TRUE) {
  # Standardise data
  # ----------------
  validate_calculate_scaling_factors_input(popn, constraint, col_aggregation, col_popn, col_constraint)

  # Reformat col_aggregation to a named vector mapping between popn columns and constraint columns
  col_aggregation <- .convert_to_named_vector(col_aggregation)

  if(anyDuplicated(data.table::as.data.table(constraint[col_aggregation]))) {
    #warning(paste("calculate_scaling_factors is aggregating the input constraints up to the requested levels.",
    #              "\nAlso, feel free to delete this warning in the source code if we're getting it too often, I'm just curious."))
    constraint <- lazy_dt(constraint) %>%
      group_by(across(unname(!!col_aggregation))) %>%
      summarise(!!sym(col_constraint) := sum(!!sym(col_constraint))) %>%
      ungroup() %>%
      data.frame()
  }

  cols <- names(popn)
  col_overlap <- intersect(names(popn), names(constraint)) %>%
    setdiff(c(col_aggregation, col_popn))
  names(cols) <- ifelse(cols %in% col_overlap, paste0(cols, ".x"), cols)

  # Make sure the columns that are factors match
  constraint <- .match_factors(popn, constraint, col_aggregation)

  # Deal with possible duplicate data column names
  col_popn_new <- paste0(col_popn, ".x")
  col_constraint_new <- paste0(col_constraint, ".y")

  popn <- rename(popn, !!sym(col_popn_new) := !!sym(col_popn))
  constraint <- rename(constraint, !!sym(col_constraint_new) := !!sym(col_constraint))

  scaling <- popn %>%
    mutate(do_scale__ = !!enquo(rows_to_constrain)) %>%
    left_join(constraint, by = col_aggregation) %>%
    group_by(across(names(col_aggregation))) %>%
    mutate(popn_total__ = sum(!!sym(col_popn_new)),
           scaling = ifelse(!do_scale__, 1,
                            ifelse(is.na(!!sym(col_constraint_new)), -999,
                                   ifelse(popn_total__ == 0, 0,
                                          !!sym(col_constraint_new)/popn_total__))),
           mixed_scaling_check = max(do_scale__) == min(do_scale__)) %>%
    as.data.frame()

  assertthat::assert_that(all(scaling$mixed_scaling_check),
                          msg = "calculate_scaling_factors was asked to aggregate to levels containing mixtures of geographies to be included and excluded from the rescaling")

  ix <- scaling$scaling == -999
  if(any(ix)) {
    missing_levels <- filter(scaling, ix) %>%
      select(-!!col_popn_new, -!!col_constraint_new, -popn_total__, -do_scale__, -mixed_scaling_check, -scaling)
    warning(paste0(capture.output({
      print(paste("calculate_scaling_factors: not all levels in the input were matched to levels in the constraint.",
                  "This isn't a problem, but for best practice you should filter these rows out with the rows_to_constrain parameter."))
      print("Levels affected:")
      print(sapply(missing_levels, unique))
    }), sep = "\n"))

    scaling[ix, "scaling"] <- 1
  }

  ix <- scaling$do_scale__ &
    scaling$popn_total__ == 0 &
    !is.na(scaling[[col_constraint_new]]) &
    scaling[[col_constraint_new]] != 0

  if(any(ix)) {
    unable_to_scale <- lazy_dt(scaling) %>%
      filter(ix==TRUE) %>%
      group_by(across(names(!!col_aggregation))) %>%
      summarise(!!col_constraint_new := first(!!sym(col_constraint_new))) %>%
      data.frame()
    
    total_unscaled <- round(sum(unable_to_scale[[col_constraint_new]]),2)
    
    warning(paste0(capture.output({
      print("calculate_scaling_factors was asked to scale populations of zero to non-zero sizes")
      print(paste0(nrow(unable_to_scale), " aggregation levels affected"))
      print(paste0("Target populations sum to ", total_unscaled))
      print("These populations will be unscaled and the output will be short by this many people")
      if(nrow(unable_to_scale) < 30) {
        print("Values:")
        print(unable_to_scale)
      } else {
        print("First 30 values:")
        print(unable_to_scale[1:30,])
      }
    }), collapse = "\n"))
  }

  scaling <- scaling %>%
    rename(!!col_popn := !!sym(col_popn_new)) %>%
    select(!!names(cols), scaling)  %>%
    setNames(c(cols, "scaling")) %>% 
    data.frame()

  return(scaling)
}





# ---------------------------------------------------------------------------


# Check the function input is valid
validate_calculate_scaling_factors_input <- function(popn, constraint, col_aggregation, col_popn, col_constraint) {

  # Type checking
  assert_that(is.data.frame(popn),
              msg = "calculate_scaling_factors needs popn to be a dataframe")
  assert_that(is.data.frame(constraint),
              msg = "calculate_scaling_factors needs constraint data to be a dataframe")
  assert_that(is.character(col_aggregation),
              msg = "calculate_scaling_factors needs the col_aggregation parameter to be a string")
  assert_that(is.string(col_popn),
              msg = "calculate_scaling_factors needs a string as the col_popn parameter")
  assert_that(is.string(col_constraint),
              msg = "calculate_scaling_factors needs a string as the col_constraint parameter")

  col_aggregation <- .convert_to_named_vector(col_aggregation) # convert to named vector mapping between popn and constraint aggregation levels

  assert_that(col_popn %in% names(popn),
              msg="in calculate_scaling_factors col_popn must be a column in popn dataframe")
  assert_that(col_constraint %in% names(constraint),
              msg="in calculate_scaling_factors col_constraint must be a column in constraint dataframe")
  assert_that(!col_popn %in% names(col_aggregation),
              msg = "calculate_scaling_factors was given a population count column name that is also a named aggregation column in the population data frame")
  assert_that(!col_popn %in% col_aggregation,
              msg = "calculate_scaling_factors was given a population count column name that is also a named aggregation column in the constraint data frame")
  assert_that(all(col_aggregation %in% names(constraint)),
              msg = "in calculate_scaling_factors some columns in col_aggregation not found in constraint")
  assert_that(all(names(col_aggregation) %in% names(popn)),
              msg = "in calculate_scaling_factors, all columns named in col_aggregation must be columns in the popn table")
  assert_that(!any(duplicated(names(col_aggregation))),
              msg = "duplicated population column names were provided to calculate_scaling_factors")
  assert_that(!any(duplicated(unname(col_aggregation))),
              msg = "duplicated constraint column names were provided to calculate_scaling_factors")
  assert_that(is.numeric(popn[[col_popn]]),
              msg = paste("calculate_scaling_factors needs a numeric column in the specified population count col:", names(col_popn)))
  assert_that(is.numeric(constraint[[col_constraint]]),
              msg = paste("calculate_scaling_factors needs a numeric column in the specified constraint constraint col:", unname(col_popn)))

  if(min(popn[[col_popn]]) < 0) {
    warning("Calculate scaling factors was called on a population data frame with negative populations.")
  }
  if(min(constraint[[col_constraint]]) < 0) {
    warning("Calculate scaling factors was called with a constraint data frame with negative populations.")
  }
  
  invisible(TRUE)
}
