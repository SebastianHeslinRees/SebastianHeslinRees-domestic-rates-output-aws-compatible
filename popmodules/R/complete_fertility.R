#' Complete the sex and age of a fertility dataframe
#'
#' #' This function has been deprecated. Use \code{complete_popn_dataframe}.
#'
#' @param fertility A dataframe containing fertility data. Any age column must be numeric.
#' @param population A dataframe containing population data. Must contain sex/age if
#'   fertility contains sex/age.  Any age column must be numeric.
#' @param col_sex_fert A character string with the fertility sex column name.
#'   Takes NULL if there is no sex column. Must be NULL if \code{col_sex_popn} is NULL.
#' @param col_sex_popn A character string with the population sex column name.
#'   Takes NULL if there is no sex column. Must be NULL if \code{col_sex_fert} is NULL.
#'   Defaults to \code{col_sex_fert}.
#' @param col_age_fert A character string with the fertility age column name.
#'   Takes NULL if there is no age column. Must be NULL if \code{col_age_popn} is NULL.
#' @param col_age_popn A character string with the population age column name.
#'   Takes NULL if there is no age column. Must be NULL if \code{col_age_fert} is NULL.
#'   Defaults to \code{col_age_fert}.
#' @param col_rate A character string with the fertility rate column name.
#'
#' @return A fertility dataframe that contains entries for all sexes present in the
#'   population dataframe, and age extended down to the lowest population age and up
#'   to the highest population age.  New entries are created with a rate value of 0.
#'
#' @importFrom assertthat assert_that
#' @importFrom tidyr complete nesting
#' @export

complete_fertility <- function(fertility, population,
                               col_sex_fert = "sex",
                               col_sex_popn = col_sex_fert,
                               col_age_fert = "age",
                               col_age_popn = col_age_fert,
                               col_rate = "rate") {

  .Deprecated("complete_popn_dataframe")
  
  # check fertility and population exist
  force(population)
  force(fertility)
  validate_complete_fertility_inputs(fertility, population,
                  col_sex_fert = col_sex_fert, col_sex_popn = col_sex_popn,
                  col_age_fert = col_age_fert, col_age_popn = col_age_popn,
                  col_rate = col_rate)

  cols_agg_fertility <- setdiff(names(fertility), col_rate)
  fill_list <- list()
  fill_list[col_rate] <- 0

  if (!is.null(col_sex_fert) && # validate_inputs ensures that sex column is present in neither or both
      !all(unique(population[[col_sex_popn]]) %in% unique(fertility[[col_sex_fert]]))) {

    non_sex_cols <- setdiff(cols_agg_fertility, col_sex_fert)
    sexes <- unique(population[[col_sex_popn]])

    fertility <- fertility %>% complete(nesting(!!!syms(non_sex_cols)),
                                               !!sym(col_sex_fert) := sexes,
                                               fill = fill_list) %>%
      as.data.frame()
  }

  if(!is.null(col_age_fert) && # validate_inputs ensures that age column is present in neither or both
     !all(unique(population[[col_age_popn]]) %in% unique(fertility[[col_age_fert]]))) {
    non_age_cols <- setdiff(cols_agg_fertility, col_age_fert)

    age_tails <- unique(population[[col_age_popn]])
    age_tails <- age_tails[age_tails < min(fertility[[col_age_fert]]) |
                             age_tails > max(fertility[[col_age_fert]])]
    ages <- unique(c(age_tails, unique(fertility[[col_age_fert]])))

    fertility <- fertility %>%
      complete(nesting(!!!syms(non_age_cols)), !!sym(col_age_fert) := ages,
                      fill = fill_list) %>%
      as.data.frame()
  }

  fertility

}


validate_complete_fertility_inputs <- function(fertility, population, col_sex_fert, col_sex_popn, col_age_fert, col_age_popn, col_rate) {
  assert_that(is.data.frame(fertility), msg = "fertility must be a dataframe")
  assert_that(is.data.frame(population), msg = "population must be a dataframe")

  assert_that((is.null(col_sex_fert) & is.null(col_sex_popn)) | (!is.null(col_sex_fert) & !is.null(col_sex_popn)), msg = "col_sex must either be NULL in both population and fertility or NULL in neither")
  assert_that((is.null(col_age_fert) & is.null(col_age_popn)) | (!is.null(col_age_fert) & !is.null(col_age_popn)), msg = "col_age must either be NULL in both population and fertility or NULL in neither")

  if (!is.null(col_sex_fert)) assert_that(col_sex_fert %in% names(fertility), msg = paste0("fertility dataframe does not contain '", col_sex_fert, "' column"))
  if (!is.null(col_sex_popn)) assert_that(col_sex_popn %in% names(population), msg = paste0("population dataframe does not contain '",col_sex_popn, "' column"))
  if (!is.null(col_age_fert)) assert_that(col_age_fert %in% names(fertility), msg = paste0("fertility dataframe does not contain '", col_age_fert, "' column"))
  if (!is.null(col_age_popn)) assert_that(col_age_popn %in% names(population), msg = paste0("population dataframe does not contain '",col_age_popn, "' column"))

  assert_that(!is.null(col_rate) & col_rate %in% names(fertility), msg = paste0("fertility dataframe does not contain '",col_rate, "' column"))

  if (!is.null(col_sex_fert)) assert_that(all(unique(fertility[[col_sex_fert]]) %in% unique(population[[col_sex_popn]])), msg = "fertility and population dataframes appear to encode sex differently")
  if (!is.null(col_age_fert)) assert_that(all(unique(fertility[[col_age_fert]]) %in% unique(population[[col_age_popn]])), msg = "fertility and population dataframes appear to encode age differently")
  if (!is.null(col_age_fert)) assert_that(is.numeric(fertility[[col_age_fert]]) & is.numeric(population[[col_age_popn]]))

  validate_population(fertility, col_aggregation = setdiff(names(fertility), col_rate),
                      test_complete = TRUE, test_unique = TRUE, check_negative_values = TRUE)

  invisible(TRUE)
}

