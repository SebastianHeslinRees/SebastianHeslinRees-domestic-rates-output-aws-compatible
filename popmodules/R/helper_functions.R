# ===========================================
#
# A place for reusable code internal to the
# popmodules package. For any short functions
# that are reused across the package but which
# aren't exported. By convention function names
# should start with a '.'
#
# ===========================================


# Function: convert character vector (unnamed or partially named) to one where
# every element is named
#
# e.g. c("a", two="b", "c") will become c(a="a", two="b", c="c")
#
# Used repeatedly in the package, usually to standardise mappings between data
# frame columns to allow for reliable joins
.convert_to_named_vector <- function(vec) {
  assert_that(is.vector(vec) | is.factor(vec),
              msg = ".convert_to_named_vector needs a vector or a factor as input")
  
  if (identical(NA, vec)) return(vec)
  
  if(is.null(names(vec))) {
    names(vec) <- vec
  } else {
    ix <- names(vec) == ""
    names(vec)[ix] <- vec[ix]
  }
  
  return(vec)
}


# ------------------------------------------------------------------------------------

# Function:  copy the factor structure of one data frame to another

# Given source and target data frames and a mapping between common columns, find
# which columns in the first data frame are(n't) factors and convert columns in
# the second data frame to match (preserving factor ordering)

# Returns the *target* data frame, i.e. the second input parameter. Watch out
# (sorry).

# The function currently ignores factor levels (i.e. doesn't try to preserve them
# if they've changed), and doesn't check whether the input factor was ordered or not

# Used in the package to make sure that the output of various functions
# preserves the factoring of the input

.match_factors <- function(dfsource, dftarget, col_mapping) {
  col_mapping <- .convert_to_named_vector(col_mapping)
  for(i in  seq_along(col_mapping)) {
    icol <- col_mapping[i]
    source_col <- dfsource[[names(icol)]]
    target_col <- dftarget[[icol]]
    
    if(is.factor(source_col) & !is.factor(target_col)) {
      if(setequal(levels(source_col), as.character(dftarget[[icol]]))) {
        dftarget[[icol]] <- factor(dftarget[[icol]])
      } else {
        warning(paste(".match_factors was given a source and target with different levels in",
                      col_mapping[i],"- a factor conversion will not be performed"))
      }
    }
    
    if(!is.factor(source_col) & is.factor(target_col)) {
      col_class <- class(source_col)
      if(col_class == "numeric") {
        dftarget[[icol]] <- as.numeric(levels(target_col)[target_col])
      } else if(col_class == "integer"){
        dftarget[[icol]] <- as.integer(levels(target_col)[target_col])
      } else {
        dftarget[[icol]] <- as.character(target_col)
      }
    }
    
  }
  return(dftarget)
}

#' A function to check if a file path ends with a slash and add one if not
#'
#' @param x A Character string
#'
#' @return The input string with a slash (`/`) on the end
#'
#' @export

.add_slash <- function(x){
  if(substr(x,nchar(x),nchar(x)) != "/"){
    x <- paste0(x,"/")
  }
  return(x)
}

#' So that functions can either be passed the path to some data
#' or a dataframe
#'
#' @param x A string or a dataframe
#' 
#' @import assertthat
#'
#' @return dataframe
#'
#' @export

.path_or_dataframe <- function(x){
  assert_that(is.string(x) | is.data.frame(x),
             msg = paste('.path_or_dataframe: x must be string or dataframe, function was passed', class(x)))
  if(is.string(x)){x <- readRDS(x)}
  return(data.frame(x))
}


#' Select columns in a dataframe and rename the geography column to the generic
#' name 'area_code'
#'
#' @param x A dataframe
#' @param col_geog String. The geography column
#' @param data_col String. The data column. Default NULL
#' @param col_agg String. The column aggregation to select
#'
#' @return dataframe
#'
#' @export

.standardise_df <- function(x, col_geog, data_col=NULL,
                            col_agg = c("year", "gss_code", col_geog, "age", "sex")){
  
  cols <- intersect(c(col_agg, data_col), names(x))
  
  select(x, all_of(cols)) %>% 
    rename(area_code = all_of(col_geog)) %>% 
    return()
}