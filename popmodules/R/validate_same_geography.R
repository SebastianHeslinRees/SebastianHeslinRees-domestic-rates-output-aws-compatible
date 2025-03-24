#' Confirm two datasets have matching geography
#'
#' Performs a check that the unique set of gss_codes in one  dataframe is the
#' same as the unique set of gss_codes in another
#'
#' @param df_1 A dataframe containing a column with geography codes or names
#' @param df_2 A dataframe containing a column with geography codes or names
#' @param col_1 Character. The name of the column to be tested in df_1
#'   (default 'gss_code')
#' @param col_2 Character. The name of the column to be tested in df_2
#'   (default to same as col_1)
#' @param warn_only Logical. Should the function return a warning or an error.
#'   If set to TRUE the function returns a warning, set to FALSE returns an error.
#'   Default FALSE. 
#' @param nested_geog Logical. Should the nested geography be tested to ensure there
#'  is a many-to-one relationship between lower and higher geographies.
#'   
#' @return Warnings or errors if there differences in the gss codes supplied
#'
#' @import dplyr
#' @importFrom tidyselect all_of
#' @importFrom assertthat assert_that
#' 
#' @export

validate_same_geog <- function(df_1, df_2, col_1="gss_code", col_2=col_1,
                               warn_only = FALSE,
                               nested_geog = FALSE){
  #validate function inputs
  assert_that(is.logical(warn_only),
              msg = paste0("in validte_same_geog, param warn_only is '",
                           warn_only, "' - must be 'TRUE' or 'FALSE'"))
  assert_that(all(col_1 %in% names(df_1)),
              msg = paste0("in validte_same_geog, col_1 not in df_1"))
  assert_that(all(col_2 %in% names(df_2)),
              msg = paste0("in validte_same_geog, col_2 not in df_2"))
  assert_that(length(col_1) == length(col_2),
              msg = "in validate_same_grography, col_1 and col_2 must be the same length")
  
  nm_1 <- deparse(substitute(df_1))
  nm_2 <- deparse(substitute(df_2))
  
  #check that the geography columns contain the same set of codes
  for(i in 1:length(col_1)){
    are_equal <- setequal(df_1[[col_1[i]]], df_2[[col_2[i]]])
    
    #if they don't then output useful info about the differences
    if(!are_equal){
      
      df_1_report <- df_1[all_of(col_1)] %>% unique() %>% rename("gss_code___" = col_1[i])
      df_2_report <- df_2[all_of(col_2)] %>% unique() %>% rename("gss_code___" = col_2[i])
      
      .validate_same_geog_report(nm_1, nm_2, df_1_report, df_2_report, warn_only)
      
    }
  }
  
  if(nested_geog){
    
    .validate_nested_geog(df_1, col_1, nm_1, warn_only)
    .validate_nested_geog(df_2, col_2, nm_2, warn_only)
    
  }
  
  invisible()
}
#-----------------

.validate_same_geog_report <- function(nm_1, nm_2, df_1, df_2, warn_only){
  
  x <- setdiff(df_1$gss_code___, df_2$gss_code___)
  
  #-----------------
  
  if(length(x)>0 && warn_only){
    warning(paste(length(x),"codes in",nm_1,"not in",nm_2, ": " , paste(x, collapse = ", ")))
  }
  
  if(length(x)>0 && !warn_only){
    stop(paste(length(x),"codes in",nm_1,"not in",nm_2, ": " , paste(x, collapse = ", ")))
  }
  
  #-----------------
  
  y <- setdiff(df_2$gss_code___, df_1$gss_code___)
  
  if(length(y)>0 && warn_only){
    warning(paste(length(y),"codes in",nm_2,"not in",nm_1, ": " , paste(y, collapse = ", ")))
  }
  
  if(length(y)>0 && !warn_only){
    stop(paste(length(y),"codes in",nm_2,"not in",nm_1, ": " , paste(y, collapse = ", ")))
  }
  
}


.validate_nested_geog <- function(x, nested_cols, nm, warn_only){
  
  y <- x %>% 
    select(all_of(nested_cols)) %>% 
    distinct() %>% 
    group_by(across(!!nested_cols[2])) %>% 
    summarise(n = n())
  
  not_unique <- y[y$n > 1,]
  codes <- not_unique[[nested_cols[2]]]
  
  if(nrow(not_unique)>0){
    
    nested_msg <- paste0("Nested geography in dataframe ", nm, ": ", nrow(not_unique), " ",
                         nested_cols[2], " codes appear in more than 1 ", nested_cols[1],
                         ". Check codes: ", paste(codes, collapse = ", "))
    
    if(warn_only){
      warning(nested_msg)
    } else {
      stop(nested_msg)
    }
    
  }
}