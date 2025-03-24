#' Re-code LA GSS Codes to a later geography
#'
#' Switches any gss codes to a later vintage
#'
#' Aggregates data where rows with the same gss code are present
#'
#' User must define the column containing the gss_code and the columns
#' which contain data. Data columns must be numeric or integer.
#'
#' @param df_in A data frame containing gss_codes and data.
#' @param col_geog A string. The column which contains gss codes (defaults to
#'   \code{gss_code}).
#' @param data_cols A string or character vector. The column(s) that contain the
#'   data to be aggregated. Defaults to last column of input dataframe.
#' @param fun Character Function to be applied in aggregating data. Either 'sum'
#'   or 'mean'. Default \code{'sum'}.
#' @param recode_to_year Numeric. Conform to geography in which year. Must be one of
#'   \code{2009,2012,2013,2018,2019,2020,2021} (2013 is census geography).
#' @param aggregate_data Logical. If set to true multiple instances of the same
#'   gss code will be aggregated using the function specified in \code{fun} parameter.
#'   Default to \code{TRUE}.
#' @param recode_gla_codes Logical. If set to \code{TRUE} re-codes to the codes
#'  used in the 2018-based (& earlier) projections. This is introduced for backward
#'  compatibility and doesn't correspond to any single vintage year.
#'  Default \code{FALSE}.
#' @param code_changes_path string. file path to the code changes database or NULL
#'  to use the hard-coded code changes database. Degault NULL.
#'
#' @return The input dataframe with gss codes changed and data aggregated
#'
#' @import dplyr
#' @import data.table
#' @importFrom dtplyr lazy_dt
#' @importFrom assertthat assert_that
#' 
#' @export

recode_gss_codes <- function(df_in,
                             col_geog="gss_code",
                             data_cols = last(names(df_in)),
                             fun = "sum",
                             recode_to_year,
                             aggregate_data = TRUE,
                             recode_gla_codes = FALSE,
                             code_changes_path = NULL){
  
  #Possible recode years
  recode_years <- c(2009,2012,2013,2018,2019,2020,2021)
  
  #validate
  .validate_recode_gss_codes(df_in, col_geog, data_cols, recode_years, recode_to_year, fun)
  
  #prepare input dataframe
  df <- df_in %>%
    as.data.frame() %>%
    rename("gss_code" = !!col_geog)
  
  col_aggregation <- setdiff(names(df),data_cols)
  
  #Recode to old codes - for backwards compatibility
  if(recode_gla_codes){
    df <- .recode_gss_to_gla(df, col_geog, col_aggregation, fun)
    return(df)
  }
  
  #prepare recoding
  recode_merges <- list()
  recode_name_changes <- list()
  
  if(is.null(code_changes_path)){
    code_changes <- .district_changes_clean()
  } else {
    code_changes <- readRDS(code_changes_path) %>%
      select(changed_to_code, changed_from_code, year, split, merge)
  }
  
  code_changes <- .ignore_splits(code_changes)
  
  #for each year make the changes a named vector inside a list
  #the x=X is necessary because the vector must not be empty
  for(yr in recode_years){
    
    recode_merges[[yr]] <- filter(code_changes, year == yr,
                                  merge == TRUE)
    
    recode_merges[[yr]] <- c(setNames(as.character(recode_merges[[yr]]$changed_to_code),
                                      recode_merges[[yr]]$changed_from_code), "x"="X")
    
    
    recode_name_changes[[yr]] <- filter(code_changes, year == yr,
                                        merge == FALSE)
    
    recode_name_changes[[yr]] <- c(setNames(as.character(recode_name_changes[[yr]]$changed_to_code),
                                            recode_name_changes[[yr]]$changed_from_code), "x"="X")
  }
  
  recode_years <- recode_years[recode_years <= recode_to_year]
  
  #in a loop so that codes that are changed and then changed again are picked-up
  for(yr in recode_years){
    
    #name changes
    df <- df %>% 
      mutate(gss_code = recode(gss_code, !!!recode_name_changes[[yr]])) 
    
    #merges
    df <- df %>% 
      mutate(gss_code = recode(gss_code, !!!recode_merges[[yr]])) 
    
    if(aggregate_data){
      
      if(fun == "sum"){
        df <- df %>% 
          lazy_dt() %>%
          group_by(across(!!col_aggregation)) %>%
          summarise_all(.funs = sum) %>%
          as.data.frame()
      }
      
      if(fun == "mean"){
        df <- df %>% 
          lazy_dt() %>%
          group_by(across(!!col_aggregation)) %>%
          summarise_all(.funs = mean) %>%
          as.data.frame()
      }
      
    }
  }
  
  df <- as.data.frame(df) %>%
    rename(!!col_geog := "gss_code")
  
  return(df)
  
}



.recode_gss_to_gla <- function(df, col_geog, col_aggregation, fun){
  
  #This is the recoding that was used for the 2018 and earlier projections
  #It is not a set of codes that ever existed simultaneously - its a 
  #lowest common denominator approach
  #Its mostly census (ie 2013 codes) with a couple of differences
  
  recoding <- c("E07000001" = "E06000056",
                "E07000002" = "E06000055",
                "E07000003" = "E06000056",
                "E07000013" = "E06000050",
                "E07000014" = "E06000049",
                "E07000015" = "E06000049",
                "E07000016" = "E06000050",
                "E07000017" = "E06000049",
                "E07000018" = "E06000050",
                "E07000019" = "E06000052",
                "E07000020" = "E06000052",
                "E07000021" = "E06000052",
                "E07000022" = "E06000052",
                "E07000023" = "E06000052",
                "E07000024" = "E06000052",
                "E07000025" = "E06000053",
                "E07000054" = "E06000047",
                "E07000055" = "E06000047",
                "E07000056" = "E06000047",
                "E07000057" = "E06000047",
                "E07000058" = "E06000047",
                "E07000059" = "E06000047",
                "E07000060" = "E06000047",
                "E06000057" = "E06000048",
                "E07000242" = "E07000097",
                "E07000243" = "E07000101",
                "E08000037" = "E08000020",
                "E07000157" = "E06000048",
                "E07000158" = "E06000048",
                "E07000159" = "E06000048",
                "E07000160" = "E06000048",
                "E07000161" = "E06000048",
                "E07000162" = "E06000048",
                "E07000182" = "E06000051",
                "E07000183" = "E06000051",
                "E07000184" = "E06000051",
                "E07000185" = "E06000051",
                "E07000186" = "E06000051",
                "E07000230" = "E06000054",
                "E07000231" = "E06000054",
                "E07000232" = "E06000054",
                "E07000233" = "E06000054",
                "E07000240" = "E07000100",
                "E07000241" = "E07000104")
  
  if(fun == "sum"){
    
    df <- df %>% 
      mutate(gss_code = recode(gss_code, !!!recoding)) %>% 
      lazy_dt() %>% 
      group_by(across(!!col_aggregation)) %>%
      summarise(across(everything(), sum)) %>%
      data.frame() %>%
      rename(!!col_geog := "gss_code")
  }
  
  if(fun == "mean"){
    
    df <- df %>% 
      mutate(gss_code = recode(gss_code, !!!recoding)) %>% 
      lazy_dt() %>% 
      group_by(across(!!col_aggregation)) %>%
      summarise(across(everything(), mean)) %>%
      data.frame() %>%
      rename(!!col_geog := "gss_code")
  }
  
  return(df)
}


# Code changes database cleaned version
.district_changes_clean <- function(){
  
  changes_list <- list(
    data.frame("E06000049","Cheshire East","E07000017","Macclesfield","The Cheshire (Structural Changes) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000050","Cheshire West and Chester","E07000013","Chester","The Cheshire (Structural Changes) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000050","Cheshire West and Chester","E07000016","Ellesmere Port & Neston","The Cheshire (Structural Changes) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000050","Cheshire West and Chester","E07000018","Vale Royal","The Cheshire (Structural Changes) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000051","Shropshire","E07000182","Bridgnorth","The Shropshire (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000051","Shropshire","E07000183","North Shropshire","The Shropshire (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000051","Shropshire","E07000184","Oswestry","The Shropshire (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000051","Shropshire","E07000185","Shrewsbury and Atcham","The Shropshire (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000051","Shropshire","E07000186","South Shropshire","The Shropshire (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000052","Cornwall","E07000019","Caradon","The Cornwall (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000052","Cornwall","E07000020","Carrick","The Cornwall (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000052","Cornwall","E07000021","Kerrier","The Cornwall (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000052","Cornwall","E07000022","North Cornwall","The Cornwall (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000052","Cornwall","E07000023","Penwith","The Cornwall (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000052","Cornwall","E07000024","Restormel","The Cornwall (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000053","Isles of Scilly","E07000025","Isles of Scilly","The Cornwall (Structural Change) Order 2008","E06",2009,FALSE,FALSE),
    data.frame("E06000054","Wiltshire","E07000230","Kennet","The Wiltshire (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000054","Wiltshire","E07000231","North Wiltshire","The Wiltshire (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000054","Wiltshire","E07000232","Salisbury","The Wiltshire (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000054","Wiltshire","E07000233","West Wiltshire","The Wiltshire (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000055","Bedford","E07000002","Bedford","The Bedfordshire (Structural Changes) Order 2008","E06",2009,FALSE,FALSE),
    data.frame("E06000056","Central Bedfordshire","E07000001","Mid Bedfordshire","The Bedfordshire (Structural Changes) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000047","County Durham","E07000054","Chester-le-Street","The County Durham (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000047","County Durham","E07000055","Derwentside","The County Durham (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000047","County Durham","E07000056","Durham","The County Durham (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000047","County Durham","E07000057","Easington","The County Durham (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000047","County Durham","E07000058","Sedgefield","The County Durham (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000047","County Durham","E07000059","Teesdale","The County Durham (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000047","County Durham","E07000060","Wear Valley","The County Durham (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000048","Northumberland","E07000157","Alnwick","The Northumberland (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000048","Northumberland","E07000158","Berwick-upon-Tweed","The Northumberland (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000048","Northumberland","E07000159","Blyth Valley","The Northumberland (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000048","Northumberland","E07000160","Castle Morpeth","The Northumberland (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000048","Northumberland","E07000161","Tynedale","The Northumberland (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000048","Northumberland","E07000162","Wansbeck","The Northumberland (Structural Change) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000049","Cheshire East","E07000014","Congleton","The Cheshire (Structural Changes) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000049","Cheshire East","E07000015","Crewe and Nantwich","The Cheshire (Structural Changes) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E06000056","Central Bedfordshire","E07000003","South Bedfordshire","The Bedfordshire (Structural Changes) Order 2008","E06",2009,FALSE,TRUE),
    data.frame("E07000240","St Albans","E07000100","St Albans","The St Albans and Welwyn Hatfield (Boundary Change) Order 2012","E07",2012,FALSE,FALSE),
    data.frame("E07000241","Welwyn Hatfield","E07000104","Welwyn Hatfield","The St Albans and Welwyn Hatfield (Boundary Change) Order 2012","E07",2012,FALSE,FALSE),
    data.frame("E06000057","Northumberland","E06000048","Northumberland","The Gateshead and Northumberland (Boundary Change) Order 2013","E06",2013,FALSE,TRUE),
    data.frame("E06000057","Northumberland","E08000020","Gateshead","The Gateshead and Northumberland (Boundary Change) Order 2013","E06",2013,TRUE,TRUE),
    data.frame("E07000242","East Hertfordshire","E07000097","East Hertfordshire","The East Hertfordshire and Stevenage (Boundary Change) Order 2013","E07",2013,TRUE,FALSE),
    data.frame("E07000243","Stevenage","E07000097","East Hertfordshire","The East Hertfordshire and Stevenage (Boundary Change) Order 2013","E07",2013,TRUE,TRUE),
    data.frame("E07000243","Stevenage","E07000101","Stevenage","The East Hertfordshire and Stevenage (Boundary Change) Order 2013","E07",2013,FALSE,TRUE),
    data.frame("E08000037","Gateshead","E08000020","Gateshead","The Gateshead and Northumberland (Boundary Change) Order 2013","E08",2013,TRUE,FALSE),
    data.frame("E07000112","Folkestone and Hythe","E07000112","Shepway","Name Change","E07",2018,FALSE,FALSE),
    data.frame("E06000058","Bournemouth, Christchurch and Poole","E07000048","Christchurch","The Bournemouth, Dorset and Poole (Structural Changes) Order 2018","E06",2019,FALSE,TRUE),
    data.frame("E06000059","Dorset","E07000049","East Dorset","The Bournemouth, Dorset and Poole (Structural Changes) Order 2018","E06",2019,FALSE,TRUE),
    data.frame("E06000059","Dorset","E07000050","North Dorset","The Bournemouth, Dorset and Poole (Structural Changes) Order 2018","E06",2019,FALSE,TRUE),
    data.frame("E06000059","Dorset","E07000053","Weymouth and Portland","The Bournemouth, Dorset and Poole (Structural Changes) Order 2018","E06",2019,FALSE,TRUE),
    data.frame("E07000244","East Suffolk","E07000205","Suffolk Coastal","The East Suffolk (Local Government Changes) Order 2018","E07",2019,FALSE,TRUE),
    data.frame("E07000246","Somerset West and Taunton","E07000190","Taunton Deane","The Somerset West and Taunton (Local Government Changes) Order 2018","E07",2019,FALSE,TRUE),
    data.frame("E07000246","Somerset West and Taunton","E07000191","West Somerset","The Somerset West and Taunton (Local Government Changes) Order 2018","E07",2019,FALSE,TRUE),
    data.frame("E06000060","Buckinghamshire","E07000004","Aylesbury Vale","The Buckinghamshire (Structural Changes) Order 2019","E06",2020,FALSE,TRUE),
    data.frame("E06000060","Buckinghamshire","E07000005","Chiltern","The Buckinghamshire (Structural Changes) Order 2019","E06",2020,FALSE,TRUE),
    data.frame("E06000060","Buckinghamshire","E07000006","South Bucks","The Buckinghamshire (Structural Changes) Order 2019","E06",2020,FALSE,TRUE),
    data.frame("E06000060","Buckinghamshire","E07000007","Wycombe","The Buckinghamshire (Structural Changes) Order 2019","E06",2020,FALSE,TRUE),
    data.frame("E06000058","Bournemouth, Christchurch and Poole","E06000028","Bournemouth","The Bournemouth, Dorset and Poole (Structural Changes) Order 2018","E06",2019,FALSE,TRUE),
    data.frame("E06000058","Bournemouth, Christchurch and Poole","E06000029","Poole","The Bournemouth, Dorset and Poole (Structural Changes) Order 2018","E06",2019,FALSE,TRUE),
    data.frame("E06000059","Dorset","E07000051","Purbeck","The Bournemouth, Dorset and Poole (Structural Changes) Order 2018","E06",2019,FALSE,TRUE),
    data.frame("E06000059","Dorset","E07000052","West Dorset","The Bournemouth, Dorset and Poole (Structural Changes) Order 2018","E06",2019,FALSE,TRUE),
    data.frame("E07000244","East Suffolk","E07000206","Waveney","The East Suffolk (Local Government Changes) Order 2018","E07",2019,FALSE,TRUE),
    data.frame("E07000245","West Suffolk","E07000201","Forest Heath","The West Suffolk (Local Government Changes) Order 2018","E07",2019,FALSE,TRUE),
    data.frame("E07000245","West Suffolk","E07000204","St Edmundsbury","The West Suffolk (Local Government Changes) Order 2018","E07",2019,FALSE,TRUE),
    data.frame("E06000061","North Northamptonshire","E07000150","Corby","The Northamptonshire (Structural Changes) Order 2020","E06",2021,FALSE,TRUE),
    data.frame("E06000061","North Northamptonshire","E07000152","East Northamptonshire","The Northamptonshire (Structural Changes) Order 2020","E06",2021,FALSE,TRUE),
    data.frame("E06000061","North Northamptonshire","E07000153","Kettering","The Northamptonshire (Structural Changes) Order 2020","E06",2021,FALSE,TRUE),
    data.frame("E06000061","North Northamptonshire","E07000156","Wellingborough","The Northamptonshire (Structural Changes) Order 2020","E06",2021,FALSE,TRUE),
    data.frame("E06000062","West Northamptonshire","E07000151","Daventry","The Northamptonshire (Structural Changes) Order 2020","E06",2021,FALSE,TRUE),
    data.frame("E06000062","West Northamptonshire","E07000154","Northampton","The Northamptonshire (Structural Changes) Order 2020","E06",2021,FALSE,TRUE),
    data.frame("E06000062","West Northamptonshire","E07000155","South Northamptonshire","The Northamptonshire (Structural Changes) Order 2020","E06",2021,FALSE,TRUE)
  )
  
  changes_df <- lapply(changes_list,
                       FUN = function(x) setnames(x, c("changed_to_code",
                                                       "changed_to_name",
                                                       "changed_from_code",
                                                       "changed_from_name",
                                                       "desc",
                                                       "entity_type",
                                                       "year",
                                                       "split",
                                                       "merge"))) %>% 
    rbindlist() %>% 
    data.frame()
  
  return(changes_df)
  
}

.ignore_splits <- function(x){
  
  #### Splits
  #there have thus far been 2 splits, both in 2013
  #both were minor alterations to boundaries
  #northumberland = 1 bungalow
  #east hertfordshire = part of a row of houses
  #both can effectively be treated as code changes with no tranfser of
  #population.
  #Therefore ignore E08000020 to E06000057
  #and ignore E07000097 to E06000243
  
  x %>% 
    filter(!(changed_from_code == "E08000020" & changed_to_code == "E06000057")) %>%
    filter(!(changed_from_code == "E07000097" & changed_to_code == "E07000243"))
  
}

.validate_recode_gss_codes <- function(df_in, col_geog, data_cols, recode_years, recode_to_year, fun){
  
  assertthat::assert_that(fun %in% c("sum","mean"),
                          msg = "in recode_gss_codes, fun must be sum or mean")
  
  assertthat::assert_that(recode_to_year %in% recode_years,
                          msg=paste("in recode_gss_codes, recode_to_year must be one of",
                                    recode_years))
  
  for(i in length(data_cols)){
    assertthat::assert_that(data_cols[i] %in% names(df_in),
                            msg = paste("in recode_gss_codes, specified data_col", data_cols[i],
                                        "not in input dataframe"))
  }
  
  assertthat::assert_that(col_geog %in% names(df_in),
                          msg = paste("in recode_gss_codes, specified col_geog", col_geog,
                                      "not in input dataframe"))
  
  invisible()
}