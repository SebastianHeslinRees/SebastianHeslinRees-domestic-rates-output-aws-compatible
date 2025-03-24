#' Read in constraint data and place in a list for use in the small area model
#'
#' The constraint_list parameter has 3 elements: 1. the location of a LA-level trend
#' projection; 2. the location of a lookup between the lower and higher geographies;
#' 3. a mapping parameter which maps between the constraint data and the
#' projection data; 4. a list indicating components to constrain. The function returns
#' a list of data frames each containing component constraint data.
#' 
#' @param constraint_list List with 4 elements.
#'  1: constraint_path: String. The path to the output folder of an LA-level
#'    projection run using the GLA trend model
#'  2: apply_lookup_path: String  The path to a lookup from the operational
#'    geography input to the constraining geography
#'  3: make_llokup_path: String. The path to a lookup from the constraint input
#'    dataframe geography to the constraining geography
#'  4 mapping: Character. A vector of column names common to both the constraint
#'    and the projection. This level at which the constraint is applied.
#'    E.g. c("gss_code","year","sex","age")
#'  5: components: A list of logical values indicating whether or not each
#'     component should be constrained. The list must include the following components:
#'     births, deaths, in_migration, out_migration, population
#' @param last_proj_yr Numeric. The final projection year.
#' @param geog_code_col String. The column name of the geographic (gss) codes
#' 
#' @return A list where each element is a data frame containing constraint data.
#' 
#' @import dplyr
#' @import assertthat
#' 
#' @export
#'
#' @examples 
#' \dontrun{
#' get_constraints(list(constraint_path =  "outputs/trend/2020/2020_CH_central_lower_21-09-21_1259/",
#'                      apply_constraint_lookup_path =  "input_data/flexible_area_model/lookups/NUTS2_motion_zone.rds",
#'                      make_constraint_lookup_path = "input_data/flexible_area_model/lookups/NUTS2_hma.rds",
#'                      mapping = c("constraint_area","year","sex","age"),
#'                      components = list(births = T,
#'                                        deaths = T,
#'                                        in_migration = F,
#'                                        out_migration = F,
#'                                        population = T)))
#'                     
#' }

get_constraints <- function(constraint_list, last_proj_yr, geog_code_col){
  
  if(!any(constraint_list$components)){return(constraint_list)}
  
  path <- .add_slash(constraint_list$constraint_path)
  input_constraint_geog <- "gss_code"
  
  .validate_get_constraints(constraint_list, last_proj_yr)
  
  constraint_list$apply_constraint_lookup <- readRDS(constraint_list$apply_constraint_lookup_path) %>% 
    .standardise_df(geog_code_col, col_agg = names(.))
  constraint_list$make_constraint_lookup <- readRDS(constraint_list$make_constraint_lookup_path)
  constraint_filter <- constraint_list$make_constraint_lookup[[input_constraint_geog]]
  
  if(constraint_list$components$population){
    constraint_list$population_constraint <- get_component_from_file(paste0(path,"population.rds"),
                                                                     last_proj_yr) %>% 
      .make_constraint(constraint_filter, input_constraint_geog, constraint_list$mapping,
                       constraint_list$make_constraint_lookup)
  }
  
  if(constraint_list$components$births){
    constraint_list$births_constraint <- get_component_from_file(paste0(path,"births.rds"),
                                                                 last_proj_yr) %>% 
      .make_constraint(constraint_filter, input_constraint_geog, constraint_list$mapping,
                       constraint_list$make_constraint_lookup)
  }
  
  if(constraint_list$components$deaths){
    constraint_list$deaths_constraint <- get_component_from_file(paste0(path,"deaths.rds"),
                                                                 last_proj_yr)  %>% 
      .make_constraint(constraint_filter, input_constraint_geog, constraint_list$mapping,
                       constraint_list$make_constraint_lookup)
  }
  
  if(constraint_list$components$in_migration){
    constraint_list$in_migration_constraint <- rbind(
      get_component_from_file(paste0(path,"int_in.rds"), last_proj_yr) %>%
        rename(inflow = int_in),
      get_component_from_file(paste0(path,"dom_in.rds"), last_proj_yr) %>%
        rename(inflow = dom_in)) %>% 
      .make_constraint(constraint_filter, input_constraint_geog, constraint_list$mapping,
                       constraint_list$make_constraint_lookup)
  }
  
  if(constraint_list$components$out_migration){
    constraint_list$out_migration_constraint <- rbind(
      get_component_from_file(paste0(path,"int_out.rds"), last_proj_yr) %>%
        rename(outflow = int_out),
      get_component_from_file(paste0(path,"dom_out.rds"), last_proj_yr) %>%
        rename(outflow = dom_out))  %>% 
      .make_constraint(constraint_filter, input_constraint_geog, constraint_list$mapping,
                       constraint_list$make_constraint_lookup)
  }
  
  return(constraint_list)
}

.make_constraint <- function(x, codes, input_constraint_geog, mapping, constraint_areas){
  nm <- last(names(x))
  x_out <- filter(x, !!sym(input_constraint_geog) %in% codes) %>% 
    left_join(constraint_areas, by = input_constraint_geog) %>% 
    group_by(across(mapping)) %>% 
    rename(value = !!nm) %>% 
    summarise(!!nm := sum(value), .groups = 'drop_last') %>% 
    data.frame()
  return(x_out)
}

.validate_get_constraints <- function(constraint_list, last_proj_yr){
  
  assert_that(is.list(constraint_list), msg = "In get_constraints: constraint_list parameter must be a list")
  assert_that(is.number(last_proj_yr), msg = "In get_constraints: constraint_list parameter must be a nummber")
  
  assert_that(setequal(names(constraint_list), c("constraint_path", "mapping", "make_constraint_lookup_path","apply_constraint_lookup_path","components")),
              msg = "In get_constraints: names(constraint_list) must be: constraint_path, mapping, components, make_constraint_lookup_path, apply_constraint_lookup_path")
  
  assert_that(is.string(constraint_list$constraint_path), msg = "In get_constraints: constraint_list$constraint_path parameter must be a string")
  assert_that(is.string(constraint_list$make_constraint_lookup_path), msg = "In get_constraints: constraint_list$make_constraint_lookup_path parameter must be a string")
  assert_that(is.string(constraint_list$apply_constraint_lookup_path), msg = "In get_constraints: constraint_list$apply_constraint_lookup_path parameter must be a string")
  
  assert_that(is.character(constraint_list$mapping), msg = "In get_constraints: constraint_list parameter must be a character vector")
  assert_that(is.list(constraint_list$components), msg = "In get_constraints: constraint_list$components parameter must be a list")
  
  assert_that(dir.exists(constraint_list$constraint_path), msg = "In get_constraints: constraint_list$constraint_path directory does not exist")
  assert_that(file.exists(constraint_list$make_constraint_lookup_path), msg = "In get_constraints: constraint_list$make_constraint_lookup_path file does not exist")
  assert_that(file.exists(constraint_list$apply_constraint_lookup_path), msg = "In get_constraints: constraint_list$apply_constraint_lookup_path file does not exist")
  
  
  assert_that(setequal(names(constraint_list$components), c("births", "deaths", "in_migration", "out_migration", "population")),
              msg = "In get_constraints: names(constraint_list$components) must be:  births, deaths, in_migration, out_migration, population")
  
  assert_that(is.logical(constraint_list$components$births), msg = "In get_constraints: constraint_list$components$births parameter must be logical")
  assert_that(is.logical(constraint_list$components$deaths), msg = "In get_constraints: constraint_list$components$deaths parameter must be logical")
  assert_that(is.logical(constraint_list$components$in_migration), msg = "In get_constraints: constraint_list$components$in_migration parameter must be logical")
  assert_that(is.logical(constraint_list$components$out_migration), msg = "In get_constraints: constraint_list$components$out_migration parameter must be logical")
  assert_that(is.logical(constraint_list$components$population), msg = "In get_constraints: constraint_list$components$population parameter must be logical")
  
  constraint_list$constraint_path <- .add_slash(constraint_list$constraint_path)
  
  if(constraint_list$components$births){
    assert_that(file.exists(paste0(constraint_list$constraint_path, "births.rds")),
                msg = paste0("In get_constraints: ", constraint_list$constraint_path, "births.rds file does not exist"))
  }
  
  if(constraint_list$components$deaths){
    assert_that(file.exists(paste0(constraint_list$constraint_path, "deaths.rds")),
                msg = paste0("In get_constraints: ", constraint_list$constraint_path, "deaths.rds file does not exist"))
  }
  
  if(constraint_list$components$in_migration){
    assert_that(file.exists(paste0(constraint_list$constraint_path, "int_in.rds")),
                msg = paste0("In get_constraints: ", constraint_list$constraint_path, "int_in.rds file does not exist"))
    
    assert_that(file.exists(paste0(constraint_list$constraint_path, "dom_in.rds")),
                msg = paste0("In get_constraints: ", constraint_list$constraint_path, "dom_in.rds file does not exist"))
    
  }
  
  if(constraint_list$components$out_migration){
    assert_that(file.exists(paste0(constraint_list$constraint_path, "int_out.rds")),
                msg = paste0("In get_constraints: ", constraint_list$constraint_path, "int_out.rds file does not exist"))
    
    assert_that(file.exists(paste0(constraint_list$constraint_path, "dom_out.rds")),
                msg = paste0("In get_constraints: ", constraint_list$constraint_path, "dom_out.rds file does not exist"))
    
  }
  
  if(constraint_list$components$population){
    assert_that(file.exists(paste0(constraint_list$constraint_path, "population.rds")),
                msg = paste0("In get_constraints: ", constraint_list$constraint_path, "population.rds file does not exist"))
  }
  
  invisible()
}
