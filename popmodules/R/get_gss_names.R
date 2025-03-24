#' Return a data frame with local authority codes and names
#'
#' @export

get_gss_names <- function(){
  data.frame(readRDS("input_data/lookup/lad18_and_region_code_to_name.rds"))
}




