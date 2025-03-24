#' Load the development versions of the GLA model suite
#' 
#' Run devtools::load_all() on the GLA model packages and popmodules
#' 
#' @param popmodules Logical Load the popmodules package Default \code{TRUE}
#' @param trend Logical Load the trendmodel package Default \code{TRUE}
#' @param housing_led Logical Load the housingledmodel package Default \code{TRUE}
#' @param small_area Logical Load the smallareamodel package Default \code{TRUE}
#' 
#' @importFrom devtools load_all
#' 
#' @export

load_gla_models <- function(popmodules = TRUE, trend = TRUE, housing_led = TRUE, small_area = TRUE){
  
  if(popmodules){
    devtools::load_all("model_code/popmodules")
  }
  
  if(trend){
    devtools::load_all("model_code/trendmodel")
  }
  
  if(small_area){
    devtools::load_all("model_code/smallareamodel")
  }
  
  if(housing_led){
    devtools::load_all("model_code/housingledmodel")
  }
  
}
