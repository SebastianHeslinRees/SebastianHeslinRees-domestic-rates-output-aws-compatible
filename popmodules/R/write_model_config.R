#' Save a model config file as a .txt
#'
#' Save model parameters in the model output directory for future reference
#'
#' @param config_list A list. The model config.
#' @param output_dir A string. The output path of the projection. Default \code{config_list$output_dir}
#'
#' @importFrom data.table fwrite
#' @export

write_model_config <- function(config_list, output_dir = config_list$output_dir){
  
  a <- data.frame(var=as.character(), value=as.character())
  
  for(i in 1:length(config_list)){
    a <- rbind(a,
               data.frame(var = paste0(names(config_list)[[i]], " "),
                          value = paste0(" ", as.character(config_list[[i]]))))
  }
  
  if(!grepl("/$", output_dir)){ output_dir <- paste0(output_dir, "/")}
  fwrite(a, paste0(output_dir, "config_list.txt"), sep = "=", col.names = FALSE)  
  
}