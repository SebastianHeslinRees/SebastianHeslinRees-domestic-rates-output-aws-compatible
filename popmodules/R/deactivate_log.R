#' Write model log file
#'
#' Run loggr::deactivate_log and then clean up the resulting log file
#'
#' @param log_file_path The path and file name (with extension) of the log file
#' 
#' @import dplyr
#' @importFrom loggr deactivate_log
#' @importFrom data.table fread fwrite
#' @importFrom stringr str_replace_all
#' 
#' @export

deactivate_log <- function(log_file_path){
  
  loggr::deactivate_log()
  
  fread(log_file_path, header = FALSE, sep = "*") %>%
    data.frame() %>% 
    filter(substr(V1,43,80)!="Unable to convert event to a log event") %>% 
    filter(substr(V1,25,49)!= "replacing previous import") %>% 
    mutate(V1 = str_replace_all(V1, " - SIMPLEWARNING - ", " ")) %>% 
    mutate(V1 = str_replace_all(V1, " - SIMPLEMESSAGE - ", " ")) %>% 
    mutate(V1 = str_replace_all(V1, " - SIMPLEERROR - ", " ")) %>% 
    mutate(V1 = str_replace_all(V1, "If this is a large number, check that the col_aggregation parameter only includes one geographic variable, as the test checks for all permutations of all varaibles.  Call with test_complete = FALSE if this is permitted",
                                " ")) %>% 
    fwrite(log_file_path, col.names = FALSE, quote=FALSE)
  
}


