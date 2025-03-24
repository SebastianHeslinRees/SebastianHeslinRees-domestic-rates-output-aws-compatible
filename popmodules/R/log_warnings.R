#' Write warnings to a text file
#' 
#' @param file_path A string giving the directory path and the name
#' of the file to save.
#'
#' @export
#' 
log_warnings <- function(file_path){
  
  z <- attr(warnings(), "names")
 
  n <- as.character()
  for(m in seq(z)){
    n[m] <- paste0(m, ": ", z[[m]], "\n")
  }
  
  if(length(n) >= 50){
    n <- c("More than 50 warnings were triggered. Showing the first 50 only.\n", n)
  }
  
  cat(as.character(n), sep = "\n", file = file_path, append = FALSE)

}
