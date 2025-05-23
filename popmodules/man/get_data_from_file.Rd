% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/get_data_from_file.R
\name{get_data_from_file}
\alias{get_data_from_file}
\title{Read data from files into a named list}
\usage{
get_data_from_file(files)
}
\arguments{
\item{files}{Named list or named vector where the value is the file path and the
name is the name of the list element to write the data to}
}
\value{
List of dataframes
}
\description{
Read data from files into a named list
}
\examples{
\dontrun{
a <- list(
  cross_border_in_constraint =
    "input_data/constraints/npp_2016_cross_border_in_constraint.rds",
  cross_border_out_constraint =
    "input_data/constraints/npp_2016_cross_border_out_constraint.rds")

g <- get_data_from_file_2(files = a)
}
}
