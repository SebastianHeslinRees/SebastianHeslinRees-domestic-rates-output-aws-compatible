% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/filter_to_LAs.R
\name{filter_to_LAs}
\alias{filter_to_LAs}
\title{Filter a dataframe to standard model geography}
\usage{
filter_to_LAs(df, gss_col = "gss_code")
}
\arguments{
\item{df}{A dataframe to be filtered}

\item{gss_col}{String. The name of the column in df containing gss_codes.
Default 'gss_code'.}
}
\description{
Filter a dataframe so that it includes only district-level data for England
and Wales and national-level data for Northern Ireland and Scotland
}
