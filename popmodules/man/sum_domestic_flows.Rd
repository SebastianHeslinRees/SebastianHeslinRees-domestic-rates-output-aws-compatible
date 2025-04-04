% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sum_domestic_flows.R
\name{sum_domestic_flows}
\alias{sum_domestic_flows}
\title{Convert origin-destination flow data to gross flow by origin or destination}
\usage{
sum_domestic_flows(domestic_flow, in_or_out, flow_col = "flow")
}
\arguments{
\item{domestic_flow}{A data frame containing sya/sex flows between geographic areas}

\item{in_or_out}{A string indicating whether the flows should be summed for
the destination (in) or origin (out) area.}

\item{flow_col}{A string. The name of the column containing the flow data.
Default \code{flow}}
}
\value{
A dataframe with flows into or out of an area summed
}
\description{
Sum origin-destination flows to give total flows by either origin or destination.
The type of flow is specified in the \code{in_or_out} parameter. The function
expects column names \code{gss_in} and \code{gss_out}. Net flows cannot be
calculated using this function.
}
