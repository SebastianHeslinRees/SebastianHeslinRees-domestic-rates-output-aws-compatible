#' Smooth a fertility curve
#'
#' Fits the Hadwiger mixture model curves (Chandola et al. 1999) to 'fertility
#' rates by age and geography' data and returns fertility rates calculated from
#' the fitted curves.
#'
#' A Levenberg-Marquardt Nonlinear Least-Squares algorithm is used to fit a
#' curve to each geographic level, with starting points chosen based on
#' previously fitted curves from 2011, and convergence tests as described here:
#' https://cran.r-project.org/web/packages/minpack.lm/minpack.lm.pdf using the
#' package defaults for convergence conditions. The parameters of the fitted
#' curve are then used to calculate new rates for each age.
#'
#' If convergence does not happen within 200 iterations, a grid search method is
#' used to run the Levenberg-Marquardt Nonlinear Least-Squares algorithm with a
#' range of starting values in order to find the best fit.
#'
#' If no fit is found then the data for that local authority is left unchanged.
#'
#' @param asfr_structure A dataframe containing the age specific fertility
#'   rates.  Ages must be in the range 15 to 44. must contain columns
#'   \code{gss_code} which contains the geography, \code{sex} which contains sex
#'   encoded as "female" or "female"/"male", \code{age} which contains ages 15
#'   to 44 and \code{fert_rate} which contains the fertility rate.
#' @param reproducible If \code{TRUE} sets a seed so that the random fitting
#'   process is reproducible. Default \code{TRUE}.
#' @param age_range_to_model Integer vector. (Consecutive) ages to include in
#'   the output. Default \code{c(15:49)}.
#'
#' @return a list with elements \code{data}, \code{LA_status} and
#'   \code{resid_sum}. \code{data} contains the smoothed fertility dataframe
#'   with ages given by \code{age_range_to_model}.  \code{LA_status} indicates
#'   whether a fit was found for each LA on the first pass, second pass, or no
#'   fit found. \code{resid_sum} contains the sum of the squares of the
#'   residuals for each LA.
#'
#' @import dplyr
#' @import minpack.lm
#' @importFrom data.table rbindlist
#' @importFrom stats deviance nls.control residuals runif setNames
#' @export

smooth_fertility <- function(asfr_structure, reproducible = TRUE, age_range_to_model=c(15:49)){

  validate_smooth_fertility(asfr_structure)

  if (reproducible == T) {
    set.seed(42)
  }

  data <- asfr_structure

  # starting values for initial fitting pass. Chosen by looking at Mixture_Hadwigers_using2011population_UPDATED_Revised_Base_Rates.csv
  m<-0.424
  a<-0.574
  b1<-3.536
  c1<-24.858
  b2<-4.815
  c2<-33.218

  # starting value range for a grid search on starting values. This is much slower than the single starting value fitting. This
  # method is used where the single value start method returns an error.
  grid_start <- data.frame(m=c(0.01,10),
                           a=c(0.01,10),
                           b1=c(0.1,50),
                           c1=c(5,30),
                           b2=c(0.1,50),
                           c2=c(25,50))

  # Hadwiger mixture model for fitting to data (Chandola et al. 1999)
  curve_function<-function(age,m,a,b1,c1,b2,c2){

    fert_rate <- a*m*(b1/c1)*(c1/age)^(3/2)*exp(-b1^2*(c1/age + age/c1 -2)) +
      (1-m)*(b2/c2)*(c2/age)^(3/2)*exp(-b2^2*(c2/age + age/c2 -2))
    
    return(fert_rate)

  }

  # get predicted output from fitted coeficients
  getPred<-function(func, fit_coefs, age) {

    fit_coefs <- as.list(fit_coefs)
    fit_coefs$age <- age
    #func should be function name in single quotes
    do.call(func, fit_coefs)

  }

  # Run the first pass fit over all GSS codes
  LAs <- unique(data$gss_code)
  coefs <- list()
  success_GSS <- list()
  resids <- list()
  i <- 1

  for (GSS in LAs){

    data_to_fit <- data %>% filter(gss_code==GSS)

    #When there is no fit the function nlsLM2 returns an error
    #The silent parameter of the try function supresses this error
    try({
      # non-linear least squares fit using modified Levenberg-Marquardt algorithm
      model_output <- nlsLM2(fert_rate ~ curve_function(age,m,a,b1,c1,b2,c2),
                             data=data_to_fit,
                             start=list(m=m,a=a,b1=b1,c1=c1,b2=b2,c2=c2),
                             control=list(maxiter=200,warnOnly=TRUE))

      coefs[[GSS]]<-coef(model_output)
      success_GSS[i]<-GSS
      resids[[GSS]]<-residuals(model_output)
      i=i+1

    }, silent=TRUE)
  }

  # Run the second pass fit over GSS codes which failed first fit
  success_GSS2<-list()
  i=1
  failed_LAs <- LAs[!LAs %in% success_GSS]

  for (GSS in failed_LAs){

    data_to_fit <- data %>% filter(gss_code==GSS)

    try({
      model_outputs <- nlsLM2(fert_rate ~ curve_function(age,m,a,b1,c1,b2,c2),
                              data=data_to_fit,
                              start=grid_start,
                              control=list(maxiter=200,warnOnly=TRUE))

      coefs[[GSS]]<-coef(model_outputs)
      #print(GSS)
      success_GSS2[i]<-GSS
      resids[[GSS]]<-residuals(model_outputs)
      i=i+1

    }, silent = TRUE)

  }
  #calculate the sum of the residuals
  resid_sum <- unlist(lapply(resids, function(x){sum(x^2)}))
  failed_GSS <- failed_LAs[!failed_LAs %in% success_GSS2]

  combine_data <- function(i){
    data.frame(age = age_range_to_model,
               gss_code=rep(names(coefs)[[i]],length(age_range_to_model)),
               fert_rate = getPred('curve_function',coefs[[i]],age_range_to_model),
               stringsAsFactors = FALSE)
  }

 smoothed_data <- lapply(seq_along(coefs),combine_data) %>%
    rbindlist() %>%
    data.frame() %>%
    mutate(sex="female",ID=paste0(age,gss_code,sex)) %>%
    mutate(fert_rate=ifelse(fert_rate<0,0,fert_rate))

  new.asfr_structure <- asfr_structure %>%
    mutate(ID=paste0(age,gss_code,sex)) %>%
    filter(!ID %in% smoothed_data$ID) %>%
    rbind(smoothed_data) %>%
    select(-ID) %>%
    data.frame()

  LA_status <- list(fitted_pass1=unlist(success_GSS), fitted_pass2=unlist(success_GSS2), not_fitted=unlist(failed_GSS))

  result <- list(data=new.asfr_structure, LA_status=LA_status, resid_sum=resid_sum)

  if (reproducible == T) {
    # 'unset' the seed
    set.seed(Sys.time())
  }

  return(result)
}




nlsLM2 <- function(formula, data=parent.frame(), start, jac=NULL,
                   algorithm="LM", control=nls.control(), lower=NULL,
                   upper=NULL, trace = FALSE, model=FALSE, ...){

  #nls.lm2<-function(par,lower=NULL, upper=NULL, fn, jac=NULL,control=nls.lm.control(), trace=FALSE, ...)

  #L <- list(par = par, lower = lower, upper = upper,
  #          fn = fn, jac=jac, control=control)

  L <- list(formula = formula, data=data, start=start, jac=jac,
            algorithm=algorithm,control=control,lower=lower,upper=upper,trace=trace,model=model)

  L <- append(L, list(...))

  #L$par <- as.data.frame(as.list(par))
  L$start <- as.data.frame(as.list(start))

  #if (NROW(L$par) == 1)
  if (NROW(L$start) == 1)
    #return(do.call(nls.lm, L))
    #try({
    return(try(do.call(nlsLM, L),silent=T))
  # })

  #if (NROW(L$par) == 2) {
  if (NROW(L$start) == 2) {

    try({
      finIter <- control$maxiter
      #u <- matrix(runif(finIter * NCOL(par)), NCOL(par))
      u <- matrix(runif(finIter * NCOL(start)), NCOL(start))

      #L$par <- t(u * unlist(par[1, ]) + (1 - u) * unlist(par[2,]))
      L$start <- t(u * unlist(start[1, ]) + (1 - u) * unlist(start[2, ]))

      #L$par <- as.data.frame(L$par)
      L$start <- as.data.frame(L$start)

      #names(L$par) <- names(par)
      names(L$start) <- names(start)

    }, silent=T)

  }
  #name<-names(L$par)
  name <- names(L$start)

  # result <- apply(L$par, 1, function(par) {
  #   L$par <- as.list(par)
  #   names(L$par)<-name
  #   xx <- try(do.call(nls.lm, L))
  #   yy <- if (inherits(xx, "try-error"))
  #     NA
  #   else xx
  #   if (trace)
  #     print(yy)
  #   yy
  # })

  result <- apply(L$start, 1, function(start) {
    L$start <- as.list(start)
    names(L$start)<-name
    xx <- try(do.call(nlsLM, L), silent=TRUE)
    yy <- if (inherits(xx, "try-error"))
      NA
    else xx
    if (trace)
      print(yy)
    yy
  })

  ss <- lapply(result, function(x) if (identical(x, NA))
    NA
    else deviance(x))
  result <- result[[which.min(ss)]]
  result$data <- substitute(data)

  result
}

#------------------------

validate_smooth_fertility <- function(asfr_structure){


  assertthat::assert_that("gss_code" %in% names(asfr_structure),
                        msg = "in smooth_fertility, the asfr_structure dataframe must have a column named gss_code")


  assertthat::assert_that("sex" %in% names(asfr_structure),
                        msg = "in smooth_fertility, the asfr_structure dataframe must have a column named sex")

  assertthat::assert_that("age" %in% names(asfr_structure),
                        msg = "in smooth_fertility, the asfr_structure dataframe must have a column named age")

  assertthat::assert_that("fert_rate" %in% names(asfr_structure),
                        msg = "in smooth_fertility, the asfr_structure dataframe must have a column named rate")

  assertthat::assert_that(setequal(asfr_structure$age, 15:44),
                          msg = "in smooth_fertility, the asfr_structure$age must be 15:44")

  assertthat::assert_that(setequal(asfr_structure$sex, "female"),
                          msg = "in smooth_fertility, the asfr_structure$sex must be female only")

  invisible()
}
