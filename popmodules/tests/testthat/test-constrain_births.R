
births <-  expand.grid(year=2000, age=45:49, gss_code=c("E01","E02","S03"), sex="female", births = 100, stringsAsFactors = FALSE)
constraint <- expand.grid(year=2000, age=45, sex="female", births=400, country = "E", stringsAsFactors = FALSE) %>%
  rbind(expand.grid(year = 2000, age = 46, sex = "female", births=1600, country = "E", stringsAsFactors = FALSE))
output <- expand.grid(year=2000, age=45:49, gss_code=c("E01","E02"), sex= "female", births = 200, country = "E", stringsAsFactors = FALSE) %>%
  rbind(expand.grid(year=2000, age=45:49, gss_code="S03", sex= "female", births = 100, country = "S", stringsAsFactors = FALSE)) %>%
  data.frame() %>%
  select(year, gss_code, sex, age, births)

x <- constrain_births(births, constraint)

test_that("constrain_births can scale up and down", {
  expect_equivalent(x, output)

  constraint_in <- dplyr::mutate(constraint, births = births/4)
  output_out <- dplyr::mutate(output, births = ifelse(substr(gss_code,1,1)=="E", births/4, births))

  expect_equivalent(constrain_births(births, constraint_in),
                    output_out)
})


#countries
constraint <- expand.grid(year=2000, age=45, sex="female", births=400, country = c("E","S"), stringsAsFactors = FALSE) %>%
  rbind(expand.grid(year = 2000, age = 46, sex = "female", births=1600, country = c("E","S"), stringsAsFactors = FALSE))
output <- expand.grid(year=2000, age=45:49, gss_code=c("E01","E02"), sex= "female", births = 200, country = "E", stringsAsFactors = FALSE) %>%
  rbind(expand.grid(year=2000, age=45:49, gss_code="S03", sex= "female", births = 400, country = "S", stringsAsFactors = FALSE)) %>%
  data.frame() %>%
  select(year, gss_code, sex, age, births)

x <- constrain_births(births, constraint)

test_that("constrain_births can scale up and down", {
  expect_equivalent(x, output)
})
