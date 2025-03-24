# popmodules

The GLA's demographic models are modular, and each model combines different methods to construct its components of change - births, deaths and (most complex) migration.

The `popmodules` package contains functions for all of these, plus code that's shared between them, functions to validate population data frames, and miscellaneous things for data wrangling.



## Installation

To install, download the `population_projections` repository from the GLA GitHub at https://github.com/Greater-London-Authority/population_projections/. Then open the popmodules RStudio project within the repository (`model_code/popmodules/popmodules.Rproj`) and run
``` r
devtools::install()
```





## Contents

This is a large package, but functionality can be grouped into 5 broad categories:

### Read data
Funtions which take file paths as arguments and read in data. Data is validatated and reurned in a format expected by popmodules functions. Many functions have the prefix `get`.

### Calculate
Functions which take raw data and aggregate, wrangle and combine it to produce rates, flows and data to be used by popmodules functions. Many function have the prefix `calculate`.

### Apply and Modify
Functions which produce populations and components by applying rates and processes to input data. Most functions are generalised and can be used on a range of components. Functions which include keywords `apply`, `constrain`, `distrubte`, `construct`, `project`, `sum`, `scale`, etc.

### Validate
Functions which validate data inputs and process outputs. These can be general and called by multiple functions, or specific to individual functions. Failure of validation criteria reports meaningful error messages or warnings. Functions include the keywords `validate`, `check`, etc.

### Wrangle
Helper functions which filter, combine and create data for use at various points and in different processes throughout the models. Functions which include the keywords `order`, `filter`, `aggregate`, etc.

### Testing
Testing infrastructure is setup. To test the current state of the package, run: 

`devtools::test('model_code/popmodules')` 

`devtools::check('model_code/popmodules')` 

`codetools::checkUsagePackage('popmodules', suppressUndefined = TRUE)`