## Fama-Macbeth (1973) replication partial 

#Load necessaay libarary
library(RPostgres)
library(tidyverse)
library(RSQLite)
library(slider)
library(furrr)
library(purrr)
library(modelsummary)
library(tseries)

# initiate conenction with WRDS
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='biaryal')



### Collect CRSP monthly stock return data ####

msf_db <- tbl(wrds, sql("select * from crsp.msf"))

start_date <- ymd("1926-01-01")
end_date <- ymd("2023-12-31")

FM_ret <- msf_db |>
  filter(date >= start_date & date <= end_date) |>
  select(
    permno, # Security identifier
    date, # Date of the observation
    ret # Return
  ) |> collect()


### Stock and Excchage Identifier ###
msenames_db <- tbl(wrds, sql("select * from crsp.msenames"))

fm_stockids <- msenames_db |>
  select (permno, primexch)|> collect()|>unique()|> filter(primexch == 'N')

fm_data <- FM_ret |> inner_join(
  fm_stockids |>
    select(permno, primexch), by = c("permno"))

MAF900_data <- dbConnect(
  SQLite(),
  "data/MAF900_data.sqlite",
  extended_types = TRUE)


dbWriteTable(MAF900_data,
             "fm_ret",
             value = fm_data,
             overwrite = TRUE)

fm_data <- fm_data |>select(permno,date,ret)|>inner_join(Fs_Idx, by = c("date"))|> arrange(permno,date)

fm_data <- tbl(MAF900_data, "fm_ret")|> collect()

Fs_Idx <- fm_data |> 
  group_by(date) |> 
  summarise(fsi_rm = mean(ret, na.rm = TRUE))

fm_data <- fm_data |> 
  left_join(Fs_Idx, by = "date")

periods <- tibble(
  formation_start = c("1926-01-31", "1930-01-31", "1935-01-31", "1940-01-31", "1945-01-31", "1950-01-31", "1955-01-31", "1960-01-31", "1965-01-31", "1970-01-31", "1975-01-31", "1980-01-31", "1985-01-31", "1990-01-31", "1995-01-31", "2000-01-31", "2005-01-31", "2010-01-31", "2015-01-31"),
  formation_end   = c("1929-12-31", "1934-12-31", "1939-12-31", "1944-12-31", "1949-12-31", "1954-12-31", "1959-12-31", "1964-12-31", "1969-12-31", "1974-12-31", "1979-12-31", "1984-12-31", "1989-12-31", "1994-12-31", "1999-12-31", "2004-12-31", "2009-12-31", "2014-12-31", "2019-12-31"),
  test_start      = c("1930-01-31", "1935-01-31", "1940-01-31", "1945-01-31", "1950-01-31", "1955-01-31", "1960-01-31", "1965-01-31", "1970-01-31", "1975-01-31", "1980-01-31", "1985-01-31", "1990-01-31", "1995-01-31", "2000-01-31", "2005-01-31", "2010-01-31", "2015-01-31", "2020-01-31"),
  test_end        = c("1934-12-31", "1939-12-31", "1944-12-31", "1949-12-31", "1954-12-31", "1959-12-31", "1964-12-31", "1969-12-31", "1974-12-31", "1979-12-31", "1984-12-31", "1989-12-31", "1994-12-31", "1999-12-31", "2004-12-31", "2009-12-31", "2014-12-31", "2019-12-31", "2023-12-31")
)

periods <- periods |> mutate(across(everything(), ymd))

estimate_beta <- function(data, min_obs = 47) {
  
  data <- data |> filter(complete.cases(ret, fsi_rm))
  
  if (nrow(data) < min_obs) {
    return(NA)
  } else {
    fit <- lm(ret ~ fsi_rm, data = data)
    return(coef(fit)[2])  # Return the beta (slope)
  }
}
