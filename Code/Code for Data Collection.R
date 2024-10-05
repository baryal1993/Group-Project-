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
library(knitr)
library(dplyr)
library(lubridate)
library(gt)

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
# Portfolio formation  time period
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

portfolios_list <- list()

for (i in 1:nrow(periods)) {
  
  # Set portfolio formation period
  port_form_bdate <- periods$formation_start[i]
  port_form_edate <- periods$formation_end[i]
  
  pform_beta <- fm_data |> 
    filter(between(date, port_form_bdate, port_form_edate)) |> 
    select(permno, date, ret, fsi_rm) |> 
    nest(data = c(date, ret, fsi_rm)) |> 
    mutate(beta = map(data, ~ estimate_beta(., min_obs = 47))) |> 
    unnest(c(beta)) |> 
    select(permno, beta) |> 
    drop_na() |> 
    mutate(rank = dense_rank(beta)) |> 
    arrange(rank)
  portfolios_list[[i]] <- pform_beta
}


periods <- tibble(
  formation_start = c("1926-01-31", "1930-01-31", "1935-01-31", "1940-01-31", "1945-01-31", "1950-01-31", "1955-01-31", "1960-01-31", "1965-01-31", "1970-01-31", "1975-01-31", "1980-01-31", "1985-01-31", "1990-01-31", "1995-01-31", "2000-01-31", "2005-01-31", "2010-01-31", "2015-01-31"),
  formation_end   = c("1929-12-31", "1934-12-31", "1939-12-31", "1944-12-31", "1949-12-31", "1954-12-31", "1959-12-31", "1964-12-31", "1969-12-31", "1974-12-31", "1979-12-31", "1984-12-31", "1989-12-31", "1994-12-31", "1999-12-31", "2004-12-31", "2009-12-31", "2014-12-31", "2019-12-31"),
  test_start      = c("1930-01-31", "1935-01-31", "1940-01-31", "1945-01-31", "1950-01-31", "1955-01-31", "1960-01-31", "1965-01-31", "1970-01-31", "1975-01-31", "1980-01-31", "1985-01-31", "1990-01-31", "1995-01-31", "2000-01-31", "2005-01-31", "2010-01-31", "2015-01-31", "2020-01-31"),
  test_end        = c("1934-12-31", "1939-12-31", "1944-12-31", "1949-12-31", "1954-12-31", "1959-12-31", "1964-12-31", "1969-12-31", "1974-12-31", "1979-12-31", "1984-12-31", "1989-12-31", "1994-12-31", "1999-12-31", "2004-12-31", "2009-12-31", "2014-12-31", "2019-12-31", "2023-12-31")
) |> mutate(across(everything(), ymd))

summary_table_1 <- tibble(
  period = integer(),
  formation_start = as.Date(character()),
  formation_end = as.Date(character()),
  test_start = as.Date(character()),
  test_end = as.Date(character()),
  total_securities = integer(),
  securities_with_data = integer()
)

for (i in 1:nrow(periods)) {
  
  # Set portfolio formation period
  port_form_bdate <- periods$formation_start[i]
  port_form_edate <- periods$formation_end[i]
  
  formation_data <- fm_data |> 
    filter(between(date, port_form_bdate, port_form_edate)) |> 
    select(permno, date, ret, fsi_rm)
  
  total_stocks <- formation_data |> 
    distinct(permno) |> 
    nrow()
  
  securities_with_data <- formation_data |> 
    group_by(permno) |> 
    filter(n() >= 47) |>  # Filter securities with at least 47 observations
    ungroup() |> 
    distinct(permno) |> 
    nrow()

# Creating Table 1
  summary_table_1 <- summary_table_1 |> add_row(
    period = i,
    formation_start = port_form_bdate,
    formation_end = port_form_edate,
    test_start = periods$test_start[i],
    test_end = periods$test_end[i],
    total_securities = total_stocks,
    securities_with_data = securities_with_data
  )
}
summary_table_1 <- summary_table_1 %>%
  mutate(
    formation_period = paste(year(formation_start), "-", year(formation_end)),
    test_period = paste(year(test_start), "-", year(test_end))

    formatted_table <- tibble(
  " " = c(
    "Portfolio Formation Period",
    "Initial Estimation Period",
    "Testing Period",
    "No. of Securities Available",
    "No. of Securities Meeting Data Requirement"
  ),
  "1" = c(
    "1926-29", "1930-34", "1935-38", summary_table_1$total_securities[1], summary_table_1$securities_with_data[1]
  ),
  "2" = c(
    "1930-34", "1935-39", "1940-43", summary_table_1$total_securities[2], summary_table_1$securities_with_data[2]
  ),
  "3" = c(
    "1935-39", "1940-44", "1945-47", summary_table_1$total_securities[3], summary_table_1$securities_with_data[3]
  ),
  "4" = c(
    "1940-44", "1945-50", "1951-54", summary_table_1$total_securities[4], summary_table_1$securities_with_data[4]
  ),
  "5" = c(
    "1945-49", "1950-54", "1955-59", summary_table_1$total_securities[5], summary_table_1$securities_with_data[5]
  ),
  "6" = c(
    "1950-54", "1955-59", "1960-64", summary_table_1$total_securities[6], summary_table_1$securities_with_data[6]
  ),
  "7" = c(
    "1955-59", "1960-64", "1965-69", summary_table_1$total_securities[7], summary_table_1$securities_with_data[7]
  ),
  "8" = c(
    "1960-64", "1965-69", "1970-74", summary_table_1$total_securities[8], summary_table_1$securities_with_data[8]
  ),
  "9" = c(
    "1965-69", "1970-74", "1975-79", summary_table_1$total_securities[9], summary_table_1$securities_with_data[9]
  ),
  "10" = c(
    "1970-74", "1975-79", "1980-84", summary_table_1$total_securities[10], summary_table_1$securities_with_data[10]
  ),
  "11" = c(
    "1975-79", "1980-84", "1985-89", summary_table_1$total_securities[11], summary_table_1$securities_with_data[11]
  ),
  "12" = c(
    "1980-84", "1985-89", "1990-94", summary_table_1$total_securities[12], summary_table_1$securities_with_data[12]
  ),
  "13" = c(
    "1985-89", "1990-94", "1995-99", summary_table_1$total_securities[13], summary_table_1$securities_with_data[13]
  ),
  "14" = c(
    "1990-94", "1995-99", "2000-04", summary_table_1$total_securities[14], summary_table_1$securities_with_data[14]
  ),
  "15" = c(
    "1995-99", "2000-04", "2005-09", summary_table_1$total_securities[15], summary_table_1$securities_with_data[15]
  ),
  "16" = c(
    "2000-04", "2005-09", "2010-14", summary_table_1$total_securities[16], summary_table_1$securities_with_data[16]
  ),
  "17" = c(
    "2005-09", "2010-14", "2015-19", summary_table_1$total_securities[17], summary_table_1$securities_with_data[17]
  ),
  "18" = c(
    "2010-14", "2015-19", "2020-23", summary_table_1$total_securities[18], summary_table_1$securities_with_data[18]
  ),
  "19" = c(
    "2015-19", "2020-23", "2024", summary_table_1$total_securities[19], summary_table_1$securities_with_data[19]
  )
)
kable(formatted_table, align = 'c', caption = "Table 1: Portfolio Formation, Estimation, and Testing Periods")

formatted_table %>%
  gt() %>%
  tab_header(
    title = "Table 1: Portfolio Formation, Estimation, and Testing Periods",
    subtitle = "Period"
)
