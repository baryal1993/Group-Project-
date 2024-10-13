## Fama-Macbeth (1973) Replication Code

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
library(kableExtra)

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

# storing in local drive 
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

# Portfolio formation, estimation, and testing periods (extended until 2023)
periods <- tibble(
  formation_start = c("1926-01-01", "1927-01-01", "1931-01-01", "1935-01-01", "1939-01-01", "1943-01-01", 
                      "1947-01-01", "1951-01-01", "1955-01-01", "1959-01-01", "1963-01-01", "1967-01-01", 
                      "1971-01-01", "1975-01-01", "1979-01-01", "1983-01-01", "1987-01-01", "1991-01-01", 
                      "1995-01-01", "1999-01-01", "2003-01-01", "2007-01-01", "2011-01-01"),
  formation_end   = c("1929-12-31", "1933-12-31", "1937-12-31", "1941-12-31", "1945-12-31", "1949-12-31", 
                      "1953-12-31", "1957-12-31", "1961-12-31", "1965-12-31", "1969-12-31", "1973-12-31", 
                      "1977-12-31", "1981-12-31", "1985-12-31", "1989-12-31", "1993-12-31", "1997-12-31", 
                      "2001-12-31", "2005-12-31", "2009-12-31", "2013-12-31", "2017-12-31"),
  estimation_start = c("1930-01-01", "1934-01-01", "1938-01-01", "1942-01-01", "1946-01-01", "1950-01-01", 
                       "1954-01-01", "1958-01-01", "1962-01-01", "1966-01-01", "1970-01-01", "1974-01-01", 
                       "1978-01-01", "1982-01-01", "1986-01-01", "1990-01-01", "1994-01-01", "1998-01-01", 
                       "2002-01-01", "2006-01-01", "2010-01-01", "2014-01-01", "2018-01-01"),
  estimation_end   = c("1934-12-31", "1938-12-31", "1942-12-31", "1946-12-31", "1950-12-31", "1954-12-31", 
                       "1958-12-31", "1962-12-31", "1966-12-31", "1970-12-31", "1974-12-31", "1978-12-31", 
                       "1982-12-31", "1986-12-31", "1990-12-31", "1994-12-31", "1998-12-31", "2002-12-31", 
                       "2006-12-31", "2010-12-31", "2014-12-31", "2018-12-31", "2022-12-31"),
  testing_start    = c("1935-01-01", "1939-01-01", "1943-01-01", "1947-01-01", "1951-01-01", "1955-01-01", 
                       "1959-01-01", "1963-01-01", "1967-01-01", "1971-01-01", "1975-01-01", "1979-01-01", 
                       "1983-01-01", "1987-01-01", "1991-01-01", "1995-01-01", "1999-01-01", "2003-01-01", 
                       "2007-01-01", "2011-01-01", "2015-01-01", "2019-01-01", "2022-01-01"),
  testing_end      = c("1938-12-31", "1942-12-31", "1946-12-31", "1950-12-31", "1954-12-31", "1958-12-31", 
                       "1962-12-31", "1966-12-31", "1970-12-31", "1974-12-31", "1978-12-31", "1982-12-31", 
                       "1986-12-31", "1990-12-31", "1994-12-31", "1998-12-31", "2002-12-31", "2006-12-31", 
                       "2010-12-31", "2014-12-31", "2018-12-31", "2022-12-31", "2023-12-31")
)

# Convert strings to dates
periods <- periods |> mutate(across(everything(), ymd))

estimate_beta <- function(data, min_obs = 47) {
  # Ensure there are no missing values in ret and fsi_rm (market return)
  data <- data |> filter(complete.cases(ret, fsi_rm))
  
  # If not enough observations, return NA
  if (nrow(data) < min_obs) {
    return(NA)
  } else {
    fit <- lm(ret ~ fsi_rm, data = data)
    return(coef(fit)[2])  # Return the beta (slope)
  }
}

# Proceed with portfolio formation and beta estimation
portfolios_list <- list()

for (i in 1:nrow(periods)) {
  
  # Set portfolio formation period
  port_form_bdate <- periods$formation_start[i]
  port_form_edate <- periods$formation_end[i]
  
  # Estimate betas for portfolio formation
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
  
  # Save the portfolio data for this period
  portfolios_list[[i]] <- pform_beta
}


periods <- tibble(
  formation_start = c("1926-01-01", "1927-01-01", "1931-01-01", "1935-01-01", "1939-01-01", "1943-01-01", 
                      "1947-01-01", "1951-01-01", "1955-01-01", "1959-01-01", "1963-01-01", "1967-01-01", 
                      "1971-01-01", "1975-01-01", "1979-01-01", "1983-01-01", "1987-01-01", "1991-01-01", 
                      "1995-01-01", "1999-01-01", "2003-01-01", "2007-01-01", "2011-01-01"),
  formation_end   = c("1929-12-31", "1933-12-31", "1937-12-31", "1941-12-31", "1945-12-31", "1949-12-31", 
                      "1953-12-31", "1957-12-31", "1961-12-31", "1965-12-31", "1969-12-31", "1973-12-31", 
                      "1977-12-31", "1981-12-31", "1985-12-31", "1989-12-31", "1993-12-31", "1997-12-31", 
                      "2001-12-31", "2005-12-31", "2009-12-31", "2013-12-31", "2017-12-31"),
  estimation_start = c("1930-01-01", "1934-01-01", "1938-01-01", "1942-01-01", "1946-01-01", "1950-01-01", 
                       "1954-01-01", "1958-01-01", "1962-01-01", "1966-01-01", "1970-01-01", "1974-01-01", 
                       "1978-01-01", "1982-01-01", "1986-01-01", "1990-01-01", "1994-01-01", "1998-01-01", 
                       "2002-01-01", "2006-01-01", "2010-01-01", "2014-01-01", "2018-01-01"),
  estimation_end   = c("1934-12-31", "1938-12-31", "1942-12-31", "1946-12-31", "1950-12-31", "1954-12-31", 
                       "1958-12-31", "1962-12-31", "1966-12-31", "1970-12-31", "1974-12-31", "1978-12-31", 
                       "1982-12-31", "1986-12-31", "1990-12-31", "1994-12-31", "1998-12-31", "2002-12-31", 
                       "2006-12-31", "2010-12-31", "2014-12-31", "2018-12-31", "2022-12-31"),
  testing_start    = c("1935-01-01", "1939-01-01", "1943-01-01", "1947-01-01", "1951-01-01", "1955-01-01", 
                       "1959-01-01", "1963-01-01", "1967-01-01", "1971-01-01", "1975-01-01", "1979-01-01", 
                       "1983-01-01", "1987-01-01", "1991-01-01", "1995-01-01", "1999-01-01", "2003-01-01", 
                       "2007-01-01", "2011-01-01", "2015-01-01", "2019-01-01", "2022-01-01"),
  testing_end      = c("1938-12-31", "1942-12-31", "1946-12-31", "1950-12-31", "1954-12-31", "1958-12-31", 
                       "1962-12-31", "1966-12-31", "1970-12-31", "1974-12-31", "1978-12-31", "1982-12-31", 
                       "1986-12-31", "1990-12-31", "1994-12-31", "1998-12-31", "2002-12-31", "2006-12-31", 
                       "2010-12-31", "2014-12-31", "2018-12-31", "2022-12-31", "2023-12-31")
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

# Loop through each period to populate the table
for (i in 1:nrow(periods)) {
  
  # Set portfolio formation period
  port_form_bdate <- periods$formation_start[i]
  port_form_edate <- periods$formation_end[i]
  
  # Filter the data for the formation period
  formation_data <- fm_data |> 
    filter(between(date, port_form_bdate, port_form_edate)) |> 
    select(permno, date, ret, fsi_rm)
  
  # Calculate the total number of securities
  total_stocks <- formation_data |> 
    distinct(permno) |> 
    nrow()
  
  # Calculate the number of securities that meet the data requirement (47 observations)
  securities_with_data <- formation_data |> 
    group_by(permno) |> 
    filter(n() >= 47) |>  # Filter securities with at least 47 observations
    ungroup() |> 
    distinct(permno) |> 
    nrow()
  
  # Add the results to the summary table
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

# View the final Table 1 summary
print(summary_table_1)

summary_table_1 <- summary_table_1 %>%
  mutate(
    formation_period = paste(year(formation_start), "-", year(formation_end)),
    test_period = paste(year(test_start), "-", year(test_end))
  )
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
    "1927-33", "1934-38", "1939-42", summary_table_1$total_securities[2], summary_table_1$securities_with_data[2]
  ),
  "3" = c(
    "1931-37", "1938-42", "1943-46", summary_table_1$total_securities[3], summary_table_1$securities_with_data[3]
  ),
  "4" = c(
    "1935-41", "1942-46", "1947-50", summary_table_1$total_securities[4], summary_table_1$securities_with_data[4]
  ),
  "5" = c(
    "1939-45", "1946-50", "1951-54", summary_table_1$total_securities[5], summary_table_1$securities_with_data[5]
  ),
  "6" = c(
    "1943-49", "1950-54", "1955-58", summary_table_1$total_securities[6], summary_table_1$securities_with_data[6]
  ),
  "7" = c(
    "1947-53", "1954-58", "1959-62", summary_table_1$total_securities[7], summary_table_1$securities_with_data[7]
  ),
  "8" = c(
    "1951-57", "1958-62", "1963-66", summary_table_1$total_securities[8], summary_table_1$securities_with_data[8]
  ),
  "9" = c(
    "1955-61", "1962-66", "1967-70", summary_table_1$total_securities[9], summary_table_1$securities_with_data[9]
  ),
  "10" = c(
    "1959-65", "1966-70", "1971-74", summary_table_1$total_securities[10], summary_table_1$securities_with_data[10]
  ),
  "11" = c(
    "1963-69", "1970-74", "1975-78", summary_table_1$total_securities[11], summary_table_1$securities_with_data[11]
  ),
  "12" = c(
    "1967-73", "1974-78", "1979-82", summary_table_1$total_securities[12], summary_table_1$securities_with_data[12]
  ),
  "13" = c(
    "1971-77", "1978-82", "1983-86", summary_table_1$total_securities[13], summary_table_1$securities_with_data[13]
  ),
  "14" = c(
    "1975-81", "1982-86", "1987-90", summary_table_1$total_securities[14], summary_table_1$securities_with_data[14]
  ),
  "15" = c(
    "1979-85", "1986-90", "1991-94", summary_table_1$total_securities[15], summary_table_1$securities_with_data[15]
  ),
  "16" = c(
    "1983-89", "1990-94", "1995-98", summary_table_1$total_securities[16], summary_table_1$securities_with_data[16]
  ),
  "17" = c(
    "1987-93", "1994-98", "1999-02", summary_table_1$total_securities[17], summary_table_1$securities_with_data[17]
  ),
  "18" = c(
    "1991-97", "1998-02", "2003-06", summary_table_1$total_securities[18], summary_table_1$securities_with_data[18]
  ),
  "19" = c(
    "1995-01", "2002-06", "2007-10", summary_table_1$total_securities[19], summary_table_1$securities_with_data[19]
  ),
  "20" = c(
    "1999-05", "2006-10", "2011-14", summary_table_1$total_securities[20], summary_table_1$securities_with_data[20]
  ),
  "21" = c(
    "2003-09", "2010-14", "2015-18", summary_table_1$total_securities[21], summary_table_1$securities_with_data[21]
  ),
  "22" = c(
    "2007-13", "2014-18", "2019-22", summary_table_1$total_securities[22], summary_table_1$securities_with_data[22]
  ),
  "23" = c(
    "2011-17", "2018-22", "2023", summary_table_1$total_securities[23], summary_table_1$securities_with_data[23]
  )
)


# Display the table 1 using knitr::kable()
kable(formatted_table, align = 'c', caption = "Table 1: Portfolio Formation, Estimation, and Testing Periods")

library(gt)
formatted_table %>%
  gt() %>%
  tab_header(
    title = "Table 1: Portfolio Formation, Estimation, and Testing Periods",
    subtitle = "Period"
  )
# Table 2 Portfolio for Estimation
# Define estimation periods for the selected years
estimation_periods <- list(
  c("1934-01-01", "1938-12-31"),
  c("1942-01-01", "1946-12-31"),
  c("1950-01-01", "1954-12-31"),
  c("1958-01-01", "1962-12-31")
)

# To ensure there are no missing values in returns or market returns
fm_data_1934_38_clean <- fm_data_1934_38 %>%
  filter(!is.na(ret) & !is.na(fsi_rm))

# Function to calculate all required statistics for each portfolio
calculate_portfolio_statistics <- function(data, num_portfolios = 20) {
  
  # Step 1: Calculate individual security statistics
  betas_and_residuals <- data %>%
    group_by(permno) %>%
    do({
      # Run the regression for each security
      model <- lm(ret ~ fsi_rm, data = .)
      
      # Extract the beta coefficient and standard error
      beta <- coef(model)[2]
      beta_se <- sqrt(vcov(model)[2, 2])  # Standard error of beta
      
      # Extract R-squared of the regression
      r_squared <- summary(model)$r.squared
      
      # Calculate fitted values and residuals manually
      fitted_vals <- fitted(model)
      residuals_vals <- .$ret - fitted_vals  # Actual - Fitted = Residuals
      
      # Calculate standard deviation of residuals
      residual_sd <- sd(residuals_vals, na.rm = TRUE)
      
      # Calculate standard deviation of returns
      return_sd <- sd(.$ret, na.rm = TRUE)
      
      # Return all statistics for each security
      data.frame(beta = beta, beta_se = beta_se, r_squared = r_squared,
                 return_sd = return_sd, residual_sd = residual_sd)
    }) %>%
    ungroup()
  
  # Step 2: Assign portfolios based on beta
  betas_and_residuals <- betas_and_residuals %>%
    arrange(beta) %>%
    mutate(Portfolio = ntile(beta, num_portfolios))  # Assign securities to 20 portfolios
  
  # Step 3: Calculate portfolio-level statistics
  portfolio_stats <- betas_and_residuals %>%
    group_by(Portfolio) %>%
    summarize(
      avg_beta = mean(beta, na.rm = TRUE),
      avg_beta_se = mean(beta_se, na.rm = TRUE),
      avg_r_squared = mean(r_squared, na.rm = TRUE),
      avg_return_sd = mean(return_sd, na.rm = TRUE),
      avg_residual_sd = mean(residual_sd, na.rm = TRUE)
    )
  
  # Step 4: Calculate the average residual SD across all portfolios
  overall_avg_residual_sd <- mean(portfolio_stats$avg_residual_sd, na.rm = TRUE)
  
  # Step 5: Calculate the residual ratio for each portfolio
  portfolio_stats <- portfolio_stats %>%
    mutate(residual_ratio = avg_residual_sd / overall_avg_residual_sd)
  
  return(portfolio_stats)
}

portfolio_estimation_1934_38 <- calculate_portfolio_statistics(fm_data_1934_38_clean)

print(portfolio_estimation_1934_38)

fm_data_1934_38_clean <- fm_data_1934_38 %>%
  filter(!is.na(ret) & !is.na(fsi_rm))

# Function to calculate portfolio statistics with Mean s(εi)
calculate_mean_residual_sd <- function(data, num_portfolios = 20) {
  
  # Step 1: Calculate individual security statistics
  betas_and_residuals <- data %>%
    group_by(permno) %>%
    do({
      # Run the regression for each security
      model <- lm(ret ~ fsi_rm, data = .)
      
      fitted_vals <- fitted(model)
      residuals_vals <- .$ret - fitted_vals  # Actual - Fitted = Residuals
      
      # Calculate standard deviation of residuals for each security
      residual_sd <- sd(residuals_vals, na.rm = TRUE)
      
      # Return residual SD for each security
      data.frame(residual_sd = residual_sd)
    }) %>%
    ungroup()
  
  # Step 2: Assign portfolios based on beta (already done in your main code)
  betas_and_residuals <- betas_and_residuals %>%
    arrange(residual_sd) %>%
    mutate(Portfolio = ntile(residual_sd, num_portfolios))  # Assign securities to portfolios
  
  # Step 3: Calculate Mean s(εi) for each portfolio
  portfolio_stats <- betas_and_residuals %>%
    group_by(Portfolio) %>%
    summarize(mean_residual_sd = mean(residual_sd, na.rm = TRUE))  # Mean s(εi) for each portfolio
  
  return(portfolio_stats)
}

# Apply the function to calculate Mean s(εi) for the 1934-1938 period
mean_residual_sd_1934_38 <- calculate_mean_residual_sd(fm_data_1934_38_clean)

print(mean_residual_sd_1934_38)

merged_portfolio_1934_38 <- portfolio_estimation_1934_38 %>%
  left_join(mean_residual_sd_1934_38, by = "Portfolio")
print(merged_portfolio_1934_38)

formatted_table_1934_38 <- data.frame(
  Statistic = c(
    "Beta (β_{p,t-1})",  # Beta for portfolio
    "s(Beta)",  # Standard error of beta
    "R² (r(Rp, Rm)²)",  # R-squared
    "s(Rp)",  # Standard deviation of portfolio returns
    "s(εp)",  # Standard deviation of residuals
    "Mean s(εi)",  # Average residual standard deviation
    "s(εp) / Mean s(εi)"  # Residual SD ratio
  ),
  `1` = round(c(formatted_table$avg_beta[1], formatted_table$avg_beta_se[1], formatted_table$avg_r_squared[1],
                formatted_table$avg_return_sd[1], formatted_table$avg_residual_sd[1], 
                formatted_table$mean_residual_sd[1], formatted_table$residual_ratio[1]), 4),
  `2` = round(c(formatted_table$avg_beta[2], formatted_table$avg_beta_se[2], formatted_table$avg_r_squared[2],
                formatted_table$avg_return_sd[2], formatted_table$avg_residual_sd[2], 
                formatted_table$mean_residual_sd[2], formatted_table$residual_ratio[2]), 4),
  `3` = round(c(formatted_table$avg_beta[3], formatted_table$avg_beta_se[3], formatted_table$avg_r_squared[3],
                formatted_table$avg_return_sd[3], formatted_table$avg_residual_sd[3], 
                formatted_table$mean_residual_sd[3], formatted_table$residual_ratio[3]), 4),
  `4` = round(c(formatted_table$avg_beta[4], formatted_table$avg_beta_se[4], formatted_table$avg_r_squared[4],
                formatted_table$avg_return_sd[4], formatted_table$avg_residual_sd[4], 
                formatted_table$mean_residual_sd[4], formatted_table$residual_ratio[4]), 4),
  `5` = round(c(formatted_table$avg_beta[5], formatted_table$avg_beta_se[5], formatted_table$avg_r_squared[5],
                formatted_table$avg_return_sd[5], formatted_table$avg_residual_sd[5], 
                formatted_table$mean_residual_sd[5], formatted_table$residual_ratio[5]), 4),
  `6` = round(c(formatted_table$avg_beta[6], formatted_table$avg_beta_se[6], formatted_table$avg_r_squared[6],
                formatted_table$avg_return_sd[6], formatted_table$avg_residual_sd[6], 
                formatted_table$mean_residual_sd[6], formatted_table$residual_ratio[6]), 4),
  `7` = round(c(formatted_table$avg_beta[7], formatted_table$avg_beta_se[7], formatted_table$avg_r_squared[7],
                formatted_table$avg_return_sd[7], formatted_table$avg_residual_sd[7], 
                formatted_table$mean_residual_sd[7], formatted_table$residual_ratio[7]), 4),
  `8` = round(c(formatted_table$avg_beta[8], formatted_table$avg_beta_se[8], formatted_table$avg_r_squared[8],
                formatted_table$avg_return_sd[8], formatted_table$avg_residual_sd[8], 
                formatted_table$mean_residual_sd[8], formatted_table$residual_ratio[8]), 4),
  `9` = round(c(formatted_table$avg_beta[9], formatted_table$avg_beta_se[9], formatted_table$avg_r_squared[9],
                formatted_table$avg_return_sd[9], formatted_table$avg_residual_sd[9], 
                formatted_table$mean_residual_sd[9], formatted_table$residual_ratio[9]), 4),
  `10` = round(c(formatted_table$avg_beta[10], formatted_table$avg_beta_se[10], formatted_table$avg_r_squared[10],
                 formatted_table$avg_return_sd[10], formatted_table$avg_residual_sd[10], 
                 formatted_table$mean_residual_sd[10], formatted_table$residual_ratio[10]), 4)
)

kable(formatted_table_1934_38, format = "html", col.names = c("Statistic", paste0(1:10))) %>%
  kable_styling(bootstrap_options = c("striped", "hover", "condensed")) %>%
  add_header_above(c(" " = 1, "Portfolios for Estimation Period 1934-38" = 10), bold = TRUE) %>%  # Add title centered
  row_spec(1, bold = FALSE, italic = FALSE)  # Remove bold from the Beta row


# Create the formatted table for portfolios 11 to 20 with statistics and periods numbers

formatted_table_1934_38_11_to_20 <- data.frame(
  Statistic = c(
    "Beta (β_{p,t-1})",  # Beta for portfolio
    "s(Beta)",           # Standard error of beta
    "R² (r(Rp, Rm)²)",   # R-squared
    "s(Rp)",             # Standard deviation of portfolio returns
    "s(εp)",             # Standard deviation of residuals
    "Mean s(εi)",        # Average residual standard deviation
    "s(εp) / Mean s(εi)" # Residual SD ratio
  ),
  `11` = round(c(formatted_table$avg_beta[11], formatted_table$avg_beta_se[11], formatted_table$avg_r_squared[11],
                 formatted_table$avg_return_sd[11], formatted_table$avg_residual_sd[11], 
                 formatted_table$mean_residual_sd[11], formatted_table$residual_ratio[11]), 4),
  `12` = round(c(formatted_table$avg_beta[12], formatted_table$avg_beta_se[12], formatted_table$avg_r_squared[12],
                 formatted_table$avg_return_sd[12], formatted_table$avg_residual_sd[12], 
                 formatted_table$mean_residual_sd[12], formatted_table$residual_ratio[12]), 4),
  `13` = round(c(formatted_table$avg_beta[13], formatted_table$avg_beta_se[13], formatted_table$avg_r_squared[13],
                 formatted_table$avg_return_sd[13], formatted_table$avg_residual_sd[13], 
                 formatted_table$mean_residual_sd[13], formatted_table$residual_ratio[13]), 4),
  `14` = round(c(formatted_table$avg_beta[14], formatted_table$avg_beta_se[14], formatted_table$avg_r_squared[14],
                 formatted_table$avg_return_sd[14], formatted_table$avg_residual_sd[14], 
                 formatted_table$mean_residual_sd[14], formatted_table$residual_ratio[14]), 4),
  `15` = round(c(formatted_table$avg_beta[15], formatted_table$avg_beta_se[15], formatted_table$avg_r_squared[15],
                 formatted_table$avg_return_sd[15], formatted_table$avg_residual_sd[15], 
                 formatted_table$mean_residual_sd[15], formatted_table$residual_ratio[15]), 4),
  `16` = round(c(formatted_table$avg_beta[16], formatted_table$avg_beta_se[16], formatted_table$avg_r_squared[16],
                 formatted_table$avg_return_sd[16], formatted_table$avg_residual_sd[16], 
                 formatted_table$mean_residual_sd[16], formatted_table$residual_ratio[16]), 4),
  `17` = round(c(formatted_table$avg_beta[17], formatted_table$avg_beta_se[17], formatted_table$avg_r_squared[17],
                 formatted_table$avg_return_sd[17], formatted_table$avg_residual_sd[17], 
                 formatted_table$mean_residual_sd[17], formatted_table$residual_ratio[17]), 4),
  `18` = round(c(formatted_table$avg_beta[18], formatted_table$avg_beta_se[18], formatted_table$avg_r_squared[18],
                 formatted_table$avg_return_sd[18], formatted_table$avg_residual_sd[18], 
                 formatted_table$mean_residual_sd[18], formatted_table$residual_ratio[18]), 4),
  `19` = round(c(formatted_table$avg_beta[19], formatted_table$avg_beta_se[19], formatted_table$avg_r_squared[19],
                 formatted_table$avg_return_sd[19], formatted_table$avg_residual_sd[19], 
                 formatted_table$mean_residual_sd[19], formatted_table$residual_ratio[19]), 4),
  `20` = round(c(formatted_table$avg_beta[20], formatted_table$avg_beta_se[20], formatted_table$avg_r_squared[20],
                 formatted_table$avg_return_sd[20], formatted_table$avg_residual_sd[20], 
                 formatted_table$mean_residual_sd[20], formatted_table$residual_ratio[20]), 4)
)

# Print the formatted table for portfolios 11 to 20 with a title and without bold formatting for the Beta row
kable(formatted_table_1934_38_11_to_20, format = "html", col.names = c("Statistic", paste0(11:20))) %>%
  kable_styling(bootstrap_options = c("striped", "hover", "condensed")) %>%
  add_header_above(c(" " = 1, "Portfolios for Estimation Period 1934-38" = 10), bold = TRUE) %>%  # Add title centered
  row_spec(1, bold = FALSE, italic = FALSE)  

#Portfolio for Estimation for Period 1942-46
start_date <- as.Date("1942-01-01")
end_date <- as.Date("1946-12-31")

# Filter the dataset for the 1942-1946 period
fm_data_1942_46 <- fm_data %>%
  filter(date >= start_date & date <= end_date)

# Check the filtered data
print(head(fm_data_1942_46))

fm_data_1942_46_clean <- fm_data_1942_46 %>%
  filter(!is.na(ret) & !is.na(fsi_rm))

# Function to calculate all required statistics for each portfolio
calculate_portfolio_statistics <- function(data, num_portfolios = 20) {
  
  # Step 1: Calculate individual security statistics
  betas_and_residuals <- data %>%
    group_by(permno) %>%
    do({
      # Run the regression for each security
      model <- lm(ret ~ fsi_rm, data = .)
      
      # Extract the beta coefficient and standard error
      beta <- coef(model)[2]
      beta_se <- sqrt(vcov(model)[2, 2])  # Standard error of beta
      
      # Extract R-squared of the regression
      r_squared <- summary(model)$r.squared
      
      # Calculate fitted values and residuals manually
      fitted_vals <- fitted(model)
      residuals_vals <- .$ret - fitted_vals  # Actual - Fitted = Residuals
      
      # Calculate standard deviation of residuals
      residual_sd <- sd(residuals_vals, na.rm = TRUE)
      
      # Calculate standard deviation of returns
      return_sd <- sd(.$ret, na.rm = TRUE)
      
      # Return all statistics for each security
      data.frame(beta = beta, beta_se = beta_se, r_squared = r_squared,
                 return_sd = return_sd, residual_sd = residual_sd)
    }) %>%
    ungroup()
  
  # Step 2: Assign portfolios based on beta
  betas_and_residuals <- betas_and_residuals %>%
    arrange(beta) %>%
    mutate(Portfolio = ntile(beta, num_portfolios))  # Assign securities to 20 portfolios
  
  # Step 3: Calculate portfolio-level statistics
  portfolio_stats <- betas_and_residuals %>%
    group_by(Portfolio) %>%
    summarize(
      avg_beta = mean(beta, na.rm = TRUE),
      avg_beta_se = mean(beta_se, na.rm = TRUE),
      avg_r_squared = mean(r_squared, na.rm = TRUE),
      avg_return_sd = mean(return_sd, na.rm = TRUE),
      avg_residual_sd = mean(residual_sd, na.rm = TRUE)
    )
  
  # Step 4: Calculate the average residual SD across all portfolios
  overall_avg_residual_sd <- mean(portfolio_stats$avg_residual_sd, na.rm = TRUE)
  
  # Step 5: Calculate the residual ratio for each portfolio
  portfolio_stats <- portfolio_stats %>%
    mutate(residual_ratio = avg_residual_sd / overall_avg_residual_sd)
  
  return(portfolio_stats)
}
# Apply the function to calculate all statistics for the 1942-1946 period
portfolio_estimation_1942_46 <- calculate_portfolio_statistics(fm_data_1942_46_clean)

# Check the results
print(portfolio_estimation_1942_46)

fm_data_1942_46_clean <- fm_data_1942_46 %>%
  filter(!is.na(ret) & !is.na(fsi_rm))

# Function to calculate portfolio statistics with Mean s(εi)
calculate_mean_residual_sd <- function(data, num_portfolios = 20) {
  
  # Step 1: Calculate individual security statistics
  betas_and_residuals <- data %>%
    group_by(permno) %>%
    do({
      # Run the regression for each security
      model <- lm(ret ~ fsi_rm, data = .)
      
      # Calculate residuals manually
      fitted_vals <- fitted(model)
      residuals_vals <- .$ret - fitted_vals  # Actual - Fitted = Residuals
      
      # Calculate standard deviation of residuals for each security
      residual_sd <- sd(residuals_vals, na.rm = TRUE)
      
      # Return residual SD for each security
      data.frame(residual_sd = residual_sd)
    }) %>%
    ungroup()
  
  # Step 2: Assign portfolios based on beta (already done in your main code)
  betas_and_residuals <- betas_and_residuals %>%
    arrange(residual_sd) %>%
    mutate(Portfolio = ntile(residual_sd, num_portfolios))  # Assign securities to portfolios
  
  # Step 3: Calculate Mean s(εi) for each portfolio
  portfolio_stats <- betas_and_residuals %>%
    group_by(Portfolio) %>%
    summarize(mean_residual_sd = mean(residual_sd, na.rm = TRUE))  # Mean s(εi) for each portfolio
  
  return(portfolio_stats)
}

# Apply the function to calculate Mean s(εi) for the 1934-1938 period
mean_residual_sd_1942_46 <- calculate_mean_residual_sd(fm_data_1942_46_clean)

# Check the results
print(mean_residual_sd_1942_46)

merged_portfolio_1942_46 <- portfolio_estimation_1942_46 %>%
  left_join(mean_residual_sd_1942_46, by = "Portfolio")

# Check the results
print(merged_portfolio_1942_46)

formatted_table_1942_46_1_to_10 <- data.frame(
  Statistic = c(
    "Beta (β_{p,t-1})",  # Beta for portfolio
    "s(Beta)",  # Standard error of beta
    "R² (r(Rp, Rm)²)",  # R-squared
    "s(Rp)",  # Standard deviation of portfolio returns
    "s(εp)",  # Standard deviation of residuals
    "Mean s(εi)",  # Average residual standard deviation
    "s(εp) / Mean s(εi)"  # Residual SD ratio
  ),
  `1` = round(c(merged_portfolio_1942_46$avg_beta[1], merged_portfolio_1942_46$avg_beta_se[1], merged_portfolio_1942_46$avg_r_squared[1],
                merged_portfolio_1942_46$avg_return_sd[1], merged_portfolio_1942_46$avg_residual_sd[1], merged_portfolio_1942_46$mean_residual_sd[1], merged_portfolio_1942_46$residual_ratio[1]), 4),
  `2` = round(c(merged_portfolio_1942_46$avg_beta[2], merged_portfolio_1942_46$avg_beta_se[2], merged_portfolio_1942_46$avg_r_squared[2],
                merged_portfolio_1942_46$avg_return_sd[2], merged_portfolio_1942_46$avg_residual_sd[2], merged_portfolio_1942_46$mean_residual_sd[2], merged_portfolio_1942_46$residual_ratio[2]), 4),
  `3` = round(c(merged_portfolio_1942_46$avg_beta[3], merged_portfolio_1942_46$avg_beta_se[3], merged_portfolio_1942_46$avg_r_squared[3],
                merged_portfolio_1942_46$avg_return_sd[3], merged_portfolio_1942_46$avg_residual_sd[3], merged_portfolio_1942_46$mean_residual_sd[3], merged_portfolio_1942_46$residual_ratio[3]), 4),
  `4` = round(c(merged_portfolio_1942_46$avg_beta[4], merged_portfolio_1942_46$avg_beta_se[4], merged_portfolio_1942_46$avg_r_squared[4],
                merged_portfolio_1942_46$avg_return_sd[4], merged_portfolio_1942_46$avg_residual_sd[4], merged_portfolio_1942_46$mean_residual_sd[4], merged_portfolio_1942_46$residual_ratio[4]), 4),
  `5` = round(c(merged_portfolio_1942_46$avg_beta[5], merged_portfolio_1942_46$avg_beta_se[5], merged_portfolio_1942_46$avg_r_squared[5],
                merged_portfolio_1942_46$avg_return_sd[5], merged_portfolio_1942_46$avg_residual_sd[5], merged_portfolio_1942_46$mean_residual_sd[5], merged_portfolio_1942_46$residual_ratio[5]), 4),
  `6` = round(c(merged_portfolio_1942_46$avg_beta[6], merged_portfolio_1942_46$avg_beta_se[6], merged_portfolio_1942_46$avg_r_squared[6],
                merged_portfolio_1942_46$avg_return_sd[6], merged_portfolio_1942_46$avg_residual_sd[6], merged_portfolio_1942_46$mean_residual_sd[6], merged_portfolio_1942_46$residual_ratio[6]), 4),
  `7` = round(c(merged_portfolio_1942_46$avg_beta[7], merged_portfolio_1942_46$avg_beta_se[7], merged_portfolio_1942_46$avg_r_squared[7],
                merged_portfolio_1942_46$avg_return_sd[7], merged_portfolio_1942_46$avg_residual_sd[7], merged_portfolio_1942_46$mean_residual_sd[7], merged_portfolio_1942_46$residual_ratio[7]), 4),
  `8` = round(c(merged_portfolio_1942_46$avg_beta[8], merged_portfolio_1942_46$avg_beta_se[8], merged_portfolio_1942_46$avg_r_squared[8],
                merged_portfolio_1942_46$avg_return_sd[8], merged_portfolio_1942_46$avg_residual_sd[8], merged_portfolio_1942_46$mean_residual_sd[8], merged_portfolio_1942_46$residual_ratio[8]), 4),
  `9` = round(c(merged_portfolio_1942_46$avg_beta[9], merged_portfolio_1942_46$avg_beta_se[9], merged_portfolio_1942_46$avg_r_squared[9],
                merged_portfolio_1942_46$avg_return_sd[9], merged_portfolio_1942_46$avg_residual_sd[9], merged_portfolio_1942_46$mean_residual_sd[9], merged_portfolio_1942_46$residual_ratio[9]), 4),
  `10` = round(c(merged_portfolio_1942_46$avg_beta[10], merged_portfolio_1942_46$avg_beta_se[10], merged_portfolio_1942_46$avg_r_squared[10],
                 merged_portfolio_1942_46$avg_return_sd[10], merged_portfolio_1942_46$avg_residual_sd[10], merged_portfolio_1942_46$mean_residual_sd[10], merged_portfolio_1942_46$residual_ratio[10]), 4)
)

# Print the formatted table using knitr::kable without altering values
kable(formatted_table_1942_46_1_to_10, format = "html", col.names = c("Statistic", paste0(1:10))) %>%
  kable_styling(bootstrap_options = c("striped", "hover", "condensed")) %>%
  add_header_above(c(" " = 1, "Portfolios for Estimation Period 1942-46" = 10)) %>%  # Add the title
  row_spec(1, bold = FALSE, italic = FALSE)

formatted_table_1942_46 <- data.frame(
  Statistic = c(
    "Beta (β_{p,t-1})",  # Beta for portfolio
    "s(Beta)",  # Standard error of beta
    "R² (r(Rp, Rm)²)",  # R-squared
    "s(Rp)",  # Standard deviation of portfolio returns
    "s(εp)",  # Standard deviation of residuals
    "Mean s(εi)",  # Average residual standard deviation
    "s(εp) / Mean s(εi)"  # Residual SD ratio
  ),
  `11` = round(c(merged_portfolio_1942_46$avg_beta[11], merged_portfolio_1942_46$avg_beta_se[11], merged_portfolio_1942_46$avg_r_squared[11],
                 merged_portfolio_1942_46$avg_return_sd[11], merged_portfolio_1942_46$avg_residual_sd[11], merged_portfolio_1942_46$mean_residual_sd[11], merged_portfolio_1942_46$residual_ratio[11]), 4),
  `12` = round(c(merged_portfolio_1942_46$avg_beta[12], merged_portfolio_1942_46$avg_beta_se[12], merged_portfolio_1942_46$avg_r_squared[12],
                 merged_portfolio_1942_46$avg_return_sd[12], merged_portfolio_1942_46$avg_residual_sd[12], merged_portfolio_1942_46$mean_residual_sd[12], merged_portfolio_1942_46$residual_ratio[12]), 4),
  `13` = round(c(merged_portfolio_1942_46$avg_beta[13], merged_portfolio_1942_46$avg_beta_se[13], merged_portfolio_1942_46$avg_r_squared[13],
                 merged_portfolio_1942_46$avg_return_sd[13], merged_portfolio_1942_46$avg_residual_sd[13], merged_portfolio_1942_46$mean_residual_sd[13], merged_portfolio_1942_46$residual_ratio[13]), 4),
  `14` = round(c(merged_portfolio_1942_46$avg_beta[14], merged_portfolio_1942_46$avg_beta_se[14], merged_portfolio_1942_46$avg_r_squared[14],
                 merged_portfolio_1942_46$avg_return_sd[14], merged_portfolio_1942_46$avg_residual_sd[14], merged_portfolio_1942_46$mean_residual_sd[14], merged_portfolio_1942_46$residual_ratio[14]), 4),
  `15` = round(c(merged_portfolio_1942_46$avg_beta[15], merged_portfolio_1942_46$avg_beta_se[15], merged_portfolio_1942_46$avg_r_squared[15],
                 merged_portfolio_1942_46$avg_return_sd[15], merged_portfolio_1942_46$avg_residual_sd[15], merged_portfolio_1942_46$mean_residual_sd[15], merged_portfolio_1942_46$residual_ratio[15]), 4),
  `16` = round(c(merged_portfolio_1942_46$avg_beta[16], merged_portfolio_1942_46$avg_beta_se[16], merged_portfolio_1942_46$avg_r_squared[16],
                 merged_portfolio_1942_46$avg_return_sd[16], merged_portfolio_1942_46$avg_residual_sd[16], merged_portfolio_1942_46$mean_residual_sd[16], merged_portfolio_1942_46$residual_ratio[16]), 4),
  `17` = round(c(merged_portfolio_1942_46$avg_beta[17], merged_portfolio_1942_46$avg_beta_se[17], merged_portfolio_1942_46$avg_r_squared[17],
                 merged_portfolio_1942_46$avg_return_sd[17], merged_portfolio_1942_46$avg_residual_sd[17], merged_portfolio_1942_46$mean_residual_sd[17], merged_portfolio_1942_46$residual_ratio[17]), 4),
  `18` = round(c(merged_portfolio_1942_46$avg_beta[18], merged_portfolio_1942_46$avg_beta_se[18], merged_portfolio_1942_46$avg_r_squared[18],
                 merged_portfolio_1942_46$avg_return_sd[18], merged_portfolio_1942_46$avg_residual_sd[18], merged_portfolio_1942_46$mean_residual_sd[18], merged_portfolio_1942_46$residual_ratio[18]), 4),
  `19` = round(c(merged_portfolio_1942_46$avg_beta[19], merged_portfolio_1942_46$avg_beta_se[19], merged_portfolio_1942_46$avg_r_squared[19],
                 merged_portfolio_1942_46$avg_return_sd[19], merged_portfolio_1942_46$avg_residual_sd[19], merged_portfolio_1942_46$mean_residual_sd[19], merged_portfolio_1942_46$residual_ratio[19]), 4),
  `20` = round(c(merged_portfolio_1942_46$avg_beta[20], merged_portfolio_1942_46$avg_beta_se[20], merged_portfolio_1942_46$avg_r_squared[20],
                 merged_portfolio_1942_46$avg_return_sd[20], merged_portfolio_1942_46$avg_residual_sd[20], merged_portfolio_1942_46$mean_residual_sd[20], merged_portfolio_1942_46$residual_ratio[20]), 4)
)

# Print the formatted table using knitr::kable without altering values
kable(formatted_table_1942_46, format = "html", col.names = c("Statistic", paste0(11:20))) %>%
  kable_styling(bootstrap_options = c("striped", "hover", "condensed")) %>%
  add_header_above(c(" " = 1, "Portfolios for Estimation Period 1942-46" = 10)) %>%  # Add the title
  row_spec(1, bold = FALSE, italic = FALSE)

start_date <- as.Date("1950-01-01")
end_date <- as.Date("1954-12-31")

# Filter the dataset for the 1950-1954 period
fm_data_1950_54 <- fm_data %>%
  filter(date >= start_date & date <= end_date)

# Check the filtered data
print(head(fm_data_1950_54))
fm_data_1950_54_clean <- fm_data_1950_54 %>%
  filter(!is.na(ret) & !is.na(fsi_rm))

# Function to calculate portfolio statistics (same as before)
calculate_portfolio_statistics <- function(data, num_portfolios = 20) {
  
  # Step 1: Calculate individual security statistics
  betas_and_residuals <- data %>%
    group_by(permno) %>%
    do({
      model <- lm(ret ~ fsi_rm, data = .)
      beta <- coef(model)[2]
      beta_se <- sqrt(vcov(model)[2, 2])  # Standard error of beta
      r_squared <- summary(model)$r.squared
      fitted_vals <- fitted(model)
      residuals_vals <- .$ret - fitted_vals  # Actual - Fitted = Residuals
      residual_sd <- sd(residuals_vals, na.rm = TRUE)
      return_sd <- sd(.$ret, na.rm = TRUE)
      
      data.frame(beta = beta, beta_se = beta_se, r_squared = r_squared,
                 return_sd = return_sd, residual_sd = residual_sd)
    }) %>%
    ungroup()
  
  # Step 2: Assign portfolios based on beta
  betas_and_residuals <- betas_and_residuals %>%
    arrange(beta) %>%
    mutate(Portfolio = ntile(beta, num_portfolios))
  
  # Step 3: Calculate portfolio-level statistics
  portfolio_stats <- betas_and_residuals %>%
    group_by(Portfolio) %>%
    summarize(
      avg_beta = mean(beta, na.rm = TRUE),
      avg_beta_se = mean(beta_se, na.rm = TRUE),
      avg_r_squared = mean(r_squared, na.rm = TRUE),
      avg_return_sd = mean(return_sd, na.rm = TRUE),
      avg_residual_sd = mean(residual_sd, na.rm = TRUE)
    )
  
  # Step 4: Calculate the average residual SD across all portfolios
  overall_avg_residual_sd <- mean(portfolio_stats$avg_residual_sd, na.rm = TRUE)
  
  # Step 5: Calculate the residual ratio for each portfolio
  portfolio_stats <- portfolio_stats %>%
    mutate(residual_ratio = avg_residual_sd / overall_avg_residual_sd)
  
  return(portfolio_stats)
}

# Apply the function to the data for 1950-54
portfolio_estimation_1950_54 <- calculate_portfolio_statistics(fm_data_1950_54_clean)

# View the results
print(portfolio_estimation_1950_54)

# Function to calculate portfolio statistics with Mean s(εi)
calculate_mean_residual_sd <- function(data, num_portfolios = 20) {
  
  # Step 1: Calculate individual security statistics
  betas_and_residuals <- data %>%
    group_by(permno) %>%
    do({
      # Run the regression for each security
      model <- lm(ret ~ fsi_rm, data = .)
      
      # Calculate residuals manually
      fitted_vals <- fitted(model)
      residuals_vals <- .$ret - fitted_vals  # Actual - Fitted = Residuals
      
      # Calculate standard deviation of residuals for each security
      residual_sd <- sd(residuals_vals, na.rm = TRUE)
      
      # Return residual SD for each security
      data.frame(residual_sd = residual_sd)
    }) %>%
    ungroup()
  
  # Step 2: Assign portfolios based on beta (already done in your main code)
  betas_and_residuals <- betas_and_residuals %>%
    arrange(residual_sd) %>%
    mutate(Portfolio = ntile(residual_sd, num_portfolios))  # Assign securities to portfolios
  
  # Step 3: Calculate Mean s(εi) for each portfolio
  portfolio_stats <- betas_and_residuals %>%
    group_by(Portfolio) %>%
    summarize(mean_residual_sd = mean(residual_sd, na.rm = TRUE))  # Mean s(εi) for each portfolio
  
  return(portfolio_stats)
}

# Apply the function to calculate Mean s(εi) for the 1934-1938 period
mean_residual_sd_1950_54 <- calculate_mean_residual_sd(fm_data_1950_54_clean)

# Check the results
print(mean_residual_sd_1950_54)

merged_portfolio_1950_54 <- portfolio_estimation_1950_54 %>%
  left_join(mean_residual_sd_1950_54, by = "Portfolio")

# Check the results
print(merged_portfolio_1950_54)

# For portfolios 1 to 10
formatted_table_1950_54_1_to_10 <- data.frame(
  Statistic = c(
    "Beta (β_{p,t-1})",  # Beta for portfolio
    "s(Beta)",  # Standard error of beta
    "R² (r(Rp, Rm)²)",  # R-squared
    "s(Rp)",  # Standard deviation of portfolio returns
    "s(εp)",  # Standard deviation of residuals
    "Mean s(εi)",  # Average residual standard deviation
    "s(εp) / Mean s(εi)"  # Residual SD ratio
  ),
  `1` = round(c(merged_portfolio_1950_54$avg_beta[1], merged_portfolio_1950_54$avg_beta_se[1], merged_portfolio_1950_54$avg_r_squared[1],
                merged_portfolio_1950_54$avg_return_sd[1], merged_portfolio_1950_54$avg_residual_sd[1], merged_portfolio_1950_54$mean_residual_sd[1], merged_portfolio_1950_54$residual_ratio[1]), 4),
  `2` = round(c(merged_portfolio_1950_54$avg_beta[2], merged_portfolio_1950_54$avg_beta_se[2], merged_portfolio_1950_54$avg_r_squared[2],
                merged_portfolio_1950_54$avg_return_sd[2], merged_portfolio_1950_54$avg_residual_sd[2], merged_portfolio_1950_54$mean_residual_sd[2], merged_portfolio_1950_54$residual_ratio[2]), 4),
  `3` = round(c(merged_portfolio_1950_54$avg_beta[3], merged_portfolio_1950_54$avg_beta_se[3], merged_portfolio_1950_54$avg_r_squared[3],
                merged_portfolio_1950_54$avg_return_sd[3], merged_portfolio_1950_54$avg_residual_sd[3], merged_portfolio_1950_54$mean_residual_sd[3], merged_portfolio_1950_54$residual_ratio[3]), 4),
  `4` = round(c(merged_portfolio_1950_54$avg_beta[4], merged_portfolio_1950_54$avg_beta_se[4], merged_portfolio_1950_54$avg_r_squared[4],
                merged_portfolio_1950_54$avg_return_sd[4], merged_portfolio_1950_54$avg_residual_sd[4], merged_portfolio_1950_54$mean_residual_sd[4], merged_portfolio_1950_54$residual_ratio[4]), 4),
  `5` = round(c(merged_portfolio_1950_54$avg_beta[5], merged_portfolio_1950_54$avg_beta_se[5], merged_portfolio_1950_54$avg_r_squared[5],
                merged_portfolio_1950_54$avg_return_sd[5], merged_portfolio_1950_54$avg_residual_sd[5], merged_portfolio_1950_54$mean_residual_sd[5], merged_portfolio_1950_54$residual_ratio[5]), 4),
  `6` = round(c(merged_portfolio_1950_54$avg_beta[6], merged_portfolio_1950_54$avg_beta_se[6], merged_portfolio_1950_54$avg_r_squared[6],
                merged_portfolio_1950_54$avg_return_sd[6], merged_portfolio_1950_54$avg_residual_sd[6], merged_portfolio_1950_54$mean_residual_sd[6], merged_portfolio_1950_54$residual_ratio[6]), 4),
  `7` = round(c(merged_portfolio_1950_54$avg_beta[7], merged_portfolio_1950_54$avg_beta_se[7], merged_portfolio_1950_54$avg_r_squared[7],
                merged_portfolio_1950_54$avg_return_sd[7], merged_portfolio_1950_54$avg_residual_sd[7], merged_portfolio_1950_54$mean_residual_sd[7], merged_portfolio_1950_54$residual_ratio[7]), 4),
  `8` = round(c(merged_portfolio_1950_54$avg_beta[8], merged_portfolio_1950_54$avg_beta_se[8], merged_portfolio_1950_54$avg_r_squared[8],
                merged_portfolio_1950_54$avg_return_sd[8], merged_portfolio_1950_54$avg_residual_sd[8], merged_portfolio_1950_54$mean_residual_sd[8], merged_portfolio_1950_54$residual_ratio[8]), 4),
  `9` = round(c(merged_portfolio_1950_54$avg_beta[9], merged_portfolio_1950_54$avg_beta_se[9], merged_portfolio_1950_54$avg_r_squared[9],
                merged_portfolio_1950_54$avg_return_sd[9], merged_portfolio_1950_54$avg_residual_sd[9], merged_portfolio_1950_54$mean_residual_sd[9], merged_portfolio_1950_54$residual_ratio[9]), 4),
  `10` = round(c(merged_portfolio_1950_54$avg_beta[10], merged_portfolio_1950_54$avg_beta_se[10], merged_portfolio_1950_54$avg_r_squared[10],
                 merged_portfolio_1950_54$avg_return_sd[10], merged_portfolio_1950_54$avg_residual_sd[10], merged_portfolio_1950_54$mean_residual_sd[10], merged_portfolio_1950_54$residual_ratio[10]), 4)
)

# Print the formatted table using knitr::kable without altering values
kable(formatted_table_1950_54_1_to_10, format = "html", col.names = c("Statistic", paste0(1:10))) %>%
  kable_styling(bootstrap_options = c("striped", "hover", "condensed")) %>%
  add_header_above(c(" " = 1, "Portfolios for Estimation Period 1950-54" = 10)) %>%  # Add the title
  row_spec(1, bold = FALSE, italic = FALSE)


# For portfolios 11 to 20
formatted_table_1950_54_11_to_20 <- data.frame(
  Statistic = c(
    "Beta (β_{p,t-1})",  # Beta for portfolio
    "s(Beta)",  # Standard error of beta
    "R² (r(Rp, Rm)²)",  # R-squared
    "s(Rp)",  # Standard deviation of portfolio returns
    "s(εp)",  # Standard deviation of residuals
    "Mean s(εi)",  # Average residual standard deviation
    "s(εp) / Mean s(εi)"  # Residual SD ratio
  ),
  `11` = round(c(merged_portfolio_1950_54$avg_beta[11], merged_portfolio_1950_54$avg_beta_se[11], merged_portfolio_1950_54$avg_r_squared[11],
                 merged_portfolio_1950_54$avg_return_sd[11], merged_portfolio_1950_54$avg_residual_sd[11], merged_portfolio_1950_54$mean_residual_sd[11], merged_portfolio_1950_54$residual_ratio[11]), 4),
  `12` = round(c(merged_portfolio_1950_54$avg_beta[12], merged_portfolio_1950_54$avg_beta_se[12], merged_portfolio_1950_54$avg_r_squared[12],
                 merged_portfolio_1950_54$avg_return_sd[12], merged_portfolio_1950_54$avg_residual_sd[12], merged_portfolio_1950_54$mean_residual_sd[12], merged_portfolio_1950_54$residual_ratio[12]), 4),
  `13` = round(c(merged_portfolio_1950_54$avg_beta[13], merged_portfolio_1950_54$avg_beta_se[13], merged_portfolio_1950_54$avg_r_squared[13],
                 merged_portfolio_1950_54$avg_return_sd[13], merged_portfolio_1950_54$avg_residual_sd[13], merged_portfolio_1950_54$mean_residual_sd[13], merged_portfolio_1950_54$residual_ratio[13]), 4),
  `14` = round(c(merged_portfolio_1950_54$avg_beta[14], merged_portfolio_1950_54$avg_beta_se[14], merged_portfolio_1950_54$avg_r_squared[14],
                 merged_portfolio_1950_54$avg_return_sd[14], merged_portfolio_1950_54$avg_residual_sd[14], merged_portfolio_1950_54$mean_residual_sd[14], merged_portfolio_1950_54$residual_ratio[14]), 4),
  `15` = round(c(merged_portfolio_1950_54$avg_beta[15], merged_portfolio_1950_54$avg_beta_se[15], merged_portfolio_1950_54$avg_r_squared[15],
                 merged_portfolio_1950_54$avg_return_sd[15], merged_portfolio_1950_54$avg_residual_sd[15], merged_portfolio_1950_54$mean_residual_sd[15], merged_portfolio_1950_54$residual_ratio[15]), 4),
  `16` = round(c(merged_portfolio_1950_54$avg_beta[16], merged_portfolio_1950_54$avg_beta_se[16], merged_portfolio_1950_54$avg_r_squared[16],
                 merged_portfolio_1950_54$avg_return_sd[16], merged_portfolio_1950_54$avg_residual_sd[16], merged_portfolio_1950_54$mean_residual_sd[16], merged_portfolio_1950_54$residual_ratio[16]), 4),
  `17` = round(c(merged_portfolio_1950_54$avg_beta[17], merged_portfolio_1950_54$avg_beta_se[17], merged_portfolio_1950_54$avg_r_squared[17],
                 merged_portfolio_1950_54$avg_return_sd[17], merged_portfolio_1950_54$avg_residual_sd[17], merged_portfolio_1950_54$mean_residual_sd[17], merged_portfolio_1950_54$residual_ratio[17]), 4),
  `18` = round(c(merged_portfolio_1950_54$avg_beta[18], merged_portfolio_1950_54$avg_beta_se[18], merged_portfolio_1950_54$avg_r_squared[18],
                 merged_portfolio_1950_54$avg_return_sd[18], merged_portfolio_1950_54$avg_residual_sd[18], merged_portfolio_1950_54$mean_residual_sd[18], merged_portfolio_1950_54$residual_ratio[18]), 4),
  `19` = round(c(merged_portfolio_1950_54$avg_beta[19], merged_portfolio_1950_54$avg_beta_se[19], merged_portfolio_1950_54$avg_r_squared[19],
                 merged_portfolio_1950_54$avg_return_sd[19], merged_portfolio_1950_54$avg_residual_sd[19], merged_portfolio_1950_54$mean_residual_sd[19], merged_portfolio_1950_54$residual_ratio[19]), 4),
  `20` = round(c(merged_portfolio_1950_54$avg_beta[20], merged_portfolio_1950_54$avg_beta_se[20], merged_portfolio_1950_54$avg_r_squared[20],
                 merged_portfolio_1950_54$avg_return_sd[20], merged_portfolio_1950_54$avg_residual_sd[20], merged_portfolio_1950_54$mean_residual_sd[20], merged_portfolio_1950_54$residual_ratio[20]), 4)
)

# Print the formatted table using knitr::kable without altering values
kable(formatted_table_1950_54_11_to_20, format = "html", col.names = c("Statistic", paste0(11:20))) %>%
  kable_styling(bootstrap_options = c("striped", "hover", "condensed")) %>%
  add_header_above(c(" " = 1, "Portfolios for Estimation Period 1950-54" = 10)) %>%  # Add the title
  row_spec(1, bold = FALSE, italic = FALSE)

#Portfolio for Estimation Period 1958-62

start_date <- as.Date("1958-01-01")
end_date <- as.Date("1962-12-31")

# Filter the dataset for the 1958-1962 period
fm_data_1958_62 <- fm_data %>%
  filter(date >= start_date & date <= end_date)

fm_data_1958_62_clean <- fm_data_1958_62 %>%
  filter(!is.na(ret) & !is.na(fsi_rm))

# Step 3: Function to calculate portfolio statistics for each security
calculate_portfolio_statistics <- function(data, num_portfolios = 20) {
  betas_and_residuals <- data %>%
    group_by(permno) %>%
    do({
      model <- lm(ret ~ fsi_rm, data = .)
      beta <- coef(model)[2]
      beta_se <- sqrt(vcov(model)[2, 2])
      r_squared <- summary(model)$r.squared
      residuals_vals <- .$ret - fitted(model)
      residual_sd <- sd(residuals_vals, na.rm = TRUE)
      return_sd <- sd(.$ret, na.rm = TRUE)
      data.frame(beta = beta, beta_se = beta_se, r_squared = r_squared,
                 return_sd = return_sd, residual_sd = residual_sd)
    }) %>%
    ungroup()
  
  betas_and_residuals <- betas_and_residuals %>%
    arrange(beta) %>%
    mutate(Portfolio = ntile(beta, num_portfolios))
  
  portfolio_stats <- betas_and_residuals %>%
    group_by(Portfolio) %>%
    summarize(
      avg_beta = mean(beta, na.rm = TRUE),
      avg_beta_se = mean(beta_se, na.rm = TRUE),
      avg_r_squared = mean(r_squared, na.rm = TRUE),
      avg_return_sd = mean(return_sd, na.rm = TRUE),
      avg_residual_sd = mean(residual_sd, na.rm = TRUE)
    )
  
  overall_avg_residual_sd <- mean(portfolio_stats$avg_residual_sd, na.rm = TRUE)
  
  portfolio_stats <- portfolio_stats %>%
    mutate(residual_ratio = avg_residual_sd / overall_avg_residual_sd)
  
  return(portfolio_stats)
}

# Step 4: Apply the function to calculate statistics for 1958-62 period
portfolio_estimation_1958_62 <- calculate_portfolio_statistics(fm_data_1958_62_clean)

# Function to calculate portfolio statistics with Mean s(εi)
calculate_mean_residual_sd <- function(data, num_portfolios = 20) {
  
  # Step 1: Calculate individual security statistics
  betas_and_residuals <- data %>%
    group_by(permno) %>%
    do({
      # Run the regression for each security
      model <- lm(ret ~ fsi_rm, data = .)
      
      # Calculate residuals manually
      fitted_vals <- fitted(model)
      residuals_vals <- .$ret - fitted_vals  # Actual - Fitted = Residuals
      
      # Calculate standard deviation of residuals for each security
      residual_sd <- sd(residuals_vals, na.rm = TRUE)
      
      # Return residual SD for each security
      data.frame(residual_sd = residual_sd)
    }) %>%
    ungroup()
  
  # Step 2: Assign portfolios based on beta (already done in your main code)
  betas_and_residuals <- betas_and_residuals %>%
    arrange(residual_sd) %>%
    mutate(Portfolio = ntile(residual_sd, num_portfolios))  
  
  # Step 3: Calculate Mean s(εi) for each portfolio
  portfolio_stats <- betas_and_residuals %>%
    group_by(Portfolio) %>%
    summarize(mean_residual_sd = mean(residual_sd, na.rm = TRUE))  
  
  return(portfolio_stats)
}

# Apply the function to calculate Mean s(εi) for the 1934-1938 period
mean_residual_sd_1958_62 <- calculate_mean_residual_sd(fm_data_1958_62_clean)


