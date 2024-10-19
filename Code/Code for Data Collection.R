## Fama-Macbeth (1973) Replication Code######
# clearing environment 
rm(list = ls())

#Loading necessaay libarary
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
library(gtable)
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


## Stock and Excchage Identifier, selecting NYSE
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


###  Create Fisher Index - for Rm ##
Fs_Idx <- fm_data|> group_by(date)|> summarise(fsi_rm = mean(ret, na.rm = TRUE))

write.csv(fm_data, "fm_data.csv")

## Combine Stock return and market return i.e. fisher index return

fm_data <- fm_data |>select(permno,date,ret)|>inner_join(Fs_Idx, by = c("date"))|> arrange(permno,date)

fm_data <- tbl(MAF900_data, "fm_ret")|> collect()

Fs_Idx <- fm_data |> 
  group_by(date) |> 
  summarise(fsi_rm = mean(ret, na.rm = TRUE))

fm_data <- fm_data |> 
  left_join(Fs_Idx, by = "date")

# Portfolio formation, estimation, and testing periods (it is similar to article for sample period and have extended until 2023 using the general trend, we have extedned tetsing period of eriod 9 to 19-67 to 19-70 as we have data now)
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
  
  summary_table <- tibble(
    period = integer(),
    formation_period = character(),
    estimation_period = character(),
    testing_period = character(),
    total_securities = integer(),
    securities_with_data_formation = integer(),
    securities_with_data_estimation = integer()
  )
  
  for (i in 1:nrow(periods)) {
    print(paste("Processing period:", i))  # Debugging
    
    # Set portfolio formation period
    port_form_bdate <- periods$formation_start[i]
    port_form_edate <- periods$formation_end[i]
    
    # Set estimation period
    est_start <- periods$estimation_start[i]
    est_end <- periods$estimation_end[i]
    
    # Filter the data for the formation period
    formation_data <- fm_data |> 
      filter(between(date, port_form_bdate, port_form_edate)) |> 
      select(permno, date, ret, fsi_rm)
    
    # Filter the data for the estimation period
    estimation_data <- fm_data |> 
      filter(between(date, est_start, est_end)) |> 
      select(permno, date, ret, fsi_rm)
    
    # Calculate the total number of securities in the formation period
    total_stocks <- formation_data |> 
      distinct(permno) |> 
      nrow()
    
    # Calculate the number of securities that meet the formation period data requirement (48 observations)
    securities_with_data_formation <- formation_data |> 
      group_by(permno) |> 
      filter(n() >= 48) |>  # Filter securities with at least 48 observations in formation period
      ungroup() |> 
      distinct(permno) |> 
      nrow()
    
    # Calculate the number of securities that meet the estimation period data requirement (60 observations)
    securities_with_data_estimation <- estimation_data |> 
      group_by(permno) |> 
      filter(n() >= 60) |>  # Filter securities with at least 60 observations in estimation period
      ungroup() |> 
      distinct(permno) |> 
      nrow()
    
    # Add the results to the summary table in the requested format
    summary_table <- summary_table |> add_row(
      period = i,
      formation_period = paste0(year(port_form_bdate), "-", year(port_form_edate)),
      estimation_period = paste0(year(est_start), "-", year(est_end)),
      testing_period = paste0(year(periods$testing_start[i]), "-", year(periods$testing_end[i])),
      total_securities = total_stocks,
      securities_with_data_formation = securities_with_data_formation,
      securities_with_data_estimation = securities_with_data_estimation
    )
  }
  summary_table <- summary_table |> distinct()
  print(summary_table)
 
### Table 1 created.   
### End of table 1.  
 
 
########Table 2 Portfolio for Estimation########
# selecting period as per article 
estimation_periods <- list(
  c("1934-01-01", "1938-12-31"),
  c("1942-01-01", "1946-12-31"),
  c("1950-01-01", "1954-12-31"),
  c("1958-01-01", "1962-12-31")
)
start_date <- as.Date("1934-01-01")
end_date <- as.Date("1938-12-31")

# Filter the dataset for the 1942-1946 period
fm_data_1934_38 <- fm_data %>%
  filter(date >= start_date & date <= end_date)

# Check the filtered data
print(head(fm_data_1934_38))

# Ensure there are no missing values in returns or market returns
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

# Apply the function to calculate all statistics for the 1934-1938 period
portfolio_estimation_1934_38 <- calculate_portfolio_statistics(fm_data_1934_38_clean)

# Check the results
print(portfolio_estimation_1934_38)


# Ensure there are no missing values in returns or market returns
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
mean_residual_sd_1934_38 <- calculate_mean_residual_sd(fm_data_1934_38_clean)

# Check the results
print(mean_residual_sd_1934_38)


# Merge the two data frames
merged_portfolio_1934_38 <- portfolio_estimation_1934_38 %>%
  left_join(mean_residual_sd_1934_38, by = "Portfolio")

# Check the results
print(merged_portfolio_1934_38)

# Specify the path to save the file
file_path <- "/Users/bibekaryal/Desktop/MRes/second Semester/Advanced Data Methods/Group Assignment/Group Project MAF900/Data/merged_portfolio_1934_38.csv"

# Save the dataset as a CSV file
write.csv(merged_portfolio_1934_38, file_path, row.names = FALSE)

#Portfolio for Estimation Period 1942-46

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

# Specify the path to save the file
file_path <- "/Users/bibekaryal/Desktop/MRes/second Semester/Advanced Data Methods/Group Assignment/Group Project MAF900/Data/merged_portfolio_1934_38.csv"

# Save the dataset as a CSV file
write.csv(merged_portfolio_1942_46, file_path, row.names = FALSE)

#Portfolio for Estimation 1950-54

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
  
  # Step 2: Assign portfolios based on beta
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

# Specify the path to save the file
file_path <- "/Users/bibekaryal/Desktop/MRes/second Semester/Advanced Data Methods/Group Assignment/Group Project MAF900/Data/merged_portfolio_1934_38.csv"

# Save the dataset as a CSV file
write.csv(merged_portfolio_1950_54, file_path, row.names = FALSE)


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
  
  # Step 2: Assign portfolios based on beta
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

merged_portfolio_1958_62 <- portfolio_estimation_1958_62 %>%
  left_join(mean_residual_sd_1958_62, by = "Portfolio")

# Specify the path to save the file
file_path <- "/Users/bibekaryal/Desktop/MRes/second Semester/Advanced Data Methods/Group Assignment/Group Project MAF900/Data/merged_portfolio_1934_38.csv"

# Save the dataset as a CSV file
write.csv(merged_portfolio_1958_62, file_path, row.names = FALSE)

### extending to period having world financial crisis
start_date <- as.Date("2006-01-01")
end_date <- as.Date("2010-12-31")

# Filter the dataset for the 2006-2010 period
fm_data_2006_10 <- fm_data %>%
  filter(date >= start_date & date <= end_date)

fm_data_2006_10_clean <- fm_data_2006_10 %>%
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
portfolio_estimation_2006_10 <- calculate_portfolio_statistics(fm_data_2006_10_clean)

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
  
  # Step 2: Assign portfolios based on beta 
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
mean_residual_sd_2006_10 <- calculate_mean_residual_sd(fm_data_2006_10_clean)

merged_portfolio_2006_10 <- portfolio_estimation_2006_10 %>%
  left_join(mean_residual_sd_2006_10, by = "Portfolio")

# Specify the path to save the file
file_path <- "/Users/bibekaryal/Desktop/MRes/second Semester/Advanced Data Methods/Group Assignment/Group Project MAF900/Data/merged_portfolio_1934_38.csv"

# Save the dataset as a CSV file
write.csv(merged_portfolio_2006_10, file_path, row.names = FALSE)


## now adding period with COVID 2019 
start_date <- as.Date("2018-01-01")
end_date <- as.Date("2022-12-31")

# Filter the dataset for the 2018-2022 period
fm_data_2018_22 <- fm_data %>%
  filter(date >= start_date & date <= end_date)

fm_data_2018_22_clean <- fm_data_2018_22 %>%
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
portfolio_estimation_2018_22 <- calculate_portfolio_statistics(fm_data_2018_22_clean)

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
mean_residual_sd_2018_22 <- calculate_mean_residual_sd(fm_data_2018_22_clean)

merged_portfolio_2018_22 <- portfolio_estimation_2018_22 %>%
  left_join(mean_residual_sd_2018_22, by = "Portfolio")

# Specify the path to save the file
file_path <- "/Users/bibekaryal/Desktop/MRes/second Semester/Advanced Data Methods/Group Assignment/Group Project MAF900/Data/merged_portfolio_1934_38.csv"

# Save the dataset as a CSV file
write.csv(merged_portfolio_2018_22, file_path, row.names = FALSE)


############### For Table 3  ###############
  temp <- tempfile(fileext = ".zip")
  download.file("https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_Factors_CSV.zip",temp)
  temp1 <- unzip(temp, exdir = ".")
  
  ff_3factors_monthly <- read.csv(temp1, skip=5, header = F) 
  names(ff_3factors_monthly) <- c('dt', 'rmrf', 'smb', 'hml', 'rf')
  unlink(temp)
  unlink(temp1)
  head(ff_3factors_monthly)
  
  ff_3factors_mon <- ff_3factors_monthly |> 
    filter(nchar(dt) == 6) |> 
    mutate(yr = str_sub(dt,1,4), mon= str_sub(dt,-2,-1),  
           date = make_date(year= yr, month = mon, day = 01),
           mkt_excess = as.numeric(rmrf), smb = as.numeric(smb),
           hml = as.numeric(hml), rf = as.numeric(rf)) |> 
    select(c('date','mkt_excess','smb','hml','rf'))
  ff_3factors_mon <- as_tibble(ff_3factors_mon)
  
  ff_3factors_mon$date <- as.Date(ff_3factors_mon$date)
  ff_3factors_mon$date <- ceiling_date(ff_3factors_mon$date, "month") - days(1)
  ff_3factors_mon$year_month <- format(ff_3factors_mon$date, "%Y-%m")
  
  risk_free<-ff_3factors_mon|>
    select(rf, year_month)|>
    mutate(rf=rf/100)
start_date <- ymd("1935-01-01")
end_date <- ymd("1968-06-30")

# Filter data for the required period
fm_data_period <- fm_data %>%
  filter(date >= start_date & date <= end_date)


estimate_bs <- function(data, min_obs = 60) {
  # Filter out groups that don't have enough observations
  if (nrow(data) < min_obs || sum(!is.na(data$ret)) < min_obs || sum(!is.na(data$mkt)) < min_obs) {
    return(tibble(beta = NA, residual_sd = NA))  # Return NA if there are not enough observations
  }
  
  # Perform the linear regression
  model <- lm(ret ~ mkt, data = data)
  
  # Extract beta (coefficient of the market return)
  beta <- coef(model)[2]
  
  # Calculate residual standard deviation (idiosyncratic risk)
  residuals_vals <- residuals(model)
  residual_sd <- sd(residuals_vals, na.rm = TRUE)
  
  return(tibble(beta = beta, residual_sd = residual_sd))
}

# Function to apply rolling estimation over time
roll_bs_estimation <- function(data, yrs, min_obs) {
  data <- data %>% 
    arrange(date)
  
  betas <- slide_period(
    .x = data,
    .i = data$date,
    .period = "year",
    .f = ~ estimate_bs(., min_obs),
    .before = yrs - 1,
    .complete = FALSE
  )
  dt1 <- data %>% mutate(yr = year(date)) %>% select(yr) %>% distinct()  
  return(tibble(
    date = dt1$yr,
    betas) %>% unnest(betas))
}

p_beta <- fm_data_period %>%
  mutate(mkt = fsi_rm) %>%
  select(permno, date, ret, mkt) %>%
  nest(data = c(date, ret, mkt)) %>%
  mutate(beta = map(data, ~ roll_bs_estimation(., yrs = 5, min_obs = 60))) %>%
  select(permno, beta) %>%
  unnest(beta) %>%
  filter(date >= "1935") %>%
  drop_na()

# View the beta estimation results
print(head(p_beta))

# Create portfolios based on the ranked betas
p_beta <- p_beta %>%
  group_by(date) %>%
  mutate(portfolio = ntile(beta, 20)) %>%
  ungroup()

# View portfolio assignment
print(head(p_beta))

port_bdate <- ymd("1935-01-01")
port_edate <- ymd("1968-06-30")

# Prepare data for portfolio return and risk calculations
for_port_ret <- fm_data_period %>%
  select(permno, date, ret) %>%
  filter(between(date, port_bdate, port_edate)) %>%
  mutate(yr = year(date),    # Create the year variable
         p_yr = yr - 1) %>%  # Create the previous year variable
  filter(!is.na(ret))   

print(head(for_port_ret))


# Calculate portfolio returns by averaging returns for each portfolio for each year
portfolio_returns <- for_port_ret %>%
  inner_join(p_beta, by = c("permno", "yr" = "date")) %>%
  group_by(p_yr, portfolio) %>%
  summarise(portfolio_return = mean(ret, na.rm = TRUE)) %>%
  ungroup()

# View the calculated portfolio returns
print(head(portfolio_returns))

# If beta is not numeric, convert it
p_beta <- p_beta %>%
  mutate(beta = as.numeric(beta))

sum(is.na(p_beta$beta))

portfolio_full_period <- p_beta %>%
  filter(date >= 1935 & date <= 1968) %>%
  filter(!is.na(beta))

# Ensure the portfolio returns are joined with the beta data
portfolio_full_period <- portfolio_returns %>%
  filter(p_yr >= 1935 & p_yr <= 1968) %>%
  inner_join(p_beta, by = c("portfolio", "p_yr" = "date")) %>%
  filter(!is.na(beta), !is.na(portfolio_return))


# Filter data for the full period (1935-6/68)
portfolio_full_period <- portfolio_returns %>%
  filter(p_yr >= 1935 & p_yr <= 1968)

periods <- list(
  list(start = ymd("1935-01-01"), end = ymd("1968-06-30"), label = "1935-6/68"),
  list(start = ymd("1935-01-01"), end = ymd("1945-12-31"), label = "1935-45"),
  list(start = ymd("1946-01-01"), end = ymd("1955-12-31"), label = "1946-55"),
  list(start = ymd("1956-01-01"), end = ymd("1968-06-30"), label = "1956-6/68"),
  list(start = ymd("1935-01-01"), end = ymd("1940-12-31"), label = "1935-40"),
  list(start = ymd("1941-01-01"), end = ymd("1945-12-31"), label = "1941-45"),
  list(start = ymd("1946-01-01"), end = ymd("1950-12-31"), label = "1946-50"),
  list(start = ymd("1951-01-01"), end = ymd("1955-12-31"), label = "1951-55"),
  list(start = ymd("1956-01-01"), end = ymd("1960-12-31"), label = "1956-60"),
  list(start = ymd("1961-01-01"), end = ymd("1968-06-30"), label = "1961-6/68")
)

compute_period_metrics <- function(start_date, end_date, label, risk_free) {
  # Filter the data for the specific period
  fm_data_period <- fm_data %>%
    filter(date >= start_date & date <= end_date)
  
  # Filter the risk-free rate for the corresponding period
  risk_free_period <- risk_free %>%
    filter(year_month >= format(start_date, "%Y-%m") & year_month <= format(end_date, "%Y-%m"))
  
  # Compute the average risk-free rate over the period
  avg_rf <- mean(risk_free_period$rf, na.rm = TRUE)
  
  # Apply the beta estimation as previously defined
  portfolio_full_period <- p_beta %>%
    filter(date >= year(start_date) & date <= year(end_date)) %>%
    filter(!is.na(beta))
  
  # Run the regression for portfolio returns on beta and beta^2
  portfolio_full_period <- portfolio_returns %>%
    filter(p_yr >= year(start_date) & p_yr <= year(end_date)) %>%
    inner_join(p_beta, by = c("portfolio", "p_yr" = "date")) %>%
    filter(!is.na(beta), !is.na(portfolio_return))
  
  # Run the cross-sectional regression of portfolio returns on beta and beta^2
  full_period_model <- lm(portfolio_return ~ beta + I(beta^2), data = portfolio_full_period)
  
  # Extract the regression summary
  full_period_summary <- summary(full_period_model)
  
  # Extract key metrics
  gamma_0 <- coef(full_period_summary)[1, 1]  # Intercept (gamma_0)
  gamma_1 <- coef(full_period_summary)[2, 1]  # Beta coefficient (gamma_1)
  s_gamma_0 <- coef(full_period_summary)[1, 2]  # Standard error of intercept
  s_gamma_1 <- coef(full_period_summary)[2, 2]  # Standard error of beta
  t_gamma_0 <- coef(full_period_summary)[1, 3]  # t-statistic for intercept
  t_gamma_1 <- coef(full_period_summary)[2, 3]  # t-statistic for beta
  r_squared <- full_period_summary$r.squared  # R-squared
  residuals <- full_period_summary$residuals  # Residuals
  s_r_squared <- sd(residuals^2, na.rm = TRUE)  # Standard deviation of R-squared
  
  # Calculate gamma_0 - R_f using the actual average risk-free rate
  gamma_0_minus_rf <- gamma_0 - avg_rf
  t_gamma_0_minus_rf <- gamma_0_minus_rf / s_gamma_0  # t(gamma_0 - R_f)
  
  # Return the results for this period
  return(data.frame(
    Period = label,
    gamma_0 = gamma_0,
    gamma_1 = gamma_1,
    gamma_0_minus_rf = gamma_0_minus_rf,
    s_gamma_0 = s_gamma_0,
    s_gamma_1 = s_gamma_1,
    t_gamma_0 = t_gamma_0,
    t_gamma_1 = t_gamma_1,
    t_gamma_0_minus_rf = t_gamma_0_minus_rf,
    r_squared = r_squared,
    s_r_squared = s_r_squared
  ))
}

# Loop over the periods and compute the results
all_period_results <- lapply(periods, function(period) {
  compute_period_metrics(period$start, period$end, period$label, risk_free)
})

# Combine the results into one table
final_results_panel_A <- do.call(rbind, all_period_results)

file_path <- "C:/test/MAF900/final_result_panel_A.csv"

# Save the dataset as a CSV file
write.csv(final_results_panel_A, file_path, row.names = FALSE)

  # Function to compute the required metrics for each period for Panel B, including actual risk-free rate
  compute_panel_b_metrics <- function(start_date, end_date, label, risk_free) {
    # Filter the data for the specific period and make sure the rows are distinct
    fm_data_period <- fm_data %>%
      filter(date >= start_date & date <= end_date) %>%
      distinct(date, fsi_rm)  # Keep only distinct date and market return values
    
    # Filter the risk-free rate for the corresponding period
    risk_free_period <- risk_free %>%
      filter(year_month >= format(start_date, "%Y-%m") & year_month <= format(end_date, "%Y-%m"))
    
    # Compute the average risk-free rate over the period
    avg_rf <- mean(risk_free_period$rf, na.rm = TRUE)
    
    # Apply the beta estimation as previously defined for Panel B
    portfolio_full_period <- p_beta %>%
      filter(date >= year(start_date) & date <= year(end_date)) %>%
      filter(!is.na(beta)) %>%
      distinct(portfolio, date, .keep_all = TRUE)  # Ensure portfolio and date are unique
    
    # Ensure that the market return (fsi_rm) is available in the data by extracting the year from the date
    portfolio_full_period <- portfolio_returns %>%
      filter(p_yr >= year(start_date) & p_yr <= year(end_date)) %>%
      distinct(portfolio, p_yr, .keep_all = TRUE) %>%  # Ensure uniqueness
      inner_join(p_beta, by = c("portfolio", "p_yr" = "date")) %>%
      left_join(fm_data_period %>% mutate(year = year(date)) %>% distinct(year, fsi_rm), 
                by = c("p_yr" = "year")) %>%  # Join using distinct rows
      filter(!is.na(beta), !is.na(portfolio_return))
    
    # Run the cross-sectional regression for Panel B
    full_period_model_b <- lm(portfolio_return ~ beta + I(beta^2) + I(beta^3), data = portfolio_full_period)
    
    # Extract the regression summary
    full_period_summary_b <- summary(full_period_model_b)
    
    # Extract key metrics for Panel B
    gamma_0 <- coef(full_period_summary_b)[1, 1]  # Intercept (gamma_0)
    gamma_1 <- coef(full_period_summary_b)[2, 1]  # Beta coefficient (gamma_1)
    gamma_2 <- coef(full_period_summary_b)[3, 1]  # Second beta coefficient (gamma_2)
    gamma_3 <- coef(full_period_summary_b)[4, 1]  # Third beta coefficient (gamma_3)
    
    # Standard errors
    s_gamma_0 <- coef(full_period_summary_b)[1, 2]  # s(gamma_0)
    s_gamma_1 <- coef(full_period_summary_b)[2, 2]  # s(gamma_1)
    s_gamma_2 <- coef(full_period_summary_b)[3, 2]  # s(gamma_2)
    
    # t-statistics
    t_gamma_0 <- coef(full_period_summary_b)[1, 3]  # t(gamma_0)
    t_gamma_1 <- coef(full_period_summary_b)[2, 3]  # t(gamma_1)
    
    # R-squared
    r_squared <- full_period_summary_b$r.squared
    
    # Residuals and standard deviation of R-squared
    residuals <- full_period_summary_b$residuals  # Get residuals from the model
    s_r_squared <- sd(residuals^2, na.rm = TRUE)  # Standard deviation of squared residuals
    
    # Calculate gamma_0 - R_f using the actual average risk-free rate
    gamma_0_minus_rf <- gamma_0 - avg_rf
    t_gamma_0_minus_rf <- gamma_0_minus_rf / s_gamma_0  # t(gamma_0 - R_f)
    
    # Return the results for this period (for Panel B)
    return(data.frame(
      Period = label,
      gamma_0 = gamma_0,
      gamma_1 = gamma_1,
      gamma_2 = gamma_2,
      gamma_3 = gamma_3,
      gamma_0_minus_rf = gamma_0_minus_rf,
      s_gamma_0 = s_gamma_0,
      s_gamma_1 = s_gamma_1,
      s_gamma_2 = s_gamma_2,
      t_gamma_0 = t_gamma_0,
      t_gamma_1 = t_gamma_1,
      t_gamma_0_minus_rf = t_gamma_0_minus_rf,
      r_squared = r_squared,
      s_r_squared = s_r_squared
    ))
  }
  
  # Apply the function to each of the periods for Panel B
  all_panel_b_results <- lapply(periods, function(period) {
    compute_panel_b_metrics(period$start, period$end, period$label, risk_free)
  })
  
  # Combine the results into one table for Panel B
  Panel_B <- do.call(rbind, all_panel_b_results)
  
  file_path <- "C:/test/MAF900/Panel_B.csv"
  write.csv(Panel_B, file_path, row.names = FALSE)

  compute_panel_c_metrics <- function(start_date, end_date, label, risk_free) {
    # Filter the data for the specific period
    fm_data_period <- fm_data %>%
      filter(date >= start_date & date <= end_date)
    
    # Filter the risk-free rate for the corresponding period
    risk_free_period <- risk_free %>%
      filter(year_month >= format(start_date, "%Y-%m") & year_month <= format(end_date, "%Y-%m"))
    
    # Compute the average risk-free rate over the period
    avg_rf <- mean(risk_free_period$rf, na.rm = TRUE)
    
    # Apply the beta estimation as previously defined for Panel C
    portfolio_full_period <- p_beta %>%
      filter(date >= year(start_date) & date <= year(end_date)) %>%
      filter(!is.na(beta))
    
    # Ensure that the market return (fsi_rm) is available in the data by extracting the year from the date
    portfolio_full_period <- portfolio_returns %>%
      filter(p_yr >= year(start_date) & p_yr <= year(end_date)) %>%
      inner_join(p_beta, by = c("portfolio", "p_yr" = "date")) %>%
      left_join(fm_data_period %>% mutate(year = year(date)) %>% select(year, fsi_rm), by = c("p_yr" = "year")) %>%
      filter(!is.na(beta), !is.na(portfolio_return))
    
    # Run the cross-sectional regression for Panel C with beta^2 and beta^3
    full_period_model_c <- lm(portfolio_return ~ beta + I(beta^2) + I(beta^3), data = portfolio_full_period)
    
    # Extract the regression summary
    full_period_summary_c <- summary(full_period_model_c)
    
    # Extract key metrics for Panel C
    gamma_0 <- coef(full_period_summary_c)[1, 1]  # Intercept (gamma_0)
    gamma_1 <- coef(full_period_summary_c)[2, 1]  # Beta coefficient (gamma_1)
    gamma_2 <- coef(full_period_summary_c)[3, 1]  # Second beta coefficient (gamma_2)
    gamma_3 <- coef(full_period_summary_c)[4, 1]  # Third beta coefficient (gamma_3)
    
    # Standard errors
    s_gamma_0 <- coef(full_period_summary_c)[1, 2]  # s(gamma_0)
    s_gamma_1 <- coef(full_period_summary_c)[2, 2]  # s(gamma_1)
    s_gamma_3 <- coef(full_period_summary_c)[4, 2]  # s(gamma_3)
    
    # t-statistics
    t_gamma_0 <- coef(full_period_summary_c)[1, 3]  # t(gamma_0)
    t_gamma_1 <- coef(full_period_summary_c)[2, 3]  # t(gamma_1)
    t_gamma_3 <- coef(full_period_summary_c)[4, 3]  # t(gamma_3)
    
    # R-squared
    r_squared <- full_period_summary_c$r.squared
    
    # Residuals and standard deviation of R-squared
    residuals <- full_period_summary_c$residuals  # Get residuals from the model
    s_r_squared <- sd(residuals^2, na.rm = TRUE)  # Standard deviation of squared residuals
    
    # Calculate gamma_0 - R_f using the actual average risk-free rate
    gamma_0_minus_rf <- gamma_0 - avg_rf
    t_gamma_0_minus_rf <- gamma_0_minus_rf / s_gamma_0  # t(gamma_0 - R_f)
    
    # Return the results for this period (for Panel C)
    return(data.frame(
      Period = label,
      gamma_0 = gamma_0,
      gamma_1 = gamma_1,
      gamma_2 = gamma_2,
      gamma_3 = gamma_3,
      gamma_0_minus_rf = gamma_0_minus_rf,
      s_gamma_0 = s_gamma_0,
      s_gamma_1 = s_gamma_1,
      s_gamma_3 = s_gamma_3,
      t_gamma_0 = t_gamma_0,
      t_gamma_1 = t_gamma_1,
      t_gamma_3 = t_gamma_3,
      t_gamma_0_minus_rf = t_gamma_0_minus_rf,
      r_squared = r_squared,
      s_r_squared = s_r_squared
    ))
  }
  
  # Apply the function to each of the periods for Panel C
  all_panel_c_results <- lapply(periods, function(period) {
    compute_panel_c_metrics(period$start, period$end, period$label, risk_free)
  })
  
  # Combine the results into one table for Panel C
  Panel_c <- do.call(rbind, all_panel_c_results)
  
  # Display the final results for Panel C
  print(Panel_c)

# Function to compute the required metrics for Panel D, including actual risk-free rate
compute_panel_d_metrics <- function(start_date, end_date, label, risk_free) {
  # Filter the data for the specific period
  fm_data_period <- fm_data %>%
    filter(date >= start_date & date <= end_date)
  
  # Filter the risk-free rate for the corresponding period
  risk_free_period <- risk_free %>%
    filter(year_month >= format(start_date, "%Y-%m") & year_month <= format(end_date, "%Y-%m"))

  # Compute the average risk-free rate over the period
  avg_rf <- mean(risk_free_period$rf, na.rm = TRUE)
  
  # Apply the beta estimation as previously defined for Panel D
  portfolio_full_period <- p_beta %>%
    filter(date >= year(start_date) & date <= year(end_date)) %>%
    filter(!is.na(beta))
  
  # Ensure that the market return (fsi_rm) is available in the data by extracting the year from the date
  portfolio_full_period <- portfolio_returns %>%
    filter(p_yr >= year(start_date) & p_yr <= year(end_date)) %>%
    inner_join(p_beta, by = c("portfolio", "p_yr" = "date")) %>%
    left_join(fm_data_period %>% mutate(year = year(date)) %>% select(year, fsi_rm), by = c("p_yr" = "year")) %>%
    filter(!is.na(beta), !is.na(portfolio_return))
  
  # Run the cross-sectional regression for Panel D with beta^2 and beta^3
  full_period_model_d <- lm(portfolio_return ~ beta + I(beta^2) + I(beta^3), data = portfolio_full_period)
  
  # Extract the regression summary
  full_period_summary_d <- summary(full_period_model_d)
  
  # Extract key metrics for Panel D
  gamma_0 <- coef(full_period_summary_d)[1, 1]  # Intercept (gamma_0)
  gamma_1 <- coef(full_period_summary_d)[2, 1]  # Beta coefficient (gamma_1)
  gamma_2 <- coef(full_period_summary_d)[3, 1]  # Second beta coefficient (gamma_2)
  gamma_3 <- coef(full_period_summary_d)[4, 1]  # Third beta coefficient (gamma_3)
  
  # Standard errors
  s_gamma_0 <- coef(full_period_summary_d)[1, 2]  # s(gamma_0)
  s_gamma_1 <- coef(full_period_summary_d)[2, 2]  # s(gamma_1)
  s_gamma_3 <- coef(full_period_summary_d)[4, 2]  # s(gamma_3)
  
  # t-statistics
  t_gamma_0 <- coef(full_period_summary_d)[1, 3]  # t(gamma_0)
  t_gamma_1 <- coef(full_period_summary_d)[2, 3]  # t(gamma_1)
  t_gamma_3 <- coef(full_period_summary_d)[4, 3]  # t(gamma_3)
  
  # R-squared
  r_squared <- full_period_summary_d$r.squared
  
  # Residuals and standard deviation of R-squared
  residuals <- full_period_summary_d$residuals  # Get residuals from the model
  s_r_squared <- sd(residuals^2, na.rm = TRUE)  # Standard deviation of squared residuals
  
  # Calculate gamma_0 - R_f using the actual average risk-free rate
  gamma_0_minus_rf <- gamma_0 - avg_rf
  t_gamma_0_minus_rf <- gamma_0_minus_rf / s_gamma_0  # t(gamma_0 - R_f)
  
  # Return the results for this period (for Panel D)
  return(data.frame(
    Period = label,
    gamma_0 = gamma_0,
    gamma_1 = gamma_1,
    gamma_2 = gamma_2,
    gamma_3 = gamma_3,
    gamma_0_minus_rf = gamma_0_minus_rf,
    s_gamma_0 = s_gamma_0,
    s_gamma_1 = s_gamma_1,
    s_gamma_3 = s_gamma_3,
    t_gamma_0 = t_gamma_0,
    t_gamma_1 = t_gamma_1,
    t_gamma_3 = t_gamma_3,
    t_gamma_0_minus_rf = t_gamma_0_minus_rf,
    r_squared = r_squared,
    s_r_squared = s_r_squared
  ))
}

# Apply the function to each of the periods for Panel D
all_panel_d_results <- lapply(periods, function(period) {
  compute_panel_d_metrics(period$start, period$end, period$label, risk_free)
})

# Combine the results into one table for Panel D
Panel_D <- do.call(rbind, all_panel_d_results)

# Display the final results for Panel D
print(Panel_D)

############################# initial code for table 4 #################

 CONSTRUCT TABLE 4
#1. need to calculate port returns' mean and sd 
#sAVE IT AS TIBBLE

#TABLE 4 SUMMARIES
#SUMMARY 1 - AVERAGE AND STANDARD DEVIATION OF LAMBDAS
lambdas_with_rf$no_grouping <- "1935 to 1968"

summary1<-lambdas_with_rf |>
  group_by("1935 to 1968")|>
  summarise(across(
    .cols = c(lambda0, lambda1, lambda0rf, rsquare), 
    .fns = list(Mean = mean, SD = sd),  
    .names = "{col}_{fn}"))

summary2<-lambdas_with_rf |>
  group_by(out)|>
  summarise(across(
    .cols = c(lambda0, lambda1, lambda0rf, rsquare), 
    .fns = list(Mean = mean, SD = sd),  
    .names = "{col}_{fn}"))

summary3<-lambdas_with_rf |>
  group_by(elt)|>
  summarise(across(
    .cols = c(lambda0, lambda1, lambda0rf, rsquare), 
    .fns = list(Mean = mean, SD = sd),  
    .names = "{col}_{fn}"))


colnames(summary3)[1] <- "Period"
colnames(summary2)[1] <- "Period"
colnames(summary1)[1] <- "Period"

summary_total <- bind_rows(summary1,summary2,summary3)

#1. need to calculate port returns' mean and sd 
risk_free<-ff_3factors_mon|>
  select(rf, year_month)

stacked_data <- read.csv("stacked_data_cross_sectional_regression.csv")
stacked_data <- as_tibble(stacked_data)

stacked_data$no_grouping <- "1935 to 1968"
stacked_data$date <- as.Date(stacked_data$date, format = "%d/%m/%Y")
stacked_data$year_month <- format(stacked_data$date, "%Y-%m")

stacked_data <- stacked_data |> left_join(risk_free, by = "year_month")|>
  mutate(rf=rf/100)
stacked_data$rm_minus_rf <- stacked_data$mkt_Port_Mean - stacked_data$rf

names(stacked_data)

table4_test1 <- stacked_data |> group_by("1935 to 1968") |> 
  summarise(mkt_ret = mean(mkt_Port_Mean),rm_minus_rf = mean(rm_minus_rf) ,rf_mean = mean(rf), market_sharpe_ratio = mean(rm_minus_rf)/sd(mkt_Port_Mean), mkt_ret_sd = sd(mkt_Port_Mean),rf_sd = sd(rf))
table4_test2 <- stacked_data |> group_by(subperiodlarge) |> 
  summarise(mkt_ret = mean(mkt_Port_Mean),rm_minus_rf = mean(rm_minus_rf) ,rf_mean = mean(rf), market_sharpe_ratio = mean(rm_minus_rf)/sd(mkt_Port_Mean), mkt_ret_sd = sd(mkt_Port_Mean),rf_sd = sd(rf))
table4_test3 <- stacked_data |> group_by(subperiodsmall) |> 
  summarise(mkt_ret = mean(mkt_Port_Mean),rm_minus_rf = mean(rm_minus_rf) ,rf_mean = mean(rf), market_sharpe_ratio = mean(rm_minus_rf)/sd(mkt_Port_Mean), mkt_ret_sd = sd(mkt_Port_Mean),rf_sd = sd(rf))

colnames(table4_test1)[1] <- "Period"
colnames(table4_test2)[1] <- "Period"
colnames(table4_test3)[1] <- "Period"


summary_rm_and_rf <- bind_rows(table4_test1,table4_test2,table4_test3)
summary_rm_and_rf

summary_total
summary_rm_and_rf
names(summary_total)
names(summary_rm_and_rf)

Table4_summary <- left_join(summary_total, summary_rm_and_rf, by = "Period")
Table4_summary$lambda1_Mean_by_mkt_ret_sd <- Table4_summary$lambda1_Mean/Table4_summary$mkt_ret_sd

Table4_summary1 <- Table4_summary|> select(Period, mkt_ret, rm_minus_rf, lambda1_Mean, lambda0_Mean,
                                           rf_mean, market_sharpe_ratio, lambda1_Mean_by_mkt_ret_sd, mkt_ret_sd, 
                                           lambda1_SD, lambda0_SD, rf_sd)
Table4_summary1

#DESIGN FUNCTION FOR CALCULATING T-STATS ON LAMBDAS AND FIRST ORDER AUTO-CORRELATION OF LAMBDAS
lambdas_with_rf_new <- stacked_data |>  filter(port == 1) |> left_join(lambdas_with_rf, by = c("year_month","date", "rf", "no_grouping")) |>
  select(date, out, elt, lambda0, lambda1, rsquare, year_month, rf, lambda0rf, no_grouping, mkt_Port_Mean, rm_minus_rf, )
lambdas_with_rf_new$obs <- 1
lambdas_with_rf_new |> mutate(mkt_ret_tstat = (mean(mkt_Port_Mean)*sqrt(sum(obs)))/(sd(mkt_Port_Mean)))
names(lambdas_with_rf_new)
lambdas_with_rf_new$no_grouping

for_tstat1 <- lambdas_with_rf_new |> group_by(no_grouping) |> 
  summarise(mkt_ret_tstat = (mean(mkt_Port_Mean)*sqrt(sum(obs)))/(sd(mkt_Port_Mean)), 
            rm_minus_rf_tstat = (mean(rm_minus_rf)*sqrt(sum(obs)))/(sd(rm_minus_rf)),
            lambda1_tstat = (mean(lambda1)*sqrt(sum(obs)))/(sd(lambda1)), 
            lambda0_tstat = (mean(lambda0)*sqrt(sum(obs)))/(sd(lambda0)),
            corr_mkt_ret = cor(mkt_Port_Mean,lag(mkt_Port_Mean), use = "na.or.complete"),
            corr_rm_minu_rf = cor(rm_minus_rf,lag(rm_minus_rf), use = "na.or.complete"),
            corr_lambda1 = cor(lambda1,lag(lambda1), use = "na.or.complete"),
            corr_lambda0 = cor(lambda0,lag(lambda0), use = "na.or.complete"),
            corr_rf = cor(rf,lag(rf), use = "na.or.complete"))

for_tstat2 <- lambdas_with_rf_new |> group_by(out) |> 
  summarise(mkt_ret_tstat = (mean(mkt_Port_Mean)*sqrt(sum(obs)))/(sd(mkt_Port_Mean)), 
            rm_minus_rf_tstat = (mean(rm_minus_rf)*sqrt(sum(obs)))/(sd(rm_minus_rf)),
            lambda1_tstat = (mean(lambda1)*sqrt(sum(obs)))/(sd(lambda1)), 
            lambda0_tstat = (mean(lambda0)*sqrt(sum(obs)))/(sd(lambda0)),
            corr_mkt_ret = cor(mkt_Port_Mean,lag(mkt_Port_Mean), use = "na.or.complete"),
            corr_rm_minu_rf = cor(rm_minus_rf,lag(rm_minus_rf), use = "na.or.complete"),
            corr_lambda1 = cor(lambda1,lag(lambda1), use = "na.or.complete"),
            corr_lambda0 = cor(lambda0,lag(lambda0), use = "na.or.complete"),
            corr_rf = cor(rf,lag(rf), use = "na.or.complete"))

for_tstat3 <- lambdas_with_rf_new |> group_by(elt) |> 
  summarise(mkt_ret_tstat = (mean(mkt_Port_Mean)*sqrt(sum(obs)))/(sd(mkt_Port_Mean)), 
            rm_minus_rf_tstat = (mean(rm_minus_rf)*sqrt(sum(obs)))/(sd(rm_minus_rf)),
            lambda1_tstat = (mean(lambda1)*sqrt(sum(obs)))/(sd(lambda1)), 
            lambda0_tstat = (mean(lambda0)*sqrt(sum(obs)))/(sd(lambda0)),
            corr_mkt_ret = cor(mkt_Port_Mean,lag(mkt_Port_Mean), use = "na.or.complete"),
            corr_rm_minu_rf = cor(rm_minus_rf,lag(rm_minus_rf), use = "na.or.complete"),
            corr_lambda1 = cor(lambda1,lag(lambda1), use = "na.or.complete"),
            corr_lambda0 = cor(lambda0,lag(lambda0), use = "na.or.complete"),
            corr_rf = cor(rf,lag(rf), use = "na.or.complete"))

colnames(for_tstat1)[1] <- "Period"
colnames(for_tstat2)[1] <- "Period"
colnames(for_tstat3)[1] <- "Period"

tstat_summary <- bind_rows(for_tstat1,for_tstat2,for_tstat3)

names(summary_total)
names(Table4_summary1)
names(tstat_summary)

Table4_summary1 <- Table4_summary1 |> select(Period, mkt_ret, rm_minus_rf, lambda1_Mean, lambda0_Mean, rf_mean,
                                             market_sharpe_ratio, lambda1_Mean_by_mkt_ret_sd, mkt_ret_sd, lambda1_SD, lambda0_SD, rf_sd)
tstat_summary <- tstat_summary |> select(Period, mkt_ret_tstat, rm_minus_rf_tstat, lambda1_tstat, lambda0_tstat, 
                                         corr_mkt_ret, corr_rm_minu_rf, corr_lambda1, corr_lambda0, corr_rf)
Table4_total <- left_join(Table4_summary1, tstat_summary, by ="Period")
Table4_total




