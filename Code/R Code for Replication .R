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

# Connect with WRDS
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


## selecting NYSE, Stock returns
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


###  Create Fisher Index
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

# Portfolio formation, estimation, and testing periods 
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
    No_of_Securities_Available = integer(),
    No_of_Securities_meeting_data_requirement = integer()
  )
  
  for (i in 1:nrow(periods)) {
    print(paste("Processing period:", i))  
    
    # Portfolio formation period
    port_form_bdate <- as.Date(periods$formation_start[i])
    port_form_edate <- as.Date(periods$formation_end[i])
    
    # Estimation period
    est_start <- as.Date(periods$estimation_start[i])
    est_end <- as.Date(periods$estimation_end[i])
    
    formation_data <- fm_data |> 
      filter(between(date, port_form_bdate, port_form_edate)) |> 
      select(permno, date, ret, fsi_rm)
    
    estimation_data <- fm_data |> 
      filter(between(date, est_start, est_end)) |> 
      select(permno, date, ret, fsi_rm)
    
    # Calculate total stocks (all securities available during the formation period)
    total_stocks <- formation_data |> 
      distinct(permno) |> 
      nrow()
    
    # Calculate the number of securities that meet the data requirement (at least 48 observations)
    securities_with_data_formation <- formation_data |> 
      group_by(permno) |> 
      filter(n() >= 48) |>  # Filter securities with at least 48 observations in formation period
      ungroup() |> 
      distinct(permno) |> 
      nrow()
    
    # Add row to the summary table
    summary_table <- summary_table |> add_row(
      period = i,
      formation_period = paste0(year(port_form_bdate), "-", year(port_form_edate)),
      estimation_period = paste0(year(est_start), "-", year(est_end)),
      testing_period = paste0(year(periods$testing_start[i]), "-", year(periods$testing_end[i])),
      No_of_Securities_Available = total_stocks,
      No_of_Securities_meeting_data_requirement = securities_with_data_formation
    )
  }

  # Transpose the summary table, keeping the original row names as column headers
  transposed_table <- as.data.frame(t(summary_table), stringsAsFactors = FALSE)
  
  # Rename the columns using the first row as column headers, then remove the first row
  colnames(transposed_table) <- transposed_table[1, ]
  transposed_table <- transposed_table[-1, ]
  
  # Display the transposed table using knitr's kable
  kable(transposed_table, caption = "Table 1 Portfolio Formation, Estimation and Testing Period") %>%
    kable_styling(bootstrap_options = c("striped", "hover", "condensed"), full_width = FALSE)
  
### Table 1 created.   
### End of table 1.
 
 
########Table 2 Portfolio for Estimation########
# selecting period 
estimation_periods <- list(
  c("1934-01-01", "1938-12-31"),
  c("1942-01-01", "1946-12-31"),
  c("1950-01-01", "1954-12-31"),
  c("1958-01-01", "1962-12-31")
)
start_date <- as.Date("1934-01-01")
end_date <- as.Date("1938-12-31")

#### 1942-1946 period
fm_data_1934_38 <- fm_data %>%
  filter(date >= start_date & date <= end_date)

fm_data_1934_38_clean <- fm_data_1934_38 %>%
  filter(!is.na(ret) & !is.na(fsi_rm))

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
     
      residual_sd <- sd(residuals_vals, na.rm = TRUE)
     
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
mean_residual_sd_1934_38 <- calculate_mean_residual_sd(fm_data_1934_38_clean)

merged_portfolio_1934_38 <- portfolio_estimation_1934_38 %>%
  left_join(mean_residual_sd_1934_38, by = "Portfolio")

# Specify the path to save the file
file_path <- "/Users/bibekaryal/Desktop/MRes/second Semester/Advanced Data Methods/Group Assignment/Group Project MAF900/Data/merged_portfolio_1934_38.csv"

write.csv(merged_portfolio_1934_38, file_path, row.names = FALSE)

#Portfolio for Estimation Period 1942-46
start_date <- as.Date("1942-01-01")
end_date <- as.Date("1946-12-31")

# Filter the dataset for the 1942-1946 period
fm_data_1942_46 <- fm_data %>%
  filter(date >= start_date & date <= end_date)

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
mean_residual_sd_1942_46 <- calculate_mean_residual_sd(fm_data_1942_46_clean)

merged_portfolio_1942_46 <- portfolio_estimation_1942_46 %>%
  left_join(mean_residual_sd_1942_46, by = "Portfolio")

# Specify the path to save the file
file_path <- "/Users/bibekaryal/Desktop/MRes/second Semester/Advanced Data Methods/Group Assignment/Group Project MAF900/Data/merged_portfolio_1934_38.csv"

# Save the dataset as a CSV file
write.csv(merged_portfolio_1942_46, file_path, row.names = FALSE)

########Portfolio for Estimation 1950-54

start_date <- as.Date("1950-01-01")
end_date <- as.Date("1954-12-31")
fm_data_1950_54 <- fm_data %>%
  filter(date >= start_date & date <= end_date)

fm_data_1950_54_clean <- fm_data_1950_54 %>%
  filter(!is.na(ret) & !is.na(fsi_rm))

# Function to calculate portfolio statistics
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

merged_portfolio_1950_54 <- portfolio_estimation_1950_54 %>%
  left_join(mean_residual_sd_1950_54, by = "Portfolio")

# Specify the path to save the file
file_path <- "/Users/bibekaryal/Desktop/MRes/second Semester/Advanced Data Methods/Group Assignment/Group Project MAF900/Data/merged_portfolio_1934_38.csv"

# Save the dataset as a CSV file
write.csv(merged_portfolio_1950_54, file_path, row.names = FALSE)


########Portfolio for Estimation Period 1958-62
start_date <- as.Date("1958-01-01")
end_date <- as.Date("1962-12-31")

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

######## now adding period with COVID 2019 
start_date <- as.Date("2018-01-01")
end_date <- as.Date("2022-12-31")

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

# Create portfolios based on the ranked betas
p_beta <- p_beta %>%
  group_by(date) %>%
  mutate(portfolio = ntile(beta, 20)) %>%
  ungroup()

port_bdate <- ymd("1935-01-01")
port_edate <- ymd("1968-06-30")

# Prepare data for portfolio return and risk calculations
for_port_ret <- fm_data_period %>%
  select(permno, date, ret) %>%
  filter(between(date, port_bdate, port_edate)) %>%
  mutate(yr = year(date),    # Create the year variable
         p_yr = yr - 1) %>%  # Create the previous year variable
  filter(!is.na(ret))   

# Calculate portfolio returns by averaging returns for each portfolio for each year
portfolio_returns <- for_port_ret %>%
  inner_join(p_beta, by = c("permno", "yr" = "date")) %>%
  group_by(p_yr, portfolio) %>%
  summarise(portfolio_return = mean(ret, na.rm = TRUE)) %>%
  ungroup()

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

  # Function to compute the required metrics for each period for Panel B
  compute_panel_b_metrics <- function(start_date, end_date, label, risk_free) {
    # Filter the data for the specific period and make sure the rows are distinct
    fm_data_period <- fm_data %>%
      filter(date >= start_date & date <= end_date) %>%
      distinct(date, fsi_rm)  # Keep only distinct date and market return values
  
    risk_free_period <- risk_free %>%
      filter(year_month >= format(start_date, "%Y-%m") & year_month <= format(end_date, "%Y-%m"))
    
    # Compute the average risk-free rate over the period
    avg_rf <- mean(risk_free_period$rf, na.rm = TRUE)
    
    # Apply the beta estimation as previously defined for Panel B
    portfolio_full_period <- p_beta %>%
      filter(date >= year(start_date) & date <= year(end_date)) %>%
      filter(!is.na(beta)) %>%
      distinct(portfolio, date, .keep_all = TRUE)  # Ensure portfolio and date are unique
   
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
 
  all_panel_c_results <- lapply(periods, function(period) {
    compute_panel_c_metrics(period$start, period$end, period$label, risk_free)
  })
  
  # Combine the results into one table for Panel C
  Panel_c <- do.call(rbind, all_panel_c_results)
  
  print(Panel_c)

  # Function to compute the required metrics for Panel D, using actual risk-free rate from data
  compute_panel_d_metrics <- function(start_date, end_date, label, risk_free) {
    # Filter fm_data for the specific period and ensure uniqueness
    fm_data_period <- fm_data %>%
      filter(date >= start_date & date <= end_date) %>%
      distinct(date, fsi_rm)  # Select distinct dates and market returns
    
    # Filter risk-free data for the corresponding period
    risk_free_period <- risk_free %>%
      filter(year_month >= format(start_date, "%Y-%m") & year_month <= format(end_date, "%Y-%m"))
    
    # Compute the average risk-free rate over the period
    avg_rf <- mean(risk_free_period$rf, na.rm = TRUE)
   
    p_beta_period <- p_beta %>%
      filter(date >= year(start_date) & date <= year(end_date)) %>%
      distinct(portfolio, date, .keep_all = TRUE)  # Ensure unique portfolios and dates
    
    # Ensure unique portfolio_returns and join with p_beta
    portfolio_full_period <- portfolio_returns %>%
      filter(p_yr >= year(start_date) & p_yr <= year(end_date)) %>%
      distinct(portfolio, p_yr, .keep_all = TRUE) %>%  # Ensure uniqueness before joining
      inner_join(p_beta_period, by = c("portfolio", "p_yr" = "date")) %>%
      left_join(fm_data_period %>% mutate(year = year(date)) %>% distinct(year, fsi_rm), 
                by = c("p_yr" = "year")) %>%
      filter(!is.na(beta), !is.na(portfolio_return))
  
    # print(portfolio_full_period %>% group_by(portfolio, p_yr) %>% summarise(n = n()) %>% filter(n > 1))
    
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
 
  print(Panel_D)



#### Complete  code for table 4 
crsp_query<- tbl(wrds, sql("select * from crsp.msf")) |>
  filter(date >= '1926-08-01' & date <= '1968-06-30') |>
  select(permno, date, ret) |> 
  collect()

crsp_query_msenames<- tbl(wrds, sql("select * from crsp.msenames")) |>
  select(permno, primexch, shrcd) |> collect() |> unique()

full_data <- crsp_query |> 
  left_join(
    crsp_query_msenames, 
    by = c('permno'), keep=FALSE
  ) |> 
  filter(primexch == 'N')|>
  filter(shrcd == 10)

full_data$year_month <- format(full_data$date, "%Y-%m")

## Combine Stock return and market return i.e. fisher index return ##
full_data <- full_data |>select(permno,date,ret)|>inner_join(Fs_Idx, by = c("date"))|> arrange(permno,date)

# Convert 'date' in both dataframes to year-month format
full_data <- full_data |> mutate(date = as.Date(format(date, "%Y-%m-01")))  # Convert to first day of the month
ff_3factors_mon <- ff_3factors_mon |> mutate(date = as.Date(format(date, "%Y-%m-01")))

## Merge CRSP data with Fama-French factors on 'year_month'
Data <- full_data |> 
  left_join(ff_3factors_mon |> select(rf, mkt_excess, date), by = 'date') |>
  mutate(rf = rf / 100)# Convert rf to percentage

### Defining period as per article description
# Define short periods (non-overlapping)
short_periods <- list(
  list(start = ymd("1935-01-01"), end = ymd("1940-12-31"), label = "1935-40"),
  list(start = ymd("1941-01-01"), end = ymd("1945-12-31"), label = "1941-45"),
  list(start = ymd("1946-01-01"), end = ymd("1950-12-31"), label = "1946-50"),
  list(start = ymd("1951-01-01"), end = ymd("1955-12-31"), label = "1951-55"),
  list(start = ymd("1956-01-01"), end = ymd("1960-12-31"), label = "1956-60"),
  list(start = ymd("1961-01-01"), end = ymd("1968-06-30"), label = "1961-6/68")
)

# Define long periods (overlapping), leaving 1935-6/68 to be assigned last to ensure all period are assigned
long_periods <- list(
  list(start = ymd("1935-01-01"), end = ymd("1945-12-31"), label = "1935-45"),
  list(start = ymd("1946-01-01"), end = ymd("1955-12-31"), label = "1946-55"),
  list(start = ymd("1956-01-01"), end = ymd("1968-06-30"), label = "1956-6/68")
)

# Function to assign periods based on the defined time ranges
assign_period <- function(data, periods, period_column) {
  data[[period_column]] <- NA  # Create a placeholder column for the new period labels
  
  for (period in periods) {
    # Assign periods where the dates fall into the defined range
    data[[period_column]][data$date >= period$start & data$date <= period$end] <- period$label
  }
  
  return(data)
}

# Apply the period assignment function for both short and long periods, ensuring the "1935-6/68" is assigned last
Data <- assign_period(Data, short_periods, "short_period")
Data <- assign_period(Data, long_periods, "long_period")

# Now reassign the full long period 1935-6/68 at the end to ensure it captures all data from 1935-06/68
Data <- assign_period(Data, list(list(start = ymd("1935-01-01"), end = ymd("1968-06-30"), label = "1935-6/68")), "full_period")

# Check the results
table(Data$short_period)  # Counts for short periods
table(Data$long_period)   # Counts for long periods
table(Data$full_period) # counts for full period

# Apply rolling window beta estimation to each stock
# Beta estimation using market excess return, ensuring minimum observations and handling NA values
estimate_beta <- function(data, min_obs = 60) {
  # Remove rows with NA values in ret or mkt_excess
  data <- data[complete.cases(data$ret, data$mkt_excess), ]
  
  # Require a minimum number of non-NA observations
  if (nrow(data) < min_obs) {
    return(NA)  # Not enough data to estimate beta
  } else {
    # Regress stock return on market excess return to get beta
    fit <- lm(ret ~ mkt_excess, data = data)
    return(coef(fit)[2])  # Return the beta estimate (slope coefficient)
  }
}
# Apply the sliding window beta calculation
Data <- Data |>
  group_by(permno) |>
  mutate(beta = slider::slide_dbl(.x = cur_data(), 
                                  .f = estimate_beta, 
                                  .before = 59,  # Use the past 60 months
                                  min_obs = 60)) |>  # Require at least 60 months of data
  ungroup()

# Assign portfolios based on beta, Here into 20 portfolios based on their beta ranking 
Data <- Data[complete.cases(Data), ]
Data <- Data |>
  group_by(short_period, date) |>
  mutate(portfolio = ntile(beta, 20)) |>  # Divide into 20 portfolios based on beta
  ungroup()

# View portfolio assignments
head(Data)

# Calculate portfolio returns and standard deviations for each portfolio in each period
summary_portfolios <- Data |>
  group_by(date, portfolio) |>
  summarise(
    port_ret = mean(fsi_rm-rf, na.rm = TRUE),  # Portfolio mean return
    port_ret_sd = sd(fsi_rm-rf, na.rm = TRUE)  # Portfolio return standard deviation
  ) |>
  ungroup()

# View the portfolio summaries
head(summary_portfolios)

# Merge `summary_portfolios` with the original data to include `beta` and other variables
summary_portfolios <- summary_portfolios |>
  left_join(Data |> select(date, portfolio, beta), by = c("date", "portfolio"))

# Function to estimate gamma coefficients using cross-sectional regression
estimate_gamma <- function(data) {
  fit <- lm(port_ret ~ beta + I(beta^2) + port_ret_sd, data = data)  # Regress portfolio returns on beta
  return(tibble(
    gamma0 = coef(fit)[1], 
    gamma1 = coef(fit)[2], 
    gamma2 = coef(fit)[3], 
    gamma3 = coef(fit)[4], 
    rsquare = summary(fit)$r.squared
  ))
}

# Now apply the gamma estimation for each year_month
gamma_results <- summary_portfolios |>
  group_by(date) |>
  summarise(gamma_values = estimate_gamma(cur_data())) |>
  ungroup()
gamma_results <- gamma_results |>
  unnest_wider(gamma_values)

# Now calculate the T-statistics for each gamma coefficient
gamma_tstat <- gamma_results |>
  mutate(
    gamma0_tstat = gamma0 / sd(gamma0, na.rm = TRUE),  # T-stat for gamma0
    gamma1_tstat = gamma1 / sd(gamma1, na.rm = TRUE),  # T-stat for gamma1
    gamma2_tstat = gamma2 / sd(gamma2, na.rm = TRUE),  # T-stat for gamma2
    gamma3_tstat = gamma3 / sd(gamma3, na.rm = TRUE)   # T-stat for gamma3
  )

# include portfolio returns (port_ret) in the gamma_results dataset
gamma_results <- gamma_results |>
  left_join(summary_portfolios |> select(date, portfolio, port_ret), by = c("date"))

# Now calculate first-order autocorrelation for portfolio returns and gamma coefficients
gamma_results <- gamma_results |>
  group_by(portfolio) |>
  mutate(
    return_autocorr = ifelse(sum(!is.na(port_ret)) > 1, cor(port_ret, lag(port_ret), use = "complete.obs"), NA),
    gamma1_autocorr = ifelse(sum(!is.na(gamma1)) > 1, cor(gamma1, lag(gamma1), use = "complete.obs"), NA),
    gamma2_autocorr = ifelse(sum(!is.na(gamma2)) > 1, cor(gamma2, lag(gamma2), use = "complete.obs"), NA),
    gamma3_autocorr = ifelse(sum(!is.na(gamma3)) > 1, cor(gamma3, lag(gamma3), use = "complete.obs"), NA)
  ) |>
  ungroup()

# Calculate portfolio returns (average return of each portfolio per month)
summary_portfolios <- Data |>
  group_by(date, portfolio) |>
  summarise(
    port_ret = mean(ret - rf, na.rm = TRUE),  # Calculate portfolio return as excess return over risk-free rate
    port_ret_sd = sd(ret - rf, na.rm = TRUE)  # Standard deviation of portfolio return
  ) |>
  ungroup()

# Reassign short, long, and full periods
Data <- assign_period(Data, short_periods, "short_period")
Data <- assign_period(Data, long_periods, "long_period")

# Assign the full period (1935-6/68)
Data <- assign_period(Data, list(list(start = ymd("1935-01-01"), end = ymd("1968-06-30"), label = "1935-6/68")), "full_period")

# Check if the periods have been correctly assigned
table(Data$short_period)
table(Data$long_period)
table(Data$full_period)
colnames(Data)

# Merge Data and summary_portfolios using 'date' and 'portfolio'
Portfolio_data <- Data |>
  left_join(summary_portfolios, by = c("date", "portfolio"))

# Select relevant columns from gamma_results to join
gamma_results_clean <- gamma_results |>
  select(date, portfolio, gamma0, gamma1, gamma2, gamma3, rsquare, return_autocorr, gamma1_autocorr, gamma2_autocorr, gamma3_autocorr)

# Merge the datasets on 'date' and 'portfolio'
merged_data <- Portfolio_data |>
  left_join(gamma_results_clean, by = c("date", "portfolio"))

# Calculate statistics for long periods
table_4_long_periods <- merged_data |>
  group_by(long_period) |>
  summarise(
    Rm_mean = mean(port_ret, na.rm = TRUE),        # Mean of market return
    Rm_minus_Rf_mean = mean((port_ret-rf), na.rm = TRUE),  # Mean of market return minus risk-free rate
    gamma1_mean = mean(gamma1, na.rm = TRUE),      # Mean of gamma1
    gamma0_mean = mean(gamma0, na.rm = TRUE),      # Mean of gamma0
    Rf_mean = mean(rf, na.rm = TRUE),             # Mean of risk-free rate
    Rm_sd = sd(port_ret, na.rm = TRUE),            # Standard deviation of market return
    Rm_minus_Rf_sd = sd((port_ret-rf), na.rm = TRUE), # Standard deviation of market return minus risk-free rate
    gamma1_by_Rm_sd = mean(gamma1, na.rm = TRUE) / sd(port_ret, na.rm = TRUE),  # gamma1 / sd(Rm)
    Rm_minus_Rf_by_Rm_sd = mean((port_ret-rf), na.rm = TRUE) / sd(port_ret, na.rm = TRUE)  # (Rm - Rf) / sd(Rm)
  ) |>
  ungroup()
# View the result for long periods
View(table_4_long_periods)

# Calculate statistics for short periods
table_4_short_periods <- merged_data |>
  group_by(short_period) |>
  summarise(
    Rm_mean = mean(port_ret, na.rm = TRUE),        # Mean of market return
    Rm_minus_Rf_mean = mean((port_ret-rf), na.rm = TRUE),  # Mean of market return minus risk-free rate
    gamma1_mean = mean(gamma1, na.rm = TRUE),      # Mean of gamma1
    gamma0_mean = mean(gamma0, na.rm = TRUE),      # Mean of gamma0
    Rf_mean = mean(rf, na.rm = TRUE),             # Mean of risk-free rate
    Rm_sd = sd(port_ret, na.rm = TRUE),            # Standard deviation of market return
    Rm_minus_Rf_sd = sd((port_ret-rf), na.rm = TRUE), # Standard deviation of market return minus risk-free rate
    gamma1_by_Rm_sd = mean(gamma1, na.rm = TRUE) / sd(port_ret, na.rm = TRUE),  # gamma1 / sd(Rm)
    Rm_minus_Rf_by_Rm_sd = mean((port_ret-rf), na.rm = TRUE) / sd(port_ret, na.rm = TRUE)  # (Rm - Rf) / sd(Rm)
  ) |>
  ungroup()

# View the result for short periods
View(table_4_short_periods)

# Calculate statistics for full period
table_4_full_period <- merged_data |>
  group_by(full_period) |>
  summarise(
    Rm_mean = mean(port_ret, na.rm = TRUE),        # Mean of market return
    Rm_minus_Rf_mean = mean((port_ret-rf), na.rm = TRUE),  # Mean of market return minus risk-free rate
    gamma1_mean = mean(gamma1, na.rm = TRUE),      # Mean of gamma1
    gamma0_mean = mean(gamma0, na.rm = TRUE),      # Mean of gamma0
    Rf_mean = mean(rf, na.rm = TRUE),             # Mean of risk-free rate
    Rm_sd = sd(port_ret, na.rm = TRUE),            # Standard deviation of market return
    Rm_minus_Rf_sd = sd((port_ret-rf), na.rm = TRUE), # Standard deviation of market return minus risk-free rate
    gamma1_by_Rm_sd = mean(gamma1, na.rm = TRUE) / sd(port_ret, na.rm = TRUE),  # gamma1 / sd(Rm)
    Rm_minus_Rf_by_Rm_sd = mean((port_ret-rf), na.rm = TRUE) / sd(port_ret, na.rm = TRUE)  # (Rm - Rf) / sd(Rm)
  ) |>
  ungroup()
# View the result for full periods
View(table_4_full_period)

# Function to round values to 4 decimal places and format as required
format_table_4 <- function(data) {
  data |>
    mutate(
      Rm_mean = round(Rm_mean, 4),
      Rm_minus_Rf_mean = round(Rm_minus_Rf_mean, 4),
      gamma1_mean = round(gamma1_mean, 4),
      gamma0_mean = round(gamma0_mean, 4),
      Rf_mean = round(Rf_mean, 4),
      Rm_minus_Rf_by_Rm_sd = round(Rm_minus_Rf_by_Rm_sd, 4),
      gamma1_by_Rm_sd = round(gamma1_by_Rm_sd, 4),
      Rm_sd = round(Rm_sd, 4),
      Rm_minus_Rf_sd = round(Rm_minus_Rf_sd, 4)
    )
}

# Rename the period columns so they are consistent across all tables
table_4_short_periods <- table_4_short_periods |> rename(period = short_period)
table_4_long_periods <- table_4_long_periods |> rename(period = long_period)
table_4_full_period <- table_4_full_period |> rename(period = full_period)

# Combine all periods into a single table in the order shown in the reference table
final_table_4 <- bind_rows(
  table_4_full_period,
  table_4_long_periods,
  table_4_short_periods
)

# Ensure the periods are ordered in the correct sequence as per article 
period_order <- c(
  "1935-6/68", "1935-45", "1946-55", "1956-6/68",  
  "1935-40", "1941-45", "1946-50", "1951-55", "1956-60", "1961-6/68")

# Recalculate necessary columns to match the original table
final_table_4 <- final_table_4 |>
  mutate(
    Rm_minus_Rf_by_Rm_sd = Rm_minus_Rf_mean / Rm_sd,  # (Rm - Rf) / sd(Rm)
    gamma1_by_Rm_sd = gamma1_mean / Rm_sd             # gamma1 / sd(Rm)
  )

# Now select and reorder the columns to match the original Table 4
final_table_4 <- final_table_4 |>
  select(
    period,               # Time periods
    Rm_mean,              # Mean of Rm (market return)
    Rm_minus_Rf_mean,     # Mean of (Rm - Rf)
    gamma1_mean,          # Mean of gamma1
    gamma0_mean,          # Mean of gamma0
    Rf_mean,              # Mean of Rf
    Rm_minus_Rf_by_Rm_sd, # (Rm - Rf) / sd(Rm)
    gamma1_by_Rm_sd,      # gamma1 / sd(Rm)
    Rm_sd,                # Standard deviation of Rm
    Rm_minus_Rf_sd        # Standard deviation of (Rm - Rf)
  ) |>
  mutate(period = factor(period, levels = period_order)) |>  # Order the period column correctly
  arrange(period)  # Arrange by the defined period order

# Format the table to 4 decimal places
format_table_4 <- function(data) {
  data |>
    mutate(
      Rm_mean = round(Rm_mean, 4),
      Rm_minus_Rf_mean = round(Rm_minus_Rf_mean, 4),
      gamma1_mean = round(gamma1_mean, 4),
      gamma0_mean = round(gamma0_mean, 4),
      Rf_mean = round(Rf_mean, 4),
      Rm_minus_Rf_by_Rm_sd = round(Rm_minus_Rf_by_Rm_sd, 4),
      gamma1_by_Rm_sd = round(gamma1_by_Rm_sd, 4),
      Rm_sd = round(Rm_sd, 4),
      Rm_minus_Rf_sd = round(Rm_minus_Rf_sd, 4)
    )
}

# Apply the formatting function to present table as per article struture 
final_table_4 <- format_table_4(final_table_4)
print(final_table_4)


######## Final Table 4 Part B ##########
## structuring table 4 part B

# Aggregate risk-free rate (rf) for each date
rf_aggregated <- Data |>
  group_by(date) |>  
  summarise(rf = mean(rf, na.rm = TRUE)) |>  
  ungroup()

# Merge the aggregated rf into the gamma_results dataset
gamma_results_with_rf <- gamma_results |>
  left_join(rf_aggregated, by = "date")

#  Reassign short, long, and full periods using the assign_period function
gamma_results_with_rf <- assign_period(gamma_results_with_rf, short_periods, "short_period")
gamma_results_with_rf <- assign_period(gamma_results_with_rf, long_periods, "long_period")
gamma_results_with_rf <- assign_period(gamma_results_with_rf, list(list(start = ymd("1935-01-01"), end = ymd("1968-06-30"), label = "1935-6/68")), "full_period")

#  Calculate all statistics for each period
gamma_results_final <- gamma_results_with_rf |>
  group_by(short_period, long_period, full_period) |>
  summarise(
    s_gamma0 = sd(gamma0, na.rm = TRUE),           # Standard deviation of gamma0
    s_rf = sd(rf, na.rm = TRUE),                  # Standard deviation of rf
    t_Rm = mean(port_ret, na.rm = TRUE) / (sd(port_ret, na.rm = TRUE) / sqrt(n())),  # T-stat for Rm
    t_Rm_minus_Rf = mean(port_ret - rf, na.rm = TRUE) / (sd(port_ret - rf, na.rm = TRUE) / sqrt(n())),  # T-stat for Rm - Rf
    t_gamma0 = mean(gamma0, na.rm = TRUE) / (sd(gamma0, na.rm = TRUE) / sqrt(n())),  # T-stat for gamma0
    t_gamma1 = mean(gamma1, na.rm = TRUE) / (sd(gamma1, na.rm = TRUE) / sqrt(n())),  # T-stat for gamma1
    rho_Rm = cor(port_ret, lag(port_ret), use = "complete.obs"),        # Autocorrelation for Rm
    rho_Rm_minus_Rf = cor(port_ret - rf, lag(port_ret - rf), use = "complete.obs"),  # Autocorrelation for Rm - Rf
    rho_gamma1 = cor(gamma1, lag(gamma1), use = "complete.obs"),        # Autocorrelation for gamma1
    rho_gamma0 = cor(gamma0, lag(gamma0), use = "complete.obs"),        # Autocorrelation for gamma0
    rho_rf = cor(rf, lag(rf), use = "complete.obs")                     # Autocorrelation for rf
  ) |>
  ungroup()

#  Rename period columns so they are consistent across all tables
gamma_results_short <- gamma_results_final |>
  filter(!is.na(short_period)) |> 
  rename(period = short_period)

gamma_results_long <- gamma_results_final |>
  filter(!is.na(long_period)) |> 
  rename(period = long_period)

gamma_results_full <- gamma_results_final |>
  filter(!is.na(full_period)) |> 
  rename(period = full_period)

#  Combine all periods into a single table with **unique rows only**
final_table_4 <- bind_rows(
  gamma_results_full,
  gamma_results_long,
  gamma_results_short
) |>
  distinct(period, .keep_all = TRUE)  # Ensure no duplicates by keeping only unique rows based on the 'period' column

# Reorder periods as specified
period_order <- c(
  "1935-6/68", "1935-45", "1946-55", "1956-6/68",  
  "1935-40", "1941-45", "1946-50", "1951-55", "1956-60", "1961-6/68"
)

#  Ensure correct period order and remove any duplicates
final_table_4 <- final_table_4 |>
  mutate(period = factor(period, levels = period_order)) |>
  arrange(period)

# Dropcolumn labels (short_period, long_period, full_period)
final_table_4 <- final_table_4 |> select(-short_period, -long_period, -full_period)

<<<<<<< HEAD
=======
# View the final table without duplicates and with all columns
print(final_table_4)
View(final_table_4)

### Completed###
### Thank you. 
>>>>>>> 661fa20aa60f6578348906b4f0c60d740858911b
