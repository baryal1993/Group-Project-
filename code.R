library(tidy)
install.packages("tidyverse")
library(tidyverse)
library(RPostgres)
wrds <- dbConnect(Postgres(),
                  host='wrds-pgdata.wharton.upenn.edu',
                  port=9737,
                  dbname='wrds',
                  sslmode='require',
                  user='wajeehanisar')
