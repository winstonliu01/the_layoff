# @author: Winston Liu (wdl33)
# Version: 1/5/21
# About: This script acts as a pipeline that combines information from the 
# stock, financial, and executive data sets. The outcome of this is a 
# cleaned up table that contains this information with over 500 publicly 
# traded companies.

##############################

# Import necessary libraries for cleaning
library(readr)
library(tidyverse)
library(lubridate)
library(data.table)

# Read in files - Be sure to change directory path as appropriate 
# Set working directory each session: Code is operating inside the_layoff folder 
xnysFinancial <- read_csv("WRDS Data/raw/financial/XNYS Financial Data.csv")
xnasFinancial <- read_csv("WRDS Data/raw/financial/XNAS Financial Data.csv")
nasdaqFinancial <- read_csv("WRDS Data/raw/financial/NASDAQ Financial data.csv")
nyseFinancial <- read_csv("WRDS Data/raw/financial/NYSE_Quarterly_Data.csv")
russellFinancial <- read_csv("WRDS Data/raw/financial/Russell Financial Data.csv")

xnysExecutive <- read_csv("WRDS Data/raw/exec/XNYS Executive Data.csv")
xnasExecutive <- read_csv("WRDS Data/raw/exec/XNAS Executive Data.csv")
nasdaqExecutive <- read_csv("WRDS Data/raw/exec/NASDAQ Executive Data.csv")
nyseExecutive <- read_csv("WRDS Data/raw/exec/NYSE_Company_EXEC_INFO.csv")
russellExecutive <- read_csv("WRDS Data/raw/exec/Russell Executive Data.csv")

xnysStock <- read_csv("WRDS Data/raw/stock/XNYS Stock Data.csv")
xnasStock <- read_csv("WRDS Data/raw/stock/XNAS Stock Data.csv")
nasdaqStock <- read_csv("WRDS Data/raw/stock/NASDAQ Stock Data.csv")
nyseStock <- read_csv("WRDS Data/raw/stock/NYSE_Company_Stock_Data.csv")
russellStock <- read_csv("WRDS Data/raw/stock/Russell 2000 Stock Data.csv")

##############################

# Verification Step: Check to see if there are duplicate tickers, 
# which means duplicate data that needs to be eliminated

fin1 <- unique(xnysFinancial$tic)
fin2 <- unique(xnasFinancial$tic)
fin3 <- unique(nasdaqFinancial$tic)
fin4 <- unique(nyseFinancial$Ticker)
fin5 <- unique(russellFinancial$tic)
financial <- c(fin1, fin2, fin3, fin4, fin5)

exec1 <- unique(xnysExecutive$TICKER)
exec2 <- unique(xnasExecutive$TICKER)
exec3 <- unique(nasdaqExecutive$TICKER)
exec4 <- unique(nyseExecutive$TICKER)
exec5 <- unique(russellExecutive$TICKER)
exec <- c(exec1, exec2, exec3, exec4, exec5)

stock1 <- unique(xnysStock$TICKER)
stock2 <- unique(xnasStock$TICKER)
stock3 <- unique(nasdaqStock$TICKER)
stock4 <- unique(nyseStock$TICKER)
stock5 <- unique(russellStock$TICKER)
stock <- c(stock1, stock2, stock3, stock4, stock5)

n_occurFin <- data.frame(table(financial))
n_occurExec <- data.frame(table(exec))
n_occurStock <- data.frame(table(stock))

n_occurFin <- n_occurFin[n_occurFin$Freq > 1,] %>% select(financial)
n_occurExec <- n_occurExec[n_occurExec$Freq > 1,] %>% select(exec)
n_occurStock <- n_occurStock[n_occurStock$Freq > 1,] %>% select(stock)

# Should print out True if they're all empty
print(nrow(n_occurFin) == 0 && nrow(n_occurExec) == 0 && nrow(n_occurStock) == 0)

##############################

# Combine data together for each data set
# Data set don't have identical columns, so we will fill with na if it is missing

# Assumption: If the data set had duplicates, the values for the entire row must be the same, not just a specific column.
# There is no time gap in the different set meaning they occur all in the same time period
# Therefore, we want to delete only one copy of duplicate data which should be the same value in a row

financial <- rbindlist(list(xnysFinancial,xnasFinancial,nasdaqFinancial,nyseFinancial, russellFinancial), 
                       fill = TRUE) %>% distinct()
executive <- rbindlist(list(xnysExecutive,xnasExecutive,nasdaqExecutive,nyseExecutive, russellExecutive), 
                       fill = TRUE) %>% distinct()
stock <- rbindlist(list(xnysStock,xnasStock,nasdaqStock,nyseStock, russellStock), 
                       fill = TRUE) %>% distinct()

# Delete columns we don't need (though we can re-add as needed) to make data cleaner
# Reference file "WRDS Instruction Manual" to learn more about what each column represents

financial <- subset(financial, select = -c(24:40)) %>%
             subset(financial, 
                    select = -c(gvkey,fyr, indfmt, consol, popsrc, datafmt, cusip, curcdq,cik,costat,exchg))

executive <- subset(executive, 
                    select = -c(ALLOTHPD, GVKEY, EXEC_LNAME, EXEC_FNAME, EXEC_MNAME, NAMEPREFIX, PAGE,
                                CUSIP, EXCHANGE, ADDRESS, CITY, STATE,ZIP, TELE,SPCODE,
                                SUB_TELE, NAICS, SICDESC, NAICSDESC,SPINDEX,SIC, EXECRANK))

stock <- subset(stock, 
                    select = -c(PERMNO, NAMEENDT, SHRCD, EXCHCD, SICCD, NCUSIP, SHRCLS, TSYMBOL, 
                                NAICS, PRIMEXCH, TRDSTAT, SECSTAT, PERMCO, ISSUNO, HEXCD, HSICIG,
                                HSICCD, CUSIP, SHRFLG, HSICMG, SPREAD, SHRENDDT,
                                ALTPRC, ALTPRCDT, RETX))

# Only keep companies that are present in both financial and stock so we can compare side-by-side
financialComp <- unique(financial$tic)
stockComp <- unique(stock$TICKER)
execComp <- unique(executive$TICKER)
intersectComp <- intersect(financialComp, stockComp) %>% intersect(execComp)

financial <- subset(financial, tic %in% intersectComp)
stock <- subset(stock, TICKER %in% intersectComp)
executive <- subset(executive, TICKER %in% intersectComp)

# Order listings alphabetically based on Ticker symbol 
stock <- stock[order(stock$TICKER), ] 
financial <- financial[order(financial$tic), ]
executive <- executive[order(executive$TICKER), ]

##############################

# To combine data from financial and stock, we must make the data from stock reflect a 
# quarterly basis instead of monthly. So we will take the average of these metrics:
# price(prc) ; number of shares outstanding (shrout) ; share volume (vol)
# though we can include more as necessary

# Convert date to format where we can perform operations on it
stock <- stock %>% mutate(dateFormatted = as.Date(stock$date,format='%m/%d/%Y')) 
financial <- financial %>% mutate(dateFormatted = as.Date(financial$datadate,format='%m/%d/%Y')) %>%
  rename(TICKER = tic)

# Figure out earliest and latest data in each data set for each company 
stockBoundary <- stock %>% group_by(TICKER) %>% 
  summarise(firstS = min(dateFormatted), lastS = max(dateFormatted)) 
financialBoundary <- financial %>% group_by(TICKER) %>% 
  summarise(firstF = min(dateFormatted), lastF = max(dateFormatted)) 
stock <- stock %>% inner_join(financialBoundary) %>% inner_join(stockBoundary) 
financial <- financial %>% inner_join(financialBoundary) %>% inner_join(stockBoundary) 

# Clean up data so they are happening in the same time frame
stock <- stock %>% group_by(TICKER) %>% 
  filter(dateFormatted >= firstF - days(65)) %>%
  filter(dateFormatted < lastF - days(65))

financial <- financial %>% group_by(TICKER) %>% 
  filter(dateFormatted < lastF)

##############################

# Verification - financial count should be 3x stock count
# A temporary patch would be to just delete companies where there are discrepancies

stockCount <- stock %>% summarize(n())
financialCount <- financial %>% summarize(n()*3)

company <- data.frame(unique(stock$TICKER)) %>% mutate(stockCount, financialCount) %>% 
  subset(select = -unique.stock.TICKER.) %>% rename(ticker = 1, stockCount = 2, financialCount = 3) %>%
  mutate(stockCount == financialCount) %>% rename(equal = 4) %>% 
  filter(equal == TRUE)

stock <- stock %>% group_by(TICKER) %>% filter(TICKER %in% company$ticker)
financial <- financial %>% group_by(TICKER) %>% filter(TICKER %in% company$ticker)

##############################

# Precondition: Once it reaches here, each ticker appears a multiple of 3 times (each quarter is 4
# months so this would correspond to a full year)
# Taken from: https://community.rstudio.com/t/how-to-take-the-average-of-every-3-rows/101338

# We have the data ordered by date and by ticker, so we can take the average 
# of the metrics for each 3 rows
GroupLabels <- 0:(nrow(stock) - 1)%/% 3
stock$Group <- GroupLabels

# Average for metrics we want to include that is converted to quarters 
AvgsMetric <- stock %>% group_by(Group) %>% 
  summarize(AvgPRC = mean(PRC), AvgShOut = mean(SHROUT), AvgShVol = mean(VOL)) 

# Needed to create our new data frame so we have common columns to use the join operation
AvgsData <- stock %>% group_by(Group) %>% 
  summarize(TICKER = TICKER, dateFormatted = max(dateFormatted))

# Construct our new data frame and eliminate each group from repeating
Avgs <- inner_join(AvgsData, AvgsMetric) %>% distinct() 

stock <- stock %>% subset(select = -c(firstF, lastF, firstS, lastS))
stock <- inner_join(stock, Avgs) %>% select(-Group) 

##############################

# Clean up executive data set so we can add to the combined data set

executive <- executive %>% group_by(TICKER) %>% 
  filter(TICKER %in% company$ticker) %>% ungroup() %>%
  filter(PCEO == "CEO") %>%
  select(EXEC_FULLNAME, TICKER, BECAMECEO, LEFTOFC, TDC1, TDC2, SAL_PCT, SHROWN_TOT, GENDER, YEAR)

executive <- executive %>%
  group_by(EXEC_FULLNAME, YEAR) %>%
  filter(row_number() == n())

executive$left <- format(as.Date(executive$LEFTOFC, format="%m/%d/%Y"),"%Y")
executive$joined <- format(as.Date(executive$BECAMECEO, format="%m/%d/%Y"),"%Y")
executive$ceoChange <- executive$YEAR == executive$left

executive <- executive %>% ungroup() %>% rename(fyearq = YEAR)

##############################

# Clean up the master data set 
master <- inner_join(financial, stock)
master <- inner_join(master, executive)

master <- master %>% 
  subset(select = -c(firstF, lastF, firstS, lastS, date, COMNAM, datacqtr, datafqtr,
                    datadate, PRC, VOL, SHROUT,BID, ASK)) %>% 
  select(dateFormatted, fyearq, TICKER, conm, revty,fic, cshtrq, mkvaltq, sic, 
         BIDLO, ASKHI, RET,AvgPRC, AvgShOut, AvgShVol,EXEC_FULLNAME, BECAMECEO,
         LEFTOFC, TDC1, TDC2, SAL_PCT, SHROWN_TOT,GENDER, ceoChange) 

# Ensure that there is at least 3 years worth of data for the ticker symbols
dataCombined <- master %>% 
  rename( date = dateFormatted,fisYr = fyearq,ticker = TICKER, compName = conm,
          totalRev = revty, country = fic, cmnShrTraded = cshtrq, totalMktVal = mkvaltq,
          industry = sic, bidLow = BIDLO, askHigh = ASKHI, holdingPeriodReturn = RET,
          avgPrc = AvgPRC, avgShOut = AvgShOut, avgShVol = AvgShVol, ceoName = EXEC_FULLNAME,
          ceoStart = BECAMECEO, ceoEnd = LEFTOFC, ceoComp1 = TDC1, ceoComp2 = TDC2,
          ceoSalPctChg = SAL_PCT,ceoShares = SHROWN_TOT, ceoGender = GENDER,
          ceoChg = ceoChange) %>%
  group_by(ticker) %>% filter(n() >= 12)

# Exports this as a CSV file
write.csv(dataCombined,"wrdsCombinedData.csv", row.names = FALSE)
