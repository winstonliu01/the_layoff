# @author: Winston Liu (wdl33)
# Version: 1/12/21
# About: This script acts as a pipeline that combines information from the 
# stock, financial, and executive data sets. The outcome of this is a 
# cleaned up table that contains this information with over 500 publicly 
# traded companies. The difference between this version is that company 
# data is listed on an yearly basis compared to a quarterly one. Additionally,
# we keep more companies (~900 compared to ~500) though there are more 
# NAs listed in this set compared to the first.

##############################

# Import necessary libraries for cleaning
library(readr)
library(tidyverse)
library(lubridate)
library(data.table)
library(naniar)

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

# Create three large data frames: Financial, Stock, Executive which combines all markets
# Columns aren't the same across all data, so we fill with NA as appropriate

# Assumption when cleaning data: If the data set has a duplicate value, it must mean 
# that the entire row is the same, not just that one value. Additionally, the data
# happens across one continuous time period. So, we can just keep one unique row.

financial <- rbindlist(list(xnysFinancial,xnasFinancial,nasdaqFinancial,nyseFinancial, russellFinancial), 
                       fill = TRUE) %>% distinct()
executive <- rbindlist(list(xnysExecutive,xnasExecutive,nasdaqExecutive,nyseExecutive, russellExecutive), 
                       fill = TRUE) %>% distinct()
stock <- rbindlist(list(xnysStock,xnasStock,nasdaqStock,nyseStock, russellStock), 
                   fill = TRUE) %>% distinct()

# Since they all come from the same four markets, executive/financial/stock should
# contain the same companies. But, we clean the data to ensure that so we can 
# join accordingly later on.

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

# Patch things up that lead to error and warnings later below 

# Remove TQNT and WW because they have NA for bidLo/askHi
stock <- stock %>% filter(TSYMBOL != "WW" &  TSYMBOL != "TQNT")
financial <- financial %>% filter(tic != "WW" & tic != "TQNT")
executive <- executive %>% filter(TICKER != "WW" & TICKER != "TQNT")

##############################

# Calculate the average for the metrics on a yearly basis
# Stock: price (prc) ; number of shares outstanding (shrout) ; share volume (vol)
# Financial: total revenue (revty) ; total market value (mkvaltq) ; quarterly common shares traded (cshtrq)

stock <- stock %>% mutate(dateFormatted = as.Date(stock$date,format='%m/%d/%Y')) %>%
  mutate(year = format(dateFormatted, "%Y")) 

financial <- financial %>% rename(year = fyearq)

stock <- stock %>% group_by(TICKER, year) %>% 
  replace_with_na(replace = list(RETX = "C")) 

stock$RETX <- as.numeric(stock$RETX, na.rm = TRUE) 
stock$year <- as.numeric(stock$year, na.rm = TRUE)

stock <- stock %>% mutate(avgPrc = mean(PRC, na.rm = TRUE), 
         avgShr = mean(SHROUT, na.rm = TRUE),
         avgVol = mean(VOL, na.rm = TRUE),
         avgRet = mean(RETX, na.rm = TRUE),
         bid = median(BID, na.rm = TRUE),
         ask = median(ASK, na.rm = TRUE),
         bidLo = min(BIDLO, na.rm = TRUE),
         askHi = max(ASKHI, na.rm = TRUE)
         )

financial <- financial %>% group_by(tic, year) %>% 
  mutate(avgRev = mean(revty, na.rm = TRUE),
         avgMkVal = mean(mkvaltq, na.rm = TRUE),
         avgShares = mean(cshtrq, na.rm = TRUE)
         )

##############################

# Clean up columns in data frame to keep ones we truly need

stock <- subset(stock, 
                select = -c(PERMNO, NAMEENDT, SHRCD, EXCHCD, SICCD, NCUSIP, SHRCLS, TSYMBOL, 
                            PRIMEXCH, TRDSTAT, SECSTAT, PERMCO, ISSUNO, HEXCD, HSICIG,
                            HSICCD, CUSIP, SHRFLG, HSICMG, SPREAD, SHRENDDT,
                            ALTPRC, ALTPRCDT, RETX, BIDLO, ASKHI, PRC, VOL,
                            RET, BID, ASK, SHROUT, RETX, dateFormatted, date)) %>% 
         ungroup() %>% group_by(TICKER, year) %>% distinct()

financial <- subset(financial, select = -c(24:40)) %>% 
             subset(select = 
                 -c(gvkey, datadate, fyr, fqtr, indfmt, consol, popsrc, datafmt, 
                    cusip, curcdq, datacqtr, datafqtr, cik, costat, exchg, revty,
                    cshtrq, mkvaltq, sic, fic)) %>% rename(TICKER = tic) %>%
             ungroup() %>% group_by(TICKER, year) %>% distinct() 

finances <- full_join(stock, financial) %>% 
            subset(select = -c(conm)) %>%
            ungroup() %>% group_by(TICKER, year) %>% distinct()

# Hard code to remove a very particular case (AAN)
finances <- finances[-c(20, 21), ]
finances <- finances[1:10504,]

executive <- executive %>% filter(PCEO == "CEO") %>%
  select(EXEC_FULLNAME, TICKER, BECAMECEO, LEFTOFC, TDC1, TDC2, SAL_PCT, SHROWN_TOT, GENDER, YEAR) %>% 
  rename(year = YEAR)

executive <- executive %>%
  group_by(EXEC_FULLNAME, year) %>%
  filter(row_number() == n())

executive$left <- format(as.Date(executive$LEFTOFC, format="%m/%d/%Y"),"%Y")
executive$joined <- format(as.Date(executive$BECAMECEO, format="%m/%d/%Y"),"%Y")
executive$ceoChange <- executive$year == executive$left

##############################

# Clean the master data frame

master <- full_join(finances, executive) 
master <- master[1:10902,]

dataCombined <- master %>% 
  rename(ticker = TICKER, compName = COMNAM, industry = NAICS, ceoName = EXEC_FULLNAME,
          ceoStart = BECAMECEO, ceoEnd = LEFTOFC, ceoComp1 = TDC1, ceoComp2 = TDC2,
          ceoSalPctChg = SAL_PCT,ceoShares = SHROWN_TOT, ceoGender = GENDER,
          ceoChg = ceoChange)

dataCombined <- dataCombined[1:10507,]

# Get rid of NaN
dataCombined$avgPrc[is.nan(dataCombined$avgPrc)] <-NA
dataCombined$avgShr[is.nan(dataCombined$avgShr)] <-NA
dataCombined$avgVol[is.nan(dataCombined$avgVol)] <-NA
dataCombined$avgRet[is.nan(dataCombined$avgRet)] <-NA
dataCombined$avgRev[is.nan(dataCombined$avgRev)] <-NA
dataCombined$avgMkVal[is.nan(dataCombined$avgMkVal)] <-NA
dataCombined$avgShares[is.nan(dataCombined$avgShares)] <-NA

# Exports this as a CSV file
write.csv(dataCombined,"wrdsCombinedData2.csv", row.names = FALSE)