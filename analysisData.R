# @author: Winston Liu (wdl33)
# Version: 1/30/21
# About: Preliminary plots for WRDS Combined Dataset

##############################

# Import necessary libraries for analysis
library(readr)
library(tidyverse)
library(lubridate)
library(ggplot2)
library(gridExtra)
library(reshape)

# Read in files - Be sure to change directory path as appropriate (need to set for each session)
data <- read_csv("~/Desktop/wrdsCombinedData.csv")
data1 <- read_csv("~/Desktop/wrdsCombinedData2.csv")

# Source helpful for graphing: https://www.r-graph-gallery.com

##############################

dataRet1 <- data %>% select(holdingPeriodReturn)
plot1 <- ggplot(dataRet1, aes(x=holdingPeriodReturn)) + 
  geom_density(color="darkblue", fill="lightblue") +
  geom_vline(aes(xintercept=mean(holdingPeriodReturn)), color="blue", linetype="dashed", size=1) + 
  coord_cartesian(xlim = c(-0.50, 0.50))

dataRet2 <- data1 %>% select(avgRet)
plot2 <- ggplot(dataRet2, aes(x=avgRet)) + 
  geom_density(color="darkblue", fill="lightblue") +
  geom_vline(aes(xintercept=mean(avgRet, na.rm = TRUE)), color="blue", linetype="dashed", size=1) + 
  coord_cartesian(xlim = c(-0.25, 0.25))

dataRet3 <- data1 %>% group_by(year) %>% select(avgRet)
plot3 <- ggplot(na.omit(dataRet3), aes(x=avgRet)) + 
  geom_density(color="darkblue", fill="lightblue") +
  ggtitle("Average Return by Year") + 
  coord_cartesian(xlim = c(-0.125, 0.125)) 
plot3 <- plot3 + facet_wrap(~year) 

# From the plot, we can see that the market follows approximately a normal 
# distribution. Knowing this can help us create models for yearly returns 
# later on.

##############################

dataCovid1 <- data1 %>% filter(year == 2020) %>% group_by(industry) %>%
  summarize(avgMkVal = mean(avgMkVal)) %>%
  arrange(desc(avgMkVal)) %>% 
  slice(1:15)

dataCovid2 <- data1 %>% filter(year == 2020) %>% group_by(industry) %>%
  summarize(avgMkVal = mean(avgMkVal)) %>%
  arrange(avgMkVal) %>% 
  slice(1:15)

dataCovid1$industry <- as.character(dataCovid1$industry)
dataCovid2$industry <- as.character(dataCovid2$industry)

plot4 <- ggplot(data=dataCovid1, aes(x=industry, y=avgMkVal)) +
  geom_bar(stat="identity") + 
  ggtitle("AvgMkVal for Top-15 Best Performing Industries in 2020")

plot5 <- ggplot(data=dataCovid2, aes(x=industry, y=avgMkVal)) +
  geom_bar(stat="identity") + 
  ggtitle("AvgMkVal for Top-15 Worst Performing Industries in 2020")

# These two graphs demonstrate the stark difference in average market value 
# between the best and worst performing industries during the start of the 
# pandemic (2020) which is an example of a stressor

##############################

totalMkVal <- data1 %>% filter(year == 2020) %>% group_by(industry) %>%
  summarize(avgMkVal = mean(avgMkVal)) %>% sum()

dataCovid1$percentage <- (dataCovid1$avgMkVal/totalMkVal) * 100
remainingIndustry <- round(100 - sum(dataCovid1$percentage), digits = 2)
remainingIndustrySum <- round(totalMkVal - sum(dataCovid1$avgMkVal), digits = 2)
remainder <- c("Other",remainingIndustrySum,remainingIndustry)

# Not a visual (yet) but it was interesting to note that the top industries 
# only consisted 3% of the overall market. This could suggest that there are 
# a plethora of industries though even small end up having a great effect on
# the total market. As a result, looking at the major players might not
# be enough to get a general trend.

##############################

dataShares <- data1 %>% group_by(year) %>% summarize(shares = sum(avgVol)) %>% na.omit()
plot6 <- ggplot(dataShares, aes(x=year, y=shares)) +
  geom_line()

# We see that there was a decrease in stocks traded, presumably due to financial crisis.
# However, since it has recovered strongly. Part of it may be a result of technological 
# innovations that made trading more accessible to everyone as well as no-commission 
# trading.

##############################

dataReturns <- data1 %>% group_by(year) %>% summarize(returns = sum(avgRet)) %>% na.omit()
plot7 <- ggplot(dataReturns, aes(x=year, y=returns)) +
  geom_line()

# This was interesting to see. Considering all major stocks, the market has 
# generally been on a major growth. Searching up S+P 500's historical prices,
# this trend agrees with it. A possible angle to examine is why the market 
# has managed to continue growing to all time highs and if it has to do with 
# a leadership change.

##############################

dataLeadership <- data1 %>% group_by(year) %>% count(ceoChg) %>% na.omit()

plot8 <- ggplot(dataLeadership, aes(x=year, y=n, fill=ceoChg)) +
  geom_bar(stat="identity", position="dodge") +
  theme(axis.text.x = element_text(angle = 90)) + 
  ggtitle("Number of CEO Changes by Year")

# We see that there have not been many CEO changes until the last couple of years. 
# A problem with the CEO data is that there seemed to be a lot of NAs, so the information might not be 
# as accurate. However, considering the number of companies that we have in our 
# database, 50 changes is noticeable. Something useful with this information is
# to see if there is any feedback describing how the culture has changed.

##############################

# Source helpful for looking up industry codes: https://www.naics.com/code-search/
# Top 25 NAICS: https://sbecs.treas.gov/TopNAICS.aspx

dataComputerSystems <- data1 %>% filter(industry == 541512)
dataCSReturns <- dataComputerSystems %>% group_by(year) %>% summarize(returns = sum(avgRet)) %>% na.omit()
plot9 <- ggplot(dataCSReturns, aes(x=year, y=returns)) +
  geom_line()

dataCSMkVal <- dataComputerSystems %>% group_by(year) %>% summarize(mkVal = sum(avgMkVal)) %>% na.omit()
plot10 <- ggplot(dataCSMkVal, aes(x=year, y=mkVal)) +
  geom_line()

# From the government website, we see that this industry code is among one of the 
# top ones for 2022, winning 68 awards. A possible limitation with the data we have 
# is all of the tickers that belong to this industry might not be included or 
# there can be some mislabeling. However, from plot9, we see that in the last 3 years,
# the returns have been on a steady growth though there may be some pullback. Similarly with 
# the market value, it plummeted near 2020. Having these time frame and trends, it 
# would help us filter to specific company to learn more about the company perception
# by their workers.

