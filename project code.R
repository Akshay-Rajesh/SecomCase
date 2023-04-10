Sys.setenv(LANG = "en")

if (!require("pacman")) install.packages("pacman")
library(data.table)
library(tidyverse)
library(psych)

# Variable definitions
Sys.setenv(JAVA_HOME='C:\\Program Files (x86)\\Java\\jre1.8.0_60') # for 64-bit version
Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_111\\bin\\server') # for 64-bit version
p_load(rJava)


# test if path_name ends with '/'
if (substr(path_name,nchar(path_name),nchar(path_name))!= "/") path_name<-paste(path_name,'/',sep='')

p_load(xlsxjars)
p_load(xlsx)

product_data<-fread("C://MPMD//Data science//00_Assignment_Files//00_Assignment_Files//product_data2.txt")
colnames(product_data) <- c('product_ID', 'productName', 'productGroup')
# check data!
dim(product_data)
head(product_data)


# test if path_name ends with '/'
if (substr(path_name,nchar(path_name),nchar(path_name))!= "/") path_name<-paste(path_name,'/',sep='')

pick_data<-fread("C://MPMD//Data science//00_Assignment_Files//00_Assignment_Files//003_pick_data.csv")
colnames(pick_data) <- c('product_ID', 'storageArea', 'cust_store','orderNo','positionInOrder','quantity','unit','dateTime')
dim(pick_data)
head(pick_data)

# connect with DB
p_load("RMySQL")

db_connection_staging<-dbConnect(MySQL(),user='htw',password='mysql',host='localhost',dbname='stagging')
# drop table if it already exists
if (dbExistsTable(db_connection_staging, "product_data")) dbRemoveTable(db_connection_staging, "product_data")

# Adjust db permission:
dbSendQuery(db_connection_staging, "SET GLOBAL local_infile = true;")

dbWriteTable(db_connection_staging, name = "product_data", value = product_data, row.names = FALSE)

# check list of tables
dbListTables(db_connection_staging)

# get cols of table
dbListFields(db_connection_staging, "product_data")

# drop table if it already exists
if (dbExistsTable(db_connection_staging, "pick_data")) dbRemoveTable(db_connection_staging, "pick_data")

dbWriteTable(db_connection_staging, name = "pick_data", value = pick_data, row.names = FALSE)

# check list of tables
dbListTables(db_connection_staging)

# get cols of table
dbListFields(db_connection_staging, "pick_data")
dbListFields(db_connection_staging, "product_data")


# remove umlauts

product_data$productName <- str_replace_all(product_data$productName ,c('ä'='a','ö'='o','ü'='u','Ä'='A','Ö'='O','Ü'='U','Ã¤'='a'))
product_data$productGroup <- str_replace_all(product_data$productGroup ,c('ä'='a','ö'='o','ü'='u','Ä'='A','Ö'='O','Ü'='U','Ã¤'='a'))

head(product_data)

pick_data$year <- as.numeric(format(as.Date(pick_data$dateTime), "%Y"))

pick_data$order_year <- paste(as.character(pick_data$orderNo), as.character(pick_data$year))

head(pick_data)

db_connection_production<-dbConnect(MySQL(),user='htw',password='mysql',host='localhost',dbname='production')

# drop table if it already exists
if (dbExistsTable(db_connection_production, "product_data")) dbRemoveTable(db_connection_production, "product_data")
if (dbExistsTable(db_connection_production, "pick_data")) dbRemoveTable(db_connection_production, "pick_data")

dbWriteTable(db_connection_production, name = "product_data", value = product_data, row.names = FALSE)

dbWriteTable(db_connection_production, name = "pick_data", value = pick_data, row.names = FALSE)

#Joining table

joinedTable <- dbGetQuery(db_connection_production, statement = "SELECT * FROM production.pick_data left join production.product_data on pick_data.product_ID = product_data.product_ID")
dim(joinedTable)
colnames(joinedTable)[11] <- "product_ID_Dupl"
head(joinedTable)

# drop table if it already exists
if (dbExistsTable(db_connection_production, "joinedTable")) dbRemoveTable(db_connection_production, "joinedTable")

dbWriteTable(db_connection_production, name = "joinedTable", value = joinedTable, row.names = FALSE)

# check list of tables
dbListTables(db_connection_production)

# get cols of table
dbListFields(db_connection_production, "joinedTable")

-------------------------------------------------Data Cleaning-----------------------------------------------------------------------------------------------
  
#Add MissingFlag column
dbGetQuery(db_connection_production, statement = "ALTER TABLE production.joinedtable ADD MissingFlag int;")

#Update MissingFlag to 0
dbGetQuery(db_connection_production, statement = "update production.joinedtable set MissingFlag = 0")

#Update flag values for null
dbGetQuery(db_connection_production, statement = "update production.joinedtable set MissingFlag = 1 where productName is Null or productGroup is Null")

#Update flag values for outliers
dbGetQuery(db_connection_production, statement = "update production.joinedtable set MissingFlag = 1 where quantity <= 0")

-----------------------------------------Schema Creation------------------------------------------------------------------------------------------------------

#Create and write custorstoremonthlypicks
custorstoremonthlypicks = dbGetQuery(db_connection_production, statement = "select cust_store, month(dateTime) as Month, count(*) as custOrStoreMonthlyTotalPicks from joinedtable where MissingFlag = 0 group by cust_store, month(dateTime)")
custorstoremonthlypicks

dbWriteTable(db_connection_production, name = "custorstoremonthlypicks", value = custorstoremonthlypicks, row.names = FALSE)

#Create and write custorstoreoverallpicks
custorstoreoverallpicks = dbGetQuery(db_connection_production, statement = "select cust_store, count(*) as custOrStoreOverallPicks from joinedtable where MissingFlag = 0 group by cust_store")
custorstoreoverallpicks

dbWriteTable(db_connection_production, name = "custorstoreoverallpicks", value = custorstoreoverallpicks, row.names = FALSE)

#Create and write custorstoretable
custorstoretable = dbGetQuery(db_connection_production, statement = "select product_ID, cust_store, dateTime from joinedtable where MissingFlag = 0")
custorstoretable

dbWriteTable(db_connection_production, name = "custorstoretable", value = custorstoretable, row.names = FALSE)

#Create and write custorstoreyearlypicks
custorstoreyearlypicks = dbGetQuery(db_connection_production, statement = "select cust_store, year(dateTime) as Year, count(*) as custOrStoreYearlyTotalPicks from joinedtable where MissingFlag = 0 group by cust_store, year(dateTime)")
custorstoreyearlypicks

dbWriteTable(db_connection_production, name = "custorstoreyearlypicks", value = custorstoreyearlypicks, row.names = FALSE)

#Create and write ordersperyear
ordersperyear = dbGetQuery(db_connection_production, statement = "select year(dateTime) as Year, count(distinct(orderNo)) as ordersperyear from joinedtable where MissingFlag = 0 group by year(dateTime)")
ordersperyear

dbWriteTable(db_connection_production, name = "ordersperyear", value = ordersperyear, row.names = FALSE)

#Create productGroupMonthlyPicks
productGroupMonthlyPicks = dbGetQuery(db_connection_production, statement = "select productGroup, month(dateTime) as Month , count(*) as ProdGrpMonthlyTotalPicks  from joinedtable where MissingFlag = 0 group by productGroup, month(dateTime)")
productGroupMonthlyPicks

dbWriteTable(db_connection_production, name = "productGroupMonthlyPicks", value = productGroupMonthlyPicks, row.names = FALSE)

#Create productGroupYearlyPicks
productGroupYearlyPicks = dbGetQuery(db_connection_production, statement = "select productGroup, year(dateTime) as Year , count(*) as ProdGrpYearlyTotalPicks  from joinedtable where MissingFlag = 0 group by productGroup, year(dateTime)")
productGroupYearlyPicks

dbWriteTable(db_connection_production, name = "productGroupYearlyPicks", value = productGroupYearlyPicks, row.names = FALSE)

#Create productGroupOverallPicks
productGroupOverallPicks = dbGetQuery(db_connection_production, statement = "select productGroup, count(*) as productGroupOverallPicks  from joinedTable where MissingFlag = 0 group by productGroup")
productGroupOverallPicks

#Create productGroupTable
productGroupTable = dbGetQuery(db_connection_production, statement = "SELECT product_ID,productGroup,dateTime FROM joinedTable where MissingFlag = 0")

dbWriteTable(db_connection_production, name = "productGroupTable", value = productGroupTable, row.names = FALSE)

#Create productSellPerGroup
productSellPerGroup = dbGetQuery(db_connection_production, statement = "select productGroup, productName , count(*) as PerProductSell  from joinedtable where MissingFlag = 0 group by productGroup,productName")
productSellPerGroup

dbWriteTable(db_connection_production, name = "productSellPerGroup", value = productSellPerGroup, row.names = FALSE)

#Create storageAreaMonthlyPicks
storageAreaMonthlyPicks = dbGetQuery(db_connection_production, statement = "select storageArea, month(dateTime) as Month , count(*) as StorageAreaMonthlyPicks from joinedTable where MissingFlag = 0 group by storageArea, Month(dateTime)")
storageAreaMonthlyPicks

dbWriteTable(db_connection_production, name = "storageAreaMonthlyPicks", value = storageAreaMonthlyPicks, row.names = FALSE)

#Create storageAreaYearlyPicks
storageAreaYearlyPicks = dbGetQuery(db_connection_production, statement = "select storageArea, year(dateTime) as year, count(*) as StorageAreaMonthlyPicks  from joinedTable where MissingFlag = 0 group by storageArea, year(dateTime)")
storageAreaYearlyPicks

dbWriteTable(db_connection_production, name = "storageAreaYearlyPicks", value = storageAreaYearlyPicks, row.names = FALSE)

#Create storageAreaOverallPicks
storageAreaOverallPicks = dbGetQuery(db_connection_production, statement = "select storageArea, count(*) as StorageAreaMonthlyPicks  from joinedTable where MissingFlag = 0 group by storageArea")
storageAreaOverallPicks

#Create storageAreaTable
storageArea = dbGetQuery(db_connection_production, statement = "SELECT product_ID,storageArea,dateTime FROM joinedTable where MissingFlag = 0")
storageArea

dbWriteTable(db_connection_production, name = "storageArea", value = storageArea, row.names = FALSE)

# Disconnect from the database
dbDisconnect(db_connection_production)