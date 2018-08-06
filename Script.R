options(max.print = 1000, scipen = 999, width = 100)
library(RevoScaleR)
rxOptions(reportProgress = 1) # reduces the amount of output RevoScaleR produces
library(dplyr)
options(dplyr.print_max = 200)
options(dplyr.width = Inf) # shows all columns of a tbl_df object
library(stringr)
library(lubridate)
library(rgeos) # spatial package
library(sp) # spatial package
library(maptools) # spatial package
library(ggmap)
library(ggplot2)
library(gridExtra) # for putting plots side by side
library(ggrepel) # avoid text overlap in plots
library(tidyr)
library(seriation) # package for reordering a distance matrix

## edit column classes
col_classes <- c('VendorID' = "factor",
                 'tpep_pickup_datetime' = "character",
                 'tpep_dropoff_datetime' = "character",
                 'passenger_count' = "integer",
                 'trip_distance' = "numeric",
                 'pickup_longitude' = "numeric",
                 'pickup_latitude' = "numeric",
                 'RateCodeID' = "factor",
                 'store_and_fwd_flag' = "factor",
                 'dropoff_longitude' = "numeric",
                 'dropoff_latitude' = "numeric",
                 'payment_type' = "factor",
                 'fare_amount' = "numeric",
                 'extra' = "numeric",
                 'mta_tax' = "numeric",
                 'tip_amount' = "numeric",
                 'tolls_amount' = "numeric",
                 'improvement_surcharge' = "numeric",
                 'total_amount' = "numeric",
                 'u' = 'numeric')

## test code with first 1000 rows of first csv file
df_sample <- read.csv("Data/yellow_tripsample_2016-01.csv", nrows = 1000, colClasses = col_classes)

## create one xdf file by compressing and combining six csv files
input_xdf <- "Data/yellow_tripdata_2016.xdf"
require(lubridate)
most_recent_day <- ymd("2016-07-01")
st <- Sys.time()
for(ii in 1:6){ # get each month's data and append it to the first month's data
  file_date <- most_recent_day - months(ii)
  input_csv <- sprintf("Data/yellow_tripsample_%s.csv", substr(file_date, 1, 7))
  append <- if (ii == 1) "none" else "rows"
  rxImport(input_csv, input_xdf, append = append, overwrite = TRUE, colClasses = col_classes)
  print(input_xdf)
}
Sys.time() - st

## compare runtime among different approaches
nyc_xdf <- RxXdfData(input_xdf)
system.time(
  rxsum_xdf <- rxSummary(~ fare_amount, nyc_xdf)
)
rxsum_xdf

nyc_csv1 <- read.csv("Data/yellow_tripsample_2016-01.csv", colClasses = col_classes)
system.time(
  rxsum_csv1 <- rxSummary(~ fare_amount, nyc_csv1)
)
rxsum_csv1

nyc_csv2 <- read.csv("Data/yellow_tripsample_2016-01.csv", colClasses = col_classes)
system.time(
  sum_csv <- summary(nyc_csv2["fare_amount"])
)
sum_csv

####################################################################
## exercise1: convert csv to xdf, then get a summary of all its columns
## store the summary in an object called sum_xdf
input_csv <- "Data/yellow_tripsample_2016-01.csv"
input_xdf <- "Data/yellow_tripsample_2016-01.xdf"
rt_xdf <- system.time({
  rxImport(input_csv, input_xdf, colClasses = col_classes)
  sum_xdf <- rxSummary(~., input_xdf)
  
})
sum_xdf
file.remove(input_xdf)

## exercie2:
rt_csv <- system.time(sum_csv <- rxSummary(~., input_csv))

## exercise3: 
rt_xdf - rt_csv

##exercise4:
amt_xdf <- rxSummary(~ tip_amount+fare_amount+total_amount, input_xdf)
columns <- c("tip_amount", "fare_amount", "total_amount")
amt_csv <- rxSummary(~ tip_amount+fare_amount+total_amount, input_csv)

########################################################################
##  Preparing the data
########################################################################

## checking column types
rxGetInfo(nyc_xdf, getVarInfo = TRUE, numRows = 5)

## transformation: calculate tip percentage (option 1)
system.time(
  rxDataStep(nyc_xdf, nyc_xdf, overwrite = TRUE, 
           transforms = list(tip_percent = ifelse(fare_amount > 0 & tip_amount < fare_amount, round(tip_amount * 100/ fare_amount, 2), NA)),
           )
)
rxSummary(~ tip_percent, nyc_xdf)

## transformation: calculate tip percentage (option 2)
system.time(
  sum_tip <- rxSummary(~ tip_percent2, nyc_xdf,
            transforms = list(tip_percent2 = ifelse(fare_amount > 0 & tip_amount < fare_amount, round(tip_amount * 100/ fare_amount, 2), NA)),
            )
)
sum_tip

## transformation: get month and year counts
rxCrossTabs(~ month:year, nyc_xdf, 
            transforms = list(
                              date = ymd_hms(tpep_pickup_datetime),
                              year = factor(year(date), levels = 2014:2016),
                              month = factor(month(date), levels = 1:12)
                              ),
            transformPackages = "lubridate"
            )

## transformation: comparing trips based on day of week and time of day
## function to create columns of date and time
xforms <- function(data){
  
  weekday_labels <- c("Sun", "Mon", "Tue", "Wed", "Thu", "Fri","Sat")
  cut_levels <- c(1, 5, 9, 12, 16, 18, 22)
  hour_labels <- c("1AM-5AM", "5AM-9AM", "9AM-12PM", "12PM-4PM", "4PM-6PM", "6PM-10PM", "10PM-1AM")
  
  pickup_datetime <- ymd_hms(data$tpep_pickup_datetime, tz = "UTC")
  pickup_hour <- addNA(cut(hour(pickup_datetime), cut_levels))
  levels(pickup_hour) <- hour_labels
  pickup_dofw <- factor(wday(pickup_datetime), levels = 1:7, labels = weekday_labels)
  
  dropoff_datetime <- ymd_hms(data$tpep_dropoff_datetime, tz = "UTC")
  dropoff_hour <- addNA(cut(hour(dropoff_datetime), cut_levels))
  levels(dropoff_hour) <- hour_labels
  dropoff_dofw <- factor(wday(dropoff_datetime), levels = 1:7, labels = weekday_labels)
  
  data$pickup_hour <- pickup_hour
  data$pickup_dofw <- pickup_dofw
  data$dropoff_hour <- dropoff_hour
  data$dropoff_dofw <- dropoff_dofw
  data$trip_duration <- as.integer(dropoff_datetime - pickup_datetime)
  
  data
}

## test if the function works properly against sample created earlier
library(lubridate)
head(xforms(df_sample))
## test with rxDataStep
head(rxDataStep(df_sample, transformFunc = xforms, transformPackages = "lubridate"))

## run the transformation function xforms on the dataset
st <- Sys.time()
rxDataStep(nyc_xdf, nyc_xdf, overwrite = TRUE, transformFunc = xforms, transformPackages = "lubridate")
Sys.time() - st


## examing the new columns
rxs1 <- rxSummary(~ pickup_hour+pickup_dofw+dropoff_hour+dropoff_dofw+trip_duration, nyc_xdf)
rxs1$categorical <- lapply(rxs1$categorical, function(x) cbind(x, prop = round(prop.table(x$Counts), 2)))
rxs1

rxs2 <- rxSummary( ~ pickup_dofw:pickup_hour, nyc_xdf)
rxs2 <- tidyr::spread(rxs2$categorical[[1]], key = pickup_hour, value = Counts)
row.names(rxs2) <- rxs2[,1]
rxs2 <- as.matrix(rxs2[,-1])
rxs2

# visualization (heatmap)
levelplot(prop.table(rxs2, 2), cuts = 4, xlab = "",ylab = "", main = "Distribution of taxis by day of week")

library(gplots)

heatmap.2(prop.table(rxs2,2),dendrogram='none', Rowv=FALSE, Colv=FALSE, trace="none")

library(pheatmap)

pheatmap(prop.table(rxs2, 2))
rxs2_norm2 <- scale(rxs2)
pheatmap(rxs2_norm2)

install.packages(dendextend)
library(dendextend)

clust_rxs2 <- hclust(dist(rxs2_norm2))
as.dendrogram(clust_rxs2) %>% plot(horiz = TRUE)
rxs2_row <- cutree(tree = as.dendrogram(clust_rxs2), k = 2)
rxs2_row <- data.frame(cluster = ifelse(test = rxs2_row == 1, yes = "cluster 1", no = "cluster 2"))

clust_rxs2_t <- hclust(dist(t(rxs2_norm2)))
as.dendrogram(clust_rxs2_t) %>% plot(horiz = TRUE)
rxs2_col <- cutree(tree = as.dendrogram(clust_rxs2_t), k = 2)
rxs2_col <- data.frame(group = ifelse(test = rxs2_col == 1, yes = "group 1", no = "group 2"))

my_colors <- colorRampPalette(c("gray42", "white", "darkorange2"))(pallette_length)
pallette_length <- 49
my_breaks <- c(seq(min(rxs2_norm2), 0, length.out = ceiling(pallette_length/2)+1),
            seq(max(rxs2_norm2)/pallette_length, max(rxs2_norm2), length.out = floor(pallette_length/2)))

pheatmap(rxs2_norm2, color = my_colors, breaks = my_breaks, 
         annotation_row = rxs2_row, cutree_rows = 2,
         annotation_col = rxs2_col, cutree_cols = 2
         )

###############################################################
## mapping
###############################################################

## add pickup and drop-off neightborhoods
library(rgeos)
library(sp)
library(maptools)
library(rgdal)
library(stringr)

##nyc_shapefile <- readOGR("Data/ZillowNeighborhoods-NY/ZillowNeighborhoods-NY.shp")
nyc_shapefile <- readShapePoly("Data/ZillowNeighborhoods-NY/ZillowNeighborhoods-NY.shp")
mht_shapefile <- subset(nyc_shapefile, str_detect(CITY, "New York City-Manhattan"))

##bkly_shaplefile <- subset(nyc_shapefile, grepl("New York City-Brooklyn", nyc_shapefile))

mht_shapefile@data$id <- as.character(mht_shapefile@data$NAME)

library(broom)

mht.points <- tidy(gBuffer(mht_shapefile, byid = TRUE, width = 0), region = "NAME")

library(dplyr)

mht.df <- inner_join(mht.points, mht_shapefile@data, by = "id")
mht.cent <- mht.df %>%
  group_by(id) %>%
  summarise(long = median(long), lat = median(lat))


library(ggrepel)

ggplot(mht.df, aes(long, lat, fill = id))+
  geom_polygon()+
  geom_path(color = "white")+
  coord_equal()+
  theme(legend.position = "none")+
  geom_text_repel(aes(label = id), data = mht.cent, size = 3)

## finding neighborhoods - create columns with NA convered to 0
data_coords <- transmute(df_sample, 
                          long = ifelse(is.na(pickup_longitude), 0, pickup_longitude),
                          lat = ifelse(is.na(pickup_latitude), 0, pickup_latitude)
                          )
## specify the columns that correspond to the coordinates
coordinates(data_coords) <- c("long", "lat") 

## assign identical CRS to data_coords
##proj4string(data_coords) <- proj4string(mht_shapefile)

## returns the neighborhoods based on coordinates
nhds <- over(data_coords, mht_shapefile)
## rename the column names in nhds
names(nhds) <- paste("pickup", tolower(names(nhds)), sep = "_")
## combine the neighborhood information with the original data
df_sample <- cbind(df_sample, nhds[ , grep("name|city", names(nhds))])
head(df_sample)

#####################################################################################
## run the transformation on the whole data
## take the code and wrap it into a transformation function,
## pass that function to rxDataStep through transfromFunc augument
#####################################################################################
find_nhds <- function(data) {
  ## extra pick-up lat and long and find their neightborhood
  pickup_longitude <- ifelse(is.na(data$pickup_longitude), 0, data$pickup_longitude)
  pickup_latitude <- ifelse(is.na(data$pickup_latitude), 0, data$pickup_latitude)
  data_coords <- data.frame(long = pickup_longitude, lat = pickup_latitude)
  coordinates(data_coords) <- c("long", "lat")
  ##proj4string(data_coords) <- proj4string(shapefile)
  nhds <- over(data_coords, shapefile)
  
  ## add only the pick-up neighborhood and city columns to the data
  data$pickup_nhd <- nhds$NAME
  data$pickup_borough <- nhds$CITY
  
  ## extra drop-off lat and long and find their neighborhood
  dropoff_longitude <- ifelse(is.na(data$dropoff_longitude), 0, data$dropoff_longitude)
  dropoff_latitude <- ifelse(is.na(data$dropoff_latitude), 0, data$dropoff_latitude)
  data_coords <- data.frame(long = dropoff_longitude, lat = dropoff_latitude)
  ##proj4string(data_coords) <- proj4string(shapefile)
  coordinates(data_coords) <- c("long", "lat")
  nhds <- over(data_coords, shapefile)
  
  ## add only the drop-off neightbood and city columns to the data
  data$dropoff_nhd <- nhds$NAME
  data$dropoff_borough <- nhds$CITY
  
  ## return the data with the new columns added in
  data
}


## test the function on the data.frame using rxDataStep
head(rxDataStep(df_sample, transformFunc = find_nhds, transformPackages = c("sp", "maptools"),
                transformObjects = list(shapefile = mht_shapefile)))

head(rxDataStep(df_sample, transformFunc = find_nhds, transformPackages = c("sp", "maptools"),
                transformObjects = list(shapefile = nyc_shapefile)))

st <- Sys.time()
nyc_xdf_nhds <- RxXdfData(nyc_xdf)
rxDataStep(nyc_xdf, nyc_xdf_nhds, overwrite = TRUE, transformFunc = find_nhds, transformPackages = c("sp", "maptools", "rgeos"),
           transformObjects = list(shapefile = mht_shapefile))
Sys.time() - st
rxGetInfo(nyc_xdf, numRows = 5)

nyc_xdf_nhds_all <- RxXdfData(nyc_xdf)
rxDataStep(nyc_xdf, nyc_xdf_nhds_all, overwrite = TRUE, transformFunc = find_nhds, transformPackages = c("sp", "maptools", "rgeos"),
           transformObjects = list(shapefile = nyc_shapefile))


################### End of section ##################################################

## Exerciese
## Check the column type for payment_type
input_csv <- "Data/yellow_tripsample_2016-01.csv"
input_xdf <- "Data/yellow_tripsample_2016-01.xdf"
rxImport(input_csv, input_xdf, overwrite = TRUE)

nyc_jan_xdf <- RxXdfData(input_xdf)
rxGetInfo(nyc_jan_xdf, getVarInfo = TRUE,varsToKeep = "payment_type" )

## run a transformation that creates a column called card_vs_cash (type factor) based payment_type
transform_column <- function(x){
  x$card_vs_cash <- factor(x$payment_type, levels = c("1", "2"), labels = c("1", "2"))

  x
}

nyc_jan_xdf_new <- rxDataStep(nyc_jan_xdf, nyc_jan_xdf, overwrite = TRUE, transformFunc = transform_column)
rxGetInfo(nyc_jan_xdf_new, getVarInfo = TRUE)

#####################################################################################################
## Lab1
#####################################################################################################
nyc_lab1 <- "Data/nyc_lab1.xdf"
nyc_lab1_xdf <- RxXdfData(nyc_lab1)
rxGetInfo(nyc_lab1_xdf, getVarInfo = TRUE)

trans_col <- function(x){
  x$Ratecode_type_desc <- factor(x$RatecodeID, levels = c("1", "2", "3","4", "5", "6"), labels =  c("1", "2", "3","4", "5", "6"))
  x$payment_type_desc <- factor(x$payment_type, levels = c("1", "2"), labels = c("1", "2"))
  
  x
}
nyc_lab1_new <- rxDataStep(nyc_lab1_xdf, nyc_lab1_xdf, overwrite = TRUE, transformFunc = trans_col)
rxGetInfo(nyc_lab1_new, getVarInfo = TRUE, varsToKeep = c("Ratecode_type_desc", "payment_type_desc"))
rxSummary(~ Ratecode_type_desc + payment_type_desc, nyc_lab1_new)
rxSummary(~., nyc_lab1_new)



#####################################################################################################
## Examing the data, using nyc_xdf_nhds for neighborhoods
#####################################################################################################
system.time(
  rxs_all <- rxSummary(~., nyc_xdf_nhds)
)
rxs_all$sDataFrame

nbhs_by_borough <- rxCrossTabs(~ pickup_nhd:pickup_borough, nyc_xdf_nhds)
nbhs_by_borough <- nbhs_by_borough$counts[[1]]
nbhs_by_borough <- as.data.frame(nbhs_by_borough)
## get the neighborhoods by borough
lnbs <- lapply(names(nbhs_by_borough), function(vv) subset(nbhs_by_borough, nbhs_by_borough[ ,vv] > 0, select = vv, drop = FALSE))
lapply(lnbs, head)

###################################################################################
## Focusing on Manhattan
###################################################################################
mht_nbhs <- rownames(nbhs_by_borough)[nbhs_by_borough$`New York City-Manhattan` >0]

refactor_columns <- function(datalist){
  datalist$pickup_nb <- factor(datalist$pickup_nhd, levels = nbhs_levels)
  datalist$dropoff_nb <- factor(datalist$dropoff_nhd, levels = nbhs_levels)
  
  datalist
}

nyc_xdf_nhds2 <- RxXdfData(nyc_xdf_nhds)

rxDataStep(nyc_xdf_nhds, nyc_xdf_nhds2, overwrite = TRUE,
           transformFunc = refactor_columns,
           transformObjects = list(nbhs_levels = mht_nbhs))


rxs_pickdrop <- rxSummary(~ pickup_nb:dropoff_nb, nyc_xdf_nhds2)
head(rxs_pickdrop$categorical[[1]])
############################################################################################
## Examing trip distance
############################################################################################
rxHistogram(~ trip_distance, nyc_xdf_nhds2, startVal = 0, endVal = 25, histType = "Percent", numBreaks = 20)

rxs <- rxSummary(~ pickup_nb:dropoff_nb, nyc_xdf_nhds2, rowSelection = (trip_distance > 15 & trip_distance < 22))
head(arrange(rxs$categorical[[1]], desc(Counts)), 10)

############################################################################################
## Examing outliers
############################################################################################
odd_trips <- rxDataStep(nyc_xdf_nhds2, rowSelection = (
  u < 0.5 & (
    (trip_distance > 50 | trip_distance <= 0) |
    (passenger_count > 5 | passenger_count <= 0) |
    (fare_amount > 5000 | fare_amount <= 0)
  )), transforms = list(u = runif(.rxNumRows)))

print(dim(odd_trips))

odd_trips %>%
  filter(trip_distance > 50) %>%
  ggplot() -> p

p + geom_histogram(aes(x = fare_amount, fill = trip_duration <= 600), binwidth = 10) +
  xlim(0, 500) + coord_fixed(ratio = 25)

############################################################################################
## Filtering by Manhattan
############################################################################################
input_xdf <- "Data/yellow_tripdata_2016_mahattan.xdf"
mht_xdf <- RxXdfData(input_xdf)

rxDataStep(nyc_xdf_nhds2, mht_xdf, overwrite = TRUE,
           rowSelection = (
             passenger_count >0 &
               trip_distance >=0 & trip_distance < 30 &
               trip_duration >= 0 & trip_duration < 60*60*24 &
               str_detect(pickup_borough, "Manhattan") &
               str_detect(dropoff_borough, "Manhattan") &
               !is.na(pickup_nb) &
               !is.na(dropoff_nb) &
               fare_amount >0
           ),
           transformPackages = "stringr",
           varsToDrop = c("extra", "mta_tax", "improvement_surcharge", "total_amount",
                          "pickup_borough", "dropoff_borough", "pickup_nhd", "dropoff_nhd"))

mht_sample_df <- rxDataStep(mht_xdf, rowSelection = (u < 0.1),
                            transforms = list(u = runif(.rxNumRows)))
dim(mht_sample_df)

####################################################################
## Exercises
####################################################################
rxHistogram(~ trip_distance, nyc_jan_xdf_new, startVal = 0, endVal = 25, 
            histType = "Percent", numBreaks = 20)
## Modify the formula in the line above so that we get a separate histogram for card and cash customers (based on the 
## card_vs_cash column created in the last exercise.
rxHistogram(~ trip_distance|card_vs_cash, nyc_jan_xdf_new, startVal = 0, 
            endVal = 25, histType = "Percent", numBreaks = 20)
##  Modify the rxHistogram call in part (1) so that instead of plotting a histogram of trip_distance, you plot a bar plot
## of a column called trip_dist_bin which bins trip_distance based on the breakpoints provided in the above code example.
## Compute trip_dist_bin on the fly (using the transforms inside of rxHistogram).
rxHistogram(~ trip_dist_bin, nyc_jan_xdf_new, startVal = 0, 
            endVal = 25,histType = "Percent", 
            transforms = list(trip_dist_bin = cut(trip_distance, breaks = c(-Inf, 0, 5, 10, Inf), labels = c("0", "<5", "5-10", "10+"))))

####################################################################
## Visualizing the data
## 1. Reordering neighborhoods
####################################################################
rxct <- rxCrossTabs(trip_distance ~ pickup_nb:dropoff_nb, mht_xdf)
res <- rxct$sums$trip_distance / rxct$counts$trip_distance

library(seriation)
## dealing with missing value
apply(is.na(res), 1, sum) ## apply(is.na(res), 2, sum) works too
res[which(is.nan(res))] <- mean(res, na.rm = TRUE) 
## the distance matrix created in the previous step will be fed
## to seriate function to order it so closer neighborhoods appear
## next to each other (rather than sorted alphabetically)
nb_order <- seriate(res) 

## use rxCube to get counts and averages for trip_distance in the 
## form of data.frame
rxc1 <- rxCube(trip_distance ~ pickup_nb:dropoff_nb, mht_xdf)
rxc2 <- rxCube(minutes_per_mile ~ pickup_nb:dropoff_nb, mht_xdf, 
               transforms = list(minutes_per_mile = (trip_duration/60)/trip_distance))
rxc3 <- rxCube(tip_percent ~ pickup_nb:dropoff_nb, mht_xdf)
res <- bind_cols(list(rxc1, rxc2, rxc3))
res <- res[ , c('pickup_nb', 'dropoff_nb', 'trip_distance', 'minutes_per_mile', 'tip_percent')]
head(res)

library(ggplot2)
ggplot(res, aes(pickup_nb, dropoff_nb)) +
  geom_tile(aes(fill = trip_distance), color = "white") +
  theme(axis.text.x = element_text(angle = 90, hjust =1)) +
  scale_fill_gradient(low = "white", high = "steelblue") +
  coord_fixed(ratio = 1)

## unlist simplifies the list structure to produce a vector
## which contains all the automic components which occur in the list
newlevs <- levels(res$pickup_nb)[unlist(nb_order)]
res$pickup_nb <- factor(res$pickup_nb, levels = unique(newlevs))
res$dropoff_nb <- factor(res$dropoff_nb, levels = unique(newlevs))

## reordered trip distance heatmap
ggplot(res, aes(pickup_nb, dropoff_nb)) + 
  geom_tile(aes(fill = trip_distance), colour = "white") + 
  theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
  scale_fill_gradient(low = "white", high = "steelblue") + 
  coord_fixed(ratio = .9)

################################################################
## 2 Neighborhood trends
################################################################

## trip traffic heatmap
ggplot(res, aes(pickup_nb, dropoff_nb)) + 
  geom_tile(aes(fill = minutes_per_mile), colour = "white") + 
  theme(axis.text.x = element_text(angle = 60, hjust = 1)) + 
  scale_fill_gradient(low = "white", high = "steelblue") + 
  coord_fixed(ratio = .9)

## fare amount and how much passengers tip in relation to which 
## neightborhoods they travel between
res %>%
  mutate(tip_color = cut(tip_percent, c(0, 5, 8, 10, 12, 15, 100))) %>%
  ggplot(aes(pickup_nb, dropoff_nb)) + 
  geom_tile(aes(fill = tip_color)) + 
  theme(axis.text.x = element_text(angle = 60, hjust = 1)) + 
  coord_fixed(ratio = .9)

################################################################
## 3 Refactoring neighborhoods
################################################################
mht_xdf_new <- RxXdfData(mht_xdf)
rxDataStep(mht_xdf, mht_xdf_new, transforms = 
                            list(pickup_nb =
                                   factor(pickup_nb, levels = newlevs),
                                 dropoff_nb =
                                   factor(dropoff_nb, levels = newlevs)),
                          transformObjects = list(newlevs = unique(newlevs)),
                          overwrite = TRUE)

################################################################
## 4 Total and marginal distribution trips between neighborhoods
################################################################
rxc <- rxCube(~ pickup_nb:dropoff_nb, mht_xdf_new)
rxc <- as.data.frame(rxc)

library(dplyr)
rxc %>%
  filter(Counts >0) %>%
  mutate(pct_all = Counts/sum(Counts)*100) %>%
  group_by(pickup_nb) %>%
  mutate(pct_by_pickup_nb = Counts/sum(Counts) * 100) %>%
  group_by(dropoff_nb) %>%
  mutate(pct_by_dropoff_nb = Counts/sum(Counts) * 100) %>%
  group_by() %>%
  arrange(desc(Counts)) -> rxcs
head(rxcs)

## taxi trip distribution between neighborhoods
ggplot(rxcs, aes(pickup_nb, dropoff_nb)) + 
  geom_tile(aes(fill = pct_all), colour = "white") + 
  theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
  scale_fill_gradient(low = "white", high = "black") + 
  coord_fixed(ratio = .9)

## trips leaving a particular neighborhood spill out into 
## other neighborhoods
ggplot(rxcs, aes(pickup_nb, dropoff_nb)) + 
  geom_tile(aes(fill = pct_by_pickup_nb), colour = "white") + 
  theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
  scale_fill_gradient(low = "white", high = "steelblue") + 
  coord_fixed(ratio = .9)

## trips ending at a particular neighborhood
ggplot(rxcs, aes(pickup_nb, dropoff_nb)) + 
  geom_tile(aes(fill = pct_by_dropoff_nb), colour = "white") + 
  theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
  scale_fill_gradient(low = "white", high = "red") + 
  coord_fixed(ratio = .9)

################################################################
## 5 Exercises
################################################################
input_csv <- 'Data/yellow_tripsample_2016-01.csv'
input_xdf <- 'Data/yellow_tripsample_2016-01.xdf'
rxImport(input_csv, input_xdf, overwrite = TRUE)

nyc_jan_xdf <- RxXdfData(input_xdf)

## select the subset of rows with trip_distance greater than some threshold.
dist_threshold <- 5

nyc_long_trips_df <- rxDataStep(nyc_jan_xdf, overwrite = TRUE,
                                rowSelection = (trip_distance > threshold),
                                transformObjects = list(threshold = dist_threshold))

## how many rows do you have in the resulting subset nyc_long_trips_df
dim(nyc_long_trips_df)

################################################################
## 6 time-related patterns 
################################################################
res1 <- rxCube(tip_percent ~ pickup_dofw:pickup_hour, mht_xdf_new)
res2 <- rxCube(fare_amount/(trip_duration/60) ~ pickup_dofw:pickup_hour, mht_xdf_new)
names(res2)[3] <- "fare_per_minute"
res <- bind_cols(list(res1, res2))
res <- res[ , c("pickup_dofw", "pickup_hour", "fare_per_minute", "tip_percent", "Counts")]

library(ggplot2)
ggplot(res, aes(pickup_dofw, pickup_hour)) +
  geom_tile(aes(fill = fare_per_minute), color = "white") +
  theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
  scale_fill_gradient(low = "white", high = "steelblue") +
  geom_text(aes(label = sprintf('%dk riders\n (%d%% tip)', signif(Counts/1000, 2), round(tip_percent, 0))), size = 2.5) +
  coord_fixed(ratio = 0.9)
################################################################
## Lab 1 
################################################################
input_xdf <- "Data/mht_lab2.xdf"
mht_lab2_xdf <- RxXdfData(input_xdf)
names(mht_lab2_xdf)

## how many cash transactions with the JFK rate code are there in the data
rxCube(~payment_type, mht_lab2_xdf,
            rowSelection = (payment_type == 2) &
              (RatecodeID == 2)
          )

## short: trip_distance <= 5, long: trip_distance > 5
## short: trip_duration <= 10*60, long: trip_duration > 10
rxCube(~payment_type, mht_lab2_xdf,
       rowSelection = (payment_type == 1) &
         (trip_distance > 5) &
         (trip_duration > 0 & trip_duration <= 10*60)
)

## Which range do people mostly tip? (use histogram)
rxHistogram(~tip_percent|payment_type_desc, mht_lab2_xdf, numBreaks = 20,
            startVal = 0, endVal = 100, histType = "Percent")

## group the tip_percent into several buckets
rxHistogram(~tip_percent_bin|payment_type_desc, mht_lab2_xdf,
            startVal = 0, endVal = 100, histType = "Percent",
            transforms = list(tip_percent_bin = cut(tip_percent, breaks = c(-Inf, 5, 10, 15, 20, 25, Inf), labels = c("up to 5", "5-10", "10-15", "15-20", "20-25", "25+"))))

## plot the histogram of tip_percent by Ratecode_type_desc
rxHistogram(~tip_percent|Ratecode_type_desc, mht_lab2_xdf,
            startVal = 0, endVal = 100, histType = "Percent",
            rowSelection = (payment_type_desc == "card"),
            numBreaks = 20)
################################################################
## Clustering Example
## 1 Looking at maps
################################################################
library(ggmap)
library(ggplot2)
map_13 <- get_map(location = c(lon = -73.98, lat = 40.76), zoom = 13)
map_14 <- get_map(location = c(lon = -73.98, lat = 40.76), zoom = 14)
map_15 <- get_map(location = c(lon = -73.98, lat = 40.76), zoom = 15)

q0 <- ggmap(map_13) +
  geom_point(aes(x = dropoff_longitude, y = dropoff_latitude),
             data = mht_sample_df, alpha = 0.15, na.rm = TRUE,
             col = "red", size = 0.5) +
  theme(panel.spacing.x=unit(0.5, "lines"),panel.spacing.y=unit(1, "lines"))

q1 <- ggmap(map_14) +
  geom_point(aes(x = dropoff_longitude, y = dropoff_latitude),
             data = mht_sample_df, alpha = 0.15, na.rm = TRUE,
             col = "red", size = 0.5) +
  theme(panel.spacing.x=unit(0.5, "lines"),panel.spacing.y=unit(1, "lines"))

q2 <- ggmap(map_15) +
  geom_point(aes(x = dropoff_longitude, y = dropoff_latitude),
             data = mht_sample_df, alpha = 0.15, na.rm = TRUE,
             col = "red", size = 0.5) +
  theme(panel.spacing.x=unit(0.5, "lines"),panel.spacing.y=unit(1, "lines"))

require(gridExtra)
grid.arrange(q1,q2, ncol = 2)

################################################################
## 2 Creating clusters
################################################################
## run kmeans algorithm on sample dataset, determine centroids
xydata <- transmute(mht_sample_df, long_std = dropoff_longitude / -74,
                    lat_std = dropoff_latitude / 40)

start_time <- Sys.time()
rxkm_sample <- kmeans(xydata, centers = 300, iter.max = 2000, 
                      nstart = 50)
Sys.time() - start_time

centroids_sample <- rxkm_sample$centers %>%
  as.data.frame %>%
  transmute(long = long_std * (-74), lat = lat_std * 40,
            size = rxkm_sample$size)

head(centroids_sample)

## run rxKmeans on full dataset with the centroid generated from pervious step
start_time <- Sys.time()
rxkm <- rxKmeans(~long_std + lat_std, mht_xdf_new, mht_xdf_new,
                 outColName = "dropoff_cluster", centers = rxkm_sample$centers,
                 transforms = list(long_std = dropoff_longitude / -74,
                                   lat_std = dropoff_latitude /40),
                 blocksPerRead = 1, overwrite = TRUE, 
                 maxIterations = 100, reportProgress = -1)
Sys.time() - start_time

clsdf <- cbind(
  transmute(as.data.frame(rxkm$centers), 
            long = long_std * (-74),
            lat = lat_std * 40),
            size = rxkm$size, withiness = rxkm$withinss)
head(clsdf)

centroids_whole <- cbind(
  transmute(as.data.frame(rxkm$centers), 
            long = long_std*(-74), 
            lat = lat_std*40),
  size = rxkm$size, withinss = rxkm$withinss)

q1 <- ggmap(map_15) +
  geom_point(data = centroids_sample, aes(x = long, y = lat, alpha = size),
             na.rm = TRUE, size = 1, col = 'red') +
  theme(panel.spacing.x=unit(0.5, "lines"),panel.spacing.y=unit(1, "lines")) +
  labs(title = "centroids using sample data")

q2 <- ggmap(map_15) +
  geom_point(data = centroids_whole, aes(x = long, y = lat, alpha = size),
             na.rm = TRUE, size = 1, col = 'red') +
  theme(panel.spacing.x=unit(0.5, "lines"),panel.spacing.y=unit(1, "lines")) +
  labs(title = "centroids using whole data")

require(gridExtra)
grid.arrange(q1, q2, ncol = 2)

################################################################
## 3 Exercises
################################################################
input_csv <- 'Data/yellow_tripsample_2016-01.csv'
input_xdf <- 'Data/yellow_tripsample_2016-01.xdf'
rxImport(input_csv, input_xdf, overwrite = TRUE)

nyc_jan_xdf <- RxXdfData(input_xdf)

xydropoff <- rxDataStep(nyc_jan_xdf, rowSelection = (u < 0.1),
                        transforms = list(u = runif(.rxNumRows),
                                          long_std = dropoff_longitude / -74,
                                          lat_std = dropoff_latitude /40),
                        varsToKeep = c("dropoff_longitude", "dropoff_latitude"))
xydropoff <- xydropoff[ , c("long_std", "lat_std")]

nclus <- 50
kmeans_nclus <- kmeans(xydropoff, centers = nclus, iter.max = 2000, nstart = 1)
sum(kmeans_nclus$withinss)

## write a function find_wss, inputs the number of clusterr, 
## outputs the total WSSs (within-cluster sum of squares)
find_wss <- function(nclus, df){
  start_time <- Sys.time()
  kmeans_nclus <- kmeans(df, centers = nclus, iter.max = 2000, nstart = 1)
  sum_wss <- sum(kmeans_nclus$withinss)
  time_diff <- Sys.time() - start_time
  wss_df <- cbind(nclus, sum_wss, time_diff)
  names(wss_df) <- c("nclus", "sum_wss", "time_diff")
  wss_df
}
find_wss(50, xydropoff)
## looping nclus
nclus_seq <- seq(20, 1000, by = 20)
## solution 1: for loop, returning a list
for (nclus in nclus_seq){
  print(find_wss(nclus, xydropoff))
}
## solution 2: sapply, ruturning a data frame
nclust_sum_wss <- t(as.data.frame(sapply(nclus_seq, function(x) find_wss(x, xydropoff))
))

nclust_sum_wss_df <- as.data.frame(nclust_sum_wss)

nclust_sum_wss_df %>%
  ggplot(aes(nclus, sum_wss)) +
  geom_point()

################################################################
## Modeling Example
## 1 A linear model for tip percent
################################################################
form_1 <- as.formula(tip_percent ~ pickup_nb:dropoff_nb + pickup_dofw:pickup_hour)
rxlm_1 <- rxLinMod(form_1, data = mht_xdf_new, dropFirst = TRUE, covCoef = TRUE)
rxlm_1
summary(rxlm_1)

rxs <- rxSummary( ~ pickup_nb + dropoff_nb + pickup_hour + pickup_dofw, mht_xdf)
ll <- lapply(rxs$categorical, function(x) x[ , 1])
names(ll) <- c('pickup_nb', 'dropoff_nb', 'pickup_hour', 'pickup_dofw')
pred_df_1 <- expand.grid(ll)
pred_df_1 <- rxPredict(rxlm_1, data = pred_df_1, computeStdErrors = TRUE, writeModelVars = TRUE)
names(pred_df_1)[1:2] <- paste(c('tip_pred', 'tip_stderr'), 1, sep = "_")
head(pred_df_1, 10)

################################################################
## 2 Examining predictions
################################################################
## pickup neighborhoods vs dropoff neighborhood
ggplot(pred_df_1, aes(x = pickup_nb, y = dropoff_nb)) + 
  geom_tile(aes(fill = tip_pred_1), colour = "white") + 
  theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
  scale_fill_gradient(low = "white", high = "red") + 
  coord_fixed(ratio = .9)

## pickup of day of week vs pickup hour
ggplot(pred_df_1, aes(pickup_dofw, pickup_hour)) +
  geom_tile(aes(fill = tip_pred_1), color = "white") +
  theme(axis.text.x = element_text(angle = 60, hjust = 1)) +
  scale_fill_gradient(low = "white", high = "red") +
  coord_fixed(ratio = 0.9)

################################################################
## 3 Choosing between models
################################################################
form_2 <- as.formula(tip_percent ~ pickup_nb:dropoff_nb)
rxlm_2 <- rxLinMod(form_2, mht_xdf_new, dropFirst = TRUE, covCoef = TRUE)
pred_df_2 <- rxPredict(rxlm_2, pred_df_1, computeStdErrors = TRUE, writeModelVars = TRUE)
names(pred_df_2)[1:2] <- paste(c("tip_pred", "tip_stderr"), 2, sep = "_")

pred_df <- pred_df_2 %>%
  select(starts_with("tip_")) %>%
  cbind(pred_df_1) %>%
  arrange(pickup_nb, dropoff_nb, pickup_dofw, pickup_hour) %>%
  select( pickup_dofw, pickup_hour,pickup_nb, dropoff_nb, starts_with("tip_pred_")) 
head(pred_df)

ggplot(pred_df) +
  geom_density(aes(tip_pred_1, col = "complex")) +
  geom_density(aes(tip_pred_2, col = "simple")) +
  facet_grid(pickup_hour ~ pickup_dofw)

rxQuantile("tip_percent", mht_xdf_new, probs = seq(0, 1, by = 0.05))
pred_df %>%
  mutate_at(vars(tip_pred_1, tip_pred_2), funs(cut(., c(-Inf, 10, 18, 23, 25, Inf)))) %>%
  ggplot() +
  geom_bar(aes(tip_pred_1, fill = "complex", alpha = 0.5)) +
  geom_bar(aes(tip_pred_2, fill = "simple", alpha = 0.5)) +
  facet_grid(pickup_hour~pickup_dofw) +
  xlab("tip percent prediction") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1))

################################################################
## Exercises
################################################################
## (1) Build a linear model for predicting tip_percent using
## trip_duration and the interaction of pickup_dow and pickup_hour.
## Find out what your adjusted R-squared is by passing the model
## object to the summary function.
input_csv <- 'Data/yellow_tripsample_2016-01.csv'
input_xdf <- 'Data/yellow_tripsample_2016-01.xdf'
rxImport(input_csv, input_xdf, overwrite = TRUE)

nyc_jan_xdf <- RxXdfData(input_xdf)

rxDataStep(nyc_jan_xdf, nyc_jan_xdf, 
           transforms = list(
             card_vs_cash = factor(payment_type, levels = 1:2, labels = c('card', 'cash')),
             tip_percent = ifelse(tip_amount < fare_amount & fare_amount > 0, tip_amount / fare_amount, NA)
           ),
           overwrite = TRUE)

xforms <- function(data) { # transformation function for extracting some date and time features
  weekday_labels <- c('Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat')
  cut_levels <- c(1, 5, 9, 12, 16, 18, 22)
  hour_labels <- c('1AM-5AM', '5AM-9AM', '9AM-12PM', '12PM-4PM', '4PM-6PM', '6PM-10PM', '10PM-1AM')
  
  pickup_datetime <- ymd_hms(data$tpep_pickup_datetime, tz = "UTC")
  pickup_hour <- addNA(cut(hour(pickup_datetime), cut_levels))
  pickup_dow <- factor(wday(pickup_datetime), levels = 1:7, labels = weekday_labels)
  levels(pickup_hour) <- hour_labels
  
  dropoff_datetime <- ymd_hms(data$tpep_dropoff_datetime, tz = "UTC")
  
  data$pickup_hour <- pickup_hour
  data$pickup_dow <- pickup_dow
  data$trip_duration <- as.integer(as.duration(dropoff_datetime - pickup_datetime))
  
  data
}

rxDataStep(nyc_jan_xdf, nyc_jan_xdf, overwrite = TRUE, transformFunc = xforms, transformPackages = "lubridate")

formula_1 <- as.formula(tip_percent ~ trip_duration + pickup_dow:pickup_hour)
linmod_1 <- rxLinMod(formula_1, nyc_jan_xdf, dropFirst = TRUE, covCoef = TRUE)
summary(linmod_1)
## Adjusted R-squared: 0.00803 

## (2) add card_vs_cash as input, call the new formual formula_2
formula_2 <- as.formula(tip_percent ~ trip_duration + pickup_dow:pickup_hour + card_vs_cash)
linmod_2 <- rxLinMod(formula_2, nyc_jan_xdf, dropFirst = TRUE, covCoef = TRUE)
summary(linmod_2)
## Adjusted R-squared: 0.7089

## (3) Use rxPredict to put the predictions made by both models
## into the data as new columns called tip_pred_1 and tip_pred_2.
## Then use rxHistogram to plot the predictions.
nyc_jan_xdf_pred <- rxPredict(linmod_1, nyc_jan_xdf, computeStdErrors = TRUE,writeModelVars = TRUE)
names(nyc_jan_xdf_pred)
names(nyc_jan_xdf_pred)[26:27] <- paste(c("tip_pred", "tip_stderr"), 1, sep = "_")
nyc_jan_xdf_pred <- rxPredict(linmod_2, nyc_jan_xdf_pred, computeStdErrors = TRUE, writeModelVars = TRUE)
names(nyc_jan_xdf_pred)
names(nyc_jan_xdf_pred)[28:29] <- paste(c("tip_pred", "tip_stderr"), 2, sep = "_")


rxHistogram(~tip_pred_1,nyc_jan_xdf_pred,numBreaks = 20,startVal = 0,
            endVal = 1,histType = "Percent")
rxHistogram(~tip_pred_2,nyc_jan_xdf_pred,numBreaks = 20,startVal = 0,
            endVal = 1,histType = "Percent")

rxQuantile("tip_pred_1", nyc_jan_xdf_pred, probs = seq(0, 1, by = 0.05))
rxQuantile("tip_pred_2", nyc_jan_xdf_pred, probs = seq(0, 1, by = 0.05))

rxHistogram(~tip_pred_1_bin, nyc_jan_xdf_pred, numBreaks = 40,histType = "Percent",
            startVal = 0, endVal = 1,
            transforms = list(tip_pred_1_bin = cut(tip_pred_1, c(-Inf, 0.12, 0.13, 0.14, 0.15, Inf), 
                                                   labels = c("<12%", "12-13%", "13-14%", "14-15%", ">15%")))
            )
rxHistogram(~tip_pred_2_bin|card_vs_cash, nyc_jan_xdf_pred, numBreaks = 40,histType = "Percent",
            startVal = 0, endVal = 1,
            transforms = list(tip_pred_2_bin = cut(tip_pred_2, c(-Inf,0, 0.1, 0.2, 0.21, Inf), 
                                                   labels = c("<0", "0-10%", "10-20%", "20-21%", ">21%")))
)

## (4) replace predictions <0 with NA, > 25 with 25, rescale the predictions to between 0 and 25
## this method doesn't seem working better than the bin mothod above
pred_jan_test <- RxXdfData(nyc_jan_xdf_pred)
rxDataStep(nyc_jan_xdf_pred, pred_jan_test, overwrite = TRUE,
           transforms = list(tip_pred_1 = ifelse(tip_pred_1 < 0, NA,
                                                 ifelse(tip_pred_1 > 0.25, 0.25, tip_pred_1)),
                             tip_pred_2 = ifelse(tip_pred_2 < 0, NA,
                                                 ifelse(tip_pred_2 >25, 0.25, tip_pred_2)))
           )
rxDataStep(pred_jan_test, pred_jan_test,
           transforms = list(
             tip_pred_1 = rescale(tip_pred_1, to = c(0, 25)),
             tip_pred_2 = rescale(tip_pred_2, to = c(0, 25))
           ), overwrite = TRUE, transformPackages = "scales")

rxHistogram(~tip_pred_1,pred_jan_test,numBreaks = 20,startVal = 0,
            endVal = 25,histType = "Percent")
rxHistogram(~tip_pred_2,pred_jan_test,numBreaks = 20,startVal = 0,
            endVal = 25,histType = "Percent")

###############################################################
## 4 Using other algorithms
###############################################################
dir.create("output", showWarnings = FALSE)
rx_split_xdf <- function(xdf = mht_xdf_new,
                         split_perc = 0.75,
                         output_path = "output/split",
                         ...){
  ## create a new factor column "split" by randomly assign "train" and "test" to the data
  ## tempfile() generates temp file, takes 3 arguments: file name pattern, directory name, and file extension.
  outFile <- tempfile(fileext = "xdf") 
  ## binomial distribution function rbinom generates random deviates, (n, size, prob)
  ## n: number of observations; size: number of trials; prob:probability of success on each trial
  rxDataStep(inData = xdf, outFile = xdf, transforms = list(
    split = factor(ifelse(rbinom(.rxNumRows, size = 1, prob = splitperc), "train", "test"))
  ), 
  transformObjects = list(splitperc = split_perc), overwrite = TRUE, ...
  )
  
  ## split the data in two based on the column we just created
  ## rxSplit function split an input .xdf file or data frame into multiple .xdf files or a list of data frames
  ## a list of data frames or an invisibile list of RxXdfData data source objects corresponding to the created out files.
  splitDS <- rxSplit(inData = xdf,
                     outFilesBase = file.path(output_path, "train"),
                     splitByFactor = "split",
                     overwrite = TRUE)
  
  return(splitDS)
}

## split the data into two parts, two lists
mht_split <- rx_split_xdf(xdf = mht_xdf_new, 
                          varsToKeep = c('payment_type', 'fare_amount', 
                                        'tip_amount', 'tip_percent', 
                                        'pickup_hour', 'pickup_dofw',
                                        'pickup_nb', 'dropoff_nb'))
names(mht_split) <- c("train", "test")

## run 3 models and compare the cost
system.time(linmod <- rxLinMod(tip_percent ~ pickup_nb:dropoff_nb + pickup_dofw:pickup_hour, 
                               data = mht_split$train, reportProgress = 0))
system.time(dtree <- rxDTree(tip_percent ~ pickup_nb + dropoff_nb + pickup_dofw + pickup_hour, 
                             data = mht_split$train, pruneCp = "auto", reportProgress = 0))
system.time(dforest <- rxDForest(tip_percent ~ pickup_nb + dropoff_nb + pickup_dofw + pickup_hour, 
                                 mht_split$train, nTree = 10, importance = TRUE, useSparseCube = TRUE, reportProgress = 0))

trained.models <- list(linmod = linmod, dtree = dtree, dforest = dforest)
save(trained.models, file = "trained_models.Rdata")

##########################################################################################
## 5 Comparing predictions
##########################################################################################
## apply the algorithm to the small dataset
## function expand.grid creates a data frame from all combinations of the supplied vectors or factors.
## arguments: ... vectors, factors or a list of containing these; stringAsFactors, logical specifying if character vectors are converted to factors
pred_df <- expand.grid(ll) 
pred_df_1 <- rxPredict(trained.models$linmod, data = pred_df, predVarNames = "pred_linmod")
pred_df_2 <- rxPredict(trained.models$dtree, data = pred_df, predVarNames = "pred_dtree")
pred_df_3 <- rxPredict(trained.models$dforest, data = pred_df, predVarNames = "pred_dforest")
## do.call constructs and executes a function all from a name or a function and a list of arguments to be passed to it.
## do.call(what, args, quote = FALSE, envir = parent.frame())
pred_df <- do.call(cbind, list(pred_df, pred_df_1, pred_df_2, pred_df_3))
head(pred_df)

observed_df <- rxSummary(tip_percent ~ pickup_nb:dropoff_nb:pickup_dofw:pickup_hour, mht_xdf_new)
## check the structure of the categorical list
str(observed_df$categorical)
observed_df <- observed_df$categorical[[1]][ , c(2:6)]
pred_df <- inner_join(pred_df, observed_df, by = names(pred_df)[1:4])

ggplot(data = pred_df) +
  geom_density(aes(x = Means, col = "observed average")) +
  geom_density(aes(x = pred_linmod, col = "linmod")) +
  geom_density(aes(x = pred_dtree, col = "dtree")) +
  geom_density(aes(x = pred_dforest, col = "dforest")) +
  xlim(-1, 30) + 
  xlab("tip percent")

##########################################################################################
## 6 Judging predictive performance
## problem: ran rxpredict function with dtree and dforest models,
## new data were writted to a column "tip_percent_pred", not the column defiedn by predVarNames
##########################################################################################
rxPredict(trained.models$linmod, data = mht_split$test, outData = mht_split$test, predVarNames = "tip_percent_pred_linmod", overwrite = TRUE)
rxPredict(trained.models$dtree, data = mht_split$test, outData = mht_split$test, predVarNames = "tip_percent_pred_dtree", overwrite = TRUE)
rxPredict(trained.models$dforest, data = mht_split$test, outData = mht_split$test, predVarNames = "tip_percent_pred_dforest", overwrite = TRUE)

rxSummary(~ SE_linmod + SE_dtree + SE_dforest, data = mht_split$test,
          transforms = list(SE_linmod = (tip_percent - tip_percent_pred_linmod)^2,
                            SE_dtree = (tip_percent - tip_percent_pred_dtree)^2,
                            SE_dforest = (tip_percent - tip_percent_pred_dforest)^2))

## Error:The sample data set for the analysis has no variables
## tip_percent_pred_dtree, and tip_percent_pred_dforest were not writted correctly

##########################################################################################
## Lab 
##########################################################################################
names(mht_lab2_xdf)

lab2_formular <- as.formula(tip_percent ~ trip_duration + pickup_dow:pickup_hour)
lab2_linmod <- rxLinMod(lab2_formular, data = mht_lab2_xdf, dropFirst = TRUE, covCoef = TRUE)
summary(lab2_linmod)

lab2_formular2 <- as.formula(tip_percent ~ trip_duration + pickup_dow:pickup_hour + payment_type_desc)
lab2_linmod2 <- rxLinMod(lab2_formular2, data = mht_lab2_xdf, dropFirst = TRUE, covCoef = TRUE)
summary(lab2_linmod2)

rxPredict(lab2_linmod, data = mht_lab2_xdf, outData = mht_lab2_xdf, predVarNames = "tip_pred_1")
rxPredict(lab2_linmod2, data = mht_lab2_xdf, outData = mht_lab2_xdf, predVarNames = "tip_pred_2")
names(mht_lab2_xdf)

rxHistogram(~ tip_pred_1, data = mht_lab2_xdf, startVal = 0, endVal = 50)
rxHistogram(~ tip_pred_2, data = mht_lab2_xdf, startVal = 0, endVal = 50)
rxHistogram(~ tip_percent, data = mht_lab2_xdf, startVal = 0, endVal = 50)

##########################################################################################
## Deploying and scaling 
## 1 WODA (write once and deploy anywhere)
##########################################################################################
input_xdf <- "Data/yellow_tripdata_2016_mahattan.xdf"
nyc_xdf <- RxXdfData(input_xdf)

sqlConnString <- readLines("sqlServerConnString.txt")
sqlRowsPerRead <- 100000
sqlTable <- "NYCTaxiBig"

nyc_sql <- RxSqlServerData(connectionString = sqlConnString, rowsPerRead = sqlRowsPerRead,
                           table = sqlTable)

## dump the content of NYC_XDF into the SQL table nyc_sql, which is called NYCTaxiBig in the SQL database
system.time(
  rxDataStep(nyc_xdf, nyc_sql, overwrite = TRUE)
)

ccColInfo <- list(
  tpep_pickup_datetime = list(type = "character"),
  tpep_dropoff_datetime = list(type = "character"),
  passenger_count = list(type = "integer"),
  trip_distance = list(type = "numeric"),
  pickup_longitude = list(type = "numeric"),
  pickup_latitude = list(type = "numeric"),
  dropoff_longitude = list(type = "numeric"),
  dropoff_latitude = list(type = "numeric"),
  RateCodeID = list(type= "factor", levels = as.character(1:6), newLevels = c("standard", "JFK", "Newark", "Nassau or Westchester", "negotiated", "group ride")),
  store_and_fwd_flat = list(type = "factor", levels = c("Y", "N")),
  payment_type = list(type = "factor", levels = as.character(1:2), newLevels = c("card", "cash")),
  fare_amount = list(type = "numeric"),
  tip_amount = list(type = "numeric"),
  total_amount = list(type = "numeric")
)

weekday_labels <- c("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")
hour_labels <- c("1AM-5AM", "5AM-9AM", "9AM-12PM", "12PM-4PM", "4PM-6PM", "6PM-10PM", "10PM-1AM")

ccColInfo$pickup_dofw <- list(type = "factor", levels = weekday_labels)
ccColInfo$pickup_hour <- list(type = "factor", levels = hour_labels)
ccColInfo$dropoff_dofw <- list(type = "factor", levels = weekday_labels)
ccColInfo$dropoff_hour <- list(type = "factor", levels = hour_labels)
ccColInfo

nyc_shapefile <- readShapePoly('Data/ZillowNeighborhoods-NY/ZillowNeighborhoods-NY.shp')
mht_shapefile <- subset(nyc_shapefile, str_detect(CITY, "New York City-Manhattan"))
manhattan_nhoods <- as.character(mht_shapefile@data$NAME)

ccColInfo$pickup_nb <- list(type = "factor", levels = manhattan_nhoods)
ccColInfo$dropoff_nb <- list(type = "factor", levels = manhattan_nhoods)

nyc_sql <- RxSqlServerData(connectionString = sqlConnString, table = sqlTable, rowsPerRead = sqlRowsPerRead, colInfo = ccColInfo)
rxGetInfo(nyc_sql, getVarInfo = TRUE, numRows = 3)

system.time(
  rxsum_sql <-rxSummary(~., nyc_sql)
)
rxsum_sql

system.time(
  linmod <- rxLinMod(tip_percent ~ pickup_nb:dropoff_nb + pickup_dofw:pickup_hour,
                     data = nyc_sql, reportProgress = 0,
                     rowSelection = (u < 0.75))
)

sqlTable <- "NYCTaxiScore"
sqlConnString <- readLines("sqlServerConnString.txt")
nyc_score <- RxSqlServerData(connectionString = sqlConnString, rowsPerRead = sqlRowsPerRead,
                             table = sqlTable)
names(nyc_score)
rxPredict(trained.models$linmod, data = nyc_sql, outData = nyc_score, predVarNames = "tip_percent_pred_linmod",
          overwrite = TRUE, rowSelection = (u >= 0.74))
