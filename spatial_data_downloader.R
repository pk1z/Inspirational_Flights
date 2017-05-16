#!/usr/bin/env Rscript

###################################################################################################################
#                                                                                                                 #
# Returns the geospatial data of airports from skyscanner's database.                                             #
# Register for apikey here: http://portal.business.skyscanner.net/en-gb/accounts/login/                           #
#                                                                                                                 #
# usage:                                                                                                          #
# 	~$ Rscript spatial_data_downloader.R './data' '{apikey}'                                                      #
#                                                                                                                 #
###################################################################################################################

######################################## LIBRARIES ##########################################################

library(jsonlite)
library(data.table)
library(stringr)

######################################## ARGUMENTS #######################################################

# taking arguments over
args = commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop('API key argument should be supplied.', call. = FALSE)
} else {
  if (length(args) == 2 ) {
    wd = args[1]
    apk = args[2]
  } else {
    stop('Too many arguments are supplied.', call. = FALSE)
  }
}

######################################## SCRIPT ##########################################################


# composing download link 3rd part with an argument
ApiKey <- paste('?apiKey', apk, sep = '=')
if (nchar(ApiKey) == 8) {
  keydt <- fread('~/Documents/creds.csv')
  ApiKey <- paste('?ApiKey', keydt[ProjectTitle == 'Insiprational_Flights', 3], sep = '=')
  rm(keydt)
}
  
# composing download links with arguments
link_geo_catalog <- paste('http://partners.api.skyscanner.net/apiservices/geo/v1.0', ApiKey, sep = '')
link_locales <- paste('http://partners.api.skyscanner.net/apiservices/reference/v1.0/locales', ApiKey, sep = '')
link_currencies <- paste('http://partners.api.skyscanner.net/apiservices/reference/v1.0/currencies', ApiKey, sep = '')


# downloading json files
geo_catalog <- jsonlite::fromJSON(link_geo_catalog, simplifyDataFrame = TRUE)
currencies <- jsonlite::fromJSON(link_currencies, simplifyDataFrame = TRUE)
locales <- jsonlite::fromJSON(link_locales, simplifyDataFrame = TRUE)
rm(list = c('apk', 'ApiKey', 'link_geo_catalog', 'link_locales', 'link_currencies'))

# creating continents table & formatting header
continents <- data.table(geo_catalog[[1]])
setnames(continents, 'Id', 'ContinentId')
setnames(continents, 'Name', 'ContinentName')
rm(geo_catalog)

# creating countries table & formatting header
countries <- rbindlist(lapply(continents[, ContinentId], 
                              function(i) 
                                cbind(rbindlist(continents[ContinentId == i, Countries], fill = TRUE), 
                                      continents[ContinentId == i, .(ContinentId, ContinentName)])
                              ), 
                       fill = TRUE)
setnames(countries, 'Id', 'CountryId')
setnames(countries, 'Name', 'CountryName')
rm(continents)

# creating regions table & formatting header
regions <- rbindlist(countries$Regions, fill = TRUE)
setnames(regions, 'Id', 'RegionId')
setnames(regions, 'Name', 'RegionName')

# creating cities table & formatting headercurrencies
cities <- rbindlist(countries$Cities, fill=TRUE)
cities[is.na(CountryId), CountryId := 'NA']
setnames(cities, 'Id', 'CityId')
setnames(cities, 'Name', 'CityName')
setnames(cities, 'Location', 'CityLocation')

# creating airports table & formatting header
airports <- rbindlist(cities$Airports, fill = TRUE)
airports[is.na(CountryId), CountryId := 'NA']
setnames(airports, 'Id', 'AirportId')
setnames(airports, 'Name', 'AirportName')
setnames(airports, 'Location', 'AirportLocation')

# joining airports & cities
setkey(airports, CityId, CountryId, RegionId)
setkey(cities, CityId, CountryId, RegionId)
all <- copy(cities[airports])
all[, Airports := NULL]
rm(list = c('airports', 'cities'))

# joining airports & cities & regions
setkey(all, RegionId, CountryId)
setkey(regions, RegionId, CountryId)
all <- regions[all]
rm(regions)

# joining airports & cities & regions & countries
setkey(all, CountryId)
setkey(countries, CountryId)
all <- countries[all]
all[, Regions := NULL]
all[, Cities := NULL]
rm(countries)

# splitting geocodes into Lon and Lat field
all[, ':=' (CityLon = as.numeric(str_split(all[, CityLocation], ', ', n = 2, simplify = TRUE)[, 1]),
            CityLat = as.numeric(str_split(all[, CityLocation], ', ', n = 2, simplify = TRUE)[, 2]),
            AirportLon = as.numeric(str_split(all[, AirportLocation], ', ', n = 2, simplify = TRUE)[, 1]),
            AirportLat = as.numeric(str_split(all[, AirportLocation], ', ', n = 2, simplify = TRUE)[, 2]))]
all[, ':=' (CityLocation = NULL,
            AirportLocation = NULL)]

# creating currencies table & formatting header
currencies <- rbindlist(currencies, fill = TRUE)
setnames(currencies, 'Code', 'Id')
colnames(currencies) <- paste('Currency', colnames(currencies), sep = '')

# joining airports & cities & regions & countries & currencies
setkey(currencies, CurrencyId)
setkey(all, CurrencyId)
all <- currencies[all]
rm(currencies)

# creating locales table & formatting header
locales <- rbindlist(locales, fill = TRUE)
colnames(locales) <- paste('Locales', colnames(locales), sep = '')
locales[, CountryId := substr(LocalesCode, 4, 5)]

# joining airports & cities & regions & countries & currencies & locales
setkey(locales, CountryId)
setkey(all, CountryId)
all <- locales[all]
rm(locales)

# reordering columns
setcolorder(all, order(colnames(all[, ])))

# saving result
file_out = paste(wd, '/geo_catalog.csv', sep = '')
fwrite(all,
       file = file_out,
       quote = TRUE,
       sep = ';',
       nThread = getDTthreads(),
       showProgress = TRUE)
rm(list = c('wd', 'file_out'))
