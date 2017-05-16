#!/usr/bin/env Rscript

###########################################################################################################################
#                                                                                                                         #
# Returns the cheapest quotes that meet this query. The prices come from skyscanner's cached prices resulting             #
# from their users’ searches.                                                                                             #
#                                                                                                                         #
# Register for apikey here: http://portal.business.skyscanner.net/en-gb/accounts/login/                                   #
#                                                                                                                         #
# {working directory}                                                                                                     #
# {timestamp_serialnumber}                                                                                                #
# {country}                                                                                                               #
# {currency}                                                                                                              #
# {locale}                                                                                                                #
# {origin place}                                                                                                          #
# {destination place}                                                                                                     #
# {outbound partial date}                                                                                                 #
# {SkyScanner API key}                                                                                                    #
# {inbound partial date}     <- not compulsory                                                                            #
#                                                                                                                         #
# usage:                                                                                                                  #
# ~$ Rscript browse_routes.R './data' '20170508_215017_1' 'HU' 'HUF' 'hu-HU' 'HU' 'UK' '2017-05' '{apikey}' '2017-05'     #
#                                                                                                                         #
###########################################################################################################################


######################################## LIBRARIES ##########################################################

library(jsonlite)
library(data.table)
library(stringr)

######################################## ARGUMENTS #######################################################

# taking arguments over
args = commandArgs(trailingOnly = TRUE)
if (length(args) < 10) {
  stop('10 arguments should be supplied.', call. = FALSE)
} else {
  if (length(args) == 10 ) {
    wd = args[1]
    tn = args[2]
    co = args[3]
    cu = args[4]
    lo = args[5]
    orp = args[6]
    dep = args[7]
    oupd = args[8]
    apk = args[9]
    inpd = args[10]
    } else {
      stop('Too many arguments are supplied.', call. = FALSE)
    }
}
######################################## SCRIPT ##########################################################


# composing download link 1st part with arguments
link1 <- paste('http://partners.api.skyscanner.net/apiservices/browseroutes/v1.0', co, cu, lo, orp, dep, oupd, sep = '/')

# composing download link 2nd part with an arguments
link2 <- paste('/', inpd, sep ='')
if (nchar(link2) == 1) {
  link2 = ''
}

# composing download link 3rd part with an argument
link3 <- paste('?ApiKey', apk, sep = '=')
if (nchar(link3) == 8) {
  keydt <- fread('~/Documents/creds.csv')
  link3 <- paste('?ApiKey', keydt[ProjectTitle == 'Insiprational_Flights', 3], sep = '=')
  rm(keydt)
} 

# composing download links with arguments from all parts
link <- paste(link1, link2, link3, sep = '')

# downloading json file
tmp <- jsonlite::fromJSON(link, simplifyDataFrame = TRUE)

# extracting Quotes table from json
Quotes <- data.table(flatten(tmp$Quotes, recursive = TRUE))

# replacing empty cells with NA
Quotes[Quotes == ''] <- NA

# renaming header of Quotes table
colnames(Quotes) <- gsub('Ids', 'Id', 
                         gsub('Inbound', 'I', 
                              gsub('Outbound', 'O', 
                                   gsub('Leg', 'L', 
                                        gsub('\\.', '_', 
                                             colnames(Quotes))))))

# refromatting some columns  of Quotes table
Quotes[, ':=' (OL_CarrierId = as.integer(as.character(OL_CarrierId)),
               IL_CarrierId = as.integer(as.character(IL_CarrierId)),
               QuoteDateTime = as.POSIXct(QuoteDateTime, tz = '', fromat = '%Y-%m-%dT%H:%M:%S'),
               OL_DepartureDate = as.POSIXct(OL_DepartureDate, tz = '', fromat = '%Y-%m-%dT%H:%M:%S'),
               IL_DepartureDate = as.POSIXct(IL_DepartureDate, tz = '', fromat = '%Y-%m-%dT%H:%M:%S'))]


# extracting Places table from json
Places <- data.table(flatten(tmp$Places, recursive = TRUE))

# replacing empty cells with NA
Places[Places == ''] <- NA


# extracting Carriers table from json
Carriers <- data.table(flatten(tmp$Carriers, recursive = TRUE))

# replacing empty cells with NA
Carriers[Carriers == ''] <- NA


# extracting Currencies table from json
Currencies <- data.table(flatten(tmp$Currencies, recursive = TRUE))
colnames(Currencies) <- paste('Currency', colnames(Currencies), sep = '')

# replacing empty cells with NA
Currencies[Currencies == ''] <- NA


# joining OutboundLeg_Origin details
tmp <- copy(Places[, ])
colnames(tmp) <- paste('OL_Origin', colnames(tmp), sep = '')
setnames(tmp, 'OL_OriginPlaceId', 'OL_OriginId')
tmp <- tmp[, c(1, 5, 2, 3), with = FALSE]

setkey(Quotes, OL_OriginId)
setkey(tmp, OL_OriginId)
Quotes <- tryCatch(tmp[Quotes], message = function(e) e)


# joining OutboundLeg_Destination details
tmp <- copy(Places[, ])
colnames(tmp) <- paste('OL_Destination', colnames(tmp), sep = '')
setnames(tmp, 'OL_DestinationPlaceId', 'OL_DestinationId')
tmp <- tmp[, c(1, 5, 2, 3), with = FALSE]

setkey(Quotes, OL_DestinationId)
setkey(tmp, OL_DestinationId)
Quotes <- tryCatch(tmp[Quotes], message = function(e) e)


# joining InboundLeg_Origin details
tmp <- copy(Places[, ])
colnames(tmp) <- paste('IL_Origin', colnames(tmp), sep = '')
setnames(tmp, 'IL_OriginPlaceId', 'IL_OriginId')
tmp <- tmp[, c(1, 5, 2, 3), with = FALSE]

setkey(Quotes, IL_OriginId)
setkey(tmp, IL_OriginId)
Quotes <- tryCatch(tmp[Quotes], message = function(e) e)


# joining InboundLeg_Destination details
tmp <- copy(Places[, ])
colnames(tmp) <- paste('IL_Destination', colnames(tmp), sep = '')
setnames(tmp, 'IL_DestinationPlaceId', 'IL_DestinationId')
tmp <- tmp[, c(1, 5, 2, 3), with = FALSE]

setkey(Quotes, IL_DestinationId)
setkey(tmp, IL_DestinationId)
Quotes <- tryCatch(tmp[Quotes], message = function(e) e)


# dropping unnecessary table
rm(Places)


# joining OutboundLeg_Carrier details
tmp <- copy(Carriers[, ])
colnames(tmp) <- c('OL_CarrierId', 'OL_CarrierName')

setkey(Quotes, OL_CarrierId)
setkey(tmp, OL_CarrierId)
Quotes <- tryCatch(tmp[Quotes], message = function(e) e)


# joining InboundLeg_Carrier details
tmp <- copy(Carriers[, ])
colnames(tmp) <- c('IL_CarrierId', 'IL_CarrierName')

setkey(Quotes, IL_CarrierId)
setkey(tmp, IL_CarrierId)
Quotes <- tryCatch(tmp[Quotes], message = function(e) e)


# dropping unnecessary table
rm(Carriers)


# adding Currency details
Quotes <- tryCatch(do.call(cbind, list(Quotes, Currencies[, .(CurrencyCode, CurrencySymbol, CurrencyThousandsSeparator, CurrencyDecimalSeparator)])), message = function(e) e)


# dropping unnecessary table
rm(Currencies)


# reordering columns & rows
setcolorder(Quotes, c('QuoteId', 'MinPrice', 'CurrencySymbol', 'Direct', 'QuoteDateTime', 
                      'OL_CarrierId', 'OL_CarrierName', 'OL_OriginName', 'OL_OriginType', 'OL_OriginId', 'OL_OriginIataCode', 'OL_DepartureDate', 
                      'OL_DestinationId', 'OL_DestinationName', 'OL_DestinationType', 'OL_DestinationIataCode', 
                      'IL_CarrierId', 'IL_CarrierName', 'IL_OriginName', 'IL_OriginType', 'IL_OriginId', 'IL_OriginIataCode', 'IL_DepartureDate', 
                      'IL_DestinationId', 'IL_DestinationName', 'IL_DestinationType', 'IL_DestinationIataCode', 
                      'CurrencyCode', 'CurrencyThousandsSeparator',  'CurrencyDecimalSeparator'))
Quotes <- tryCatch(Quotes[order(QuoteId)], message = function(e) e)


# creating new output filename by adding direction and date to the name of the result file
outfile <- paste('routes_', orp, '_', dep, '_', tn, '.csv', sep = '')


# saving results
setwd(wd)
fwrite(Quotes, 
       file = outfile,
       quote = TRUE,
       sep = ';',
       nThread = getDTthreads(),
       showProgress = TRUE)


# dropping unnecessary tables and variables
rm(Quotes, tmp, apk, co, cu, tn, dep, inpd, link1, link2, link3, link, lo, orp, oupd, outfile, wd)
