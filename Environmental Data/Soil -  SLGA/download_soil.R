library(terra)

r <- rast(' /vsicurl/https://data.tern.org.au/landscapes/slga/NationalMaps/SoilAndLandscapeGrid/AVP/AVP_000_005_EV_N_P_AU_TRN_N_20220826.tif')
r <- rast('/vsicurl/https://esoil.io/TERNLandscapes/Public/Products/TERN/Covariates/Mosaics/90m/PM_Gravity.tif')

writeRaster(r, './')

apikey <- paste0('apikey:', 'azhHM2hEb3BrSm9IZHpnUC5eI0FSNXJjL3pkNSQKCmo6ZmQ8XHs/XU54ID0vXyBefihpIQl1CyUqRl9UT3M9QnxoC01UVzxccHxLPTh9d3c2')
r2 <- rast(paste0('/vsicurl/https://',apikey,'@data.tern.org.au/landscapes/slga/NationalMaps/SoilClassifications/ASC/90m/ASC_EV_C_P_AU_TRN_N.cog.tif'))
plot(r2)
