# Title     : TODO
# Objective : TODO
# Created by: paul
# Created on: 06/03/2019

suppressMessages(library(DBI))
suppressMessages(library(RMySQL))
suppressMessages(library(ReGenesees))

args = commandArgs(trailingOnly = TRUE)

m <- dbDriver("MySQL")
con <- dbConnect(m, user = args[1], password = args[2], host = args[3], dbname = args[4])

poprowvec <- dbReadTable(con, "poprowvec_unsamp")
survey_input_aux <- dbReadTable(con, "survey_unsamp_aux")

#delete columns not needed
poprowvec$C_group <- NULL

#delete records with missing design weight
survey_input_aux$OOHDesignWeight[survey_input_aux$OOHDesignWeight == 0] <- NA
survey_input_aux <- survey_input_aux[complete.cases(survey_input_aux$OOHDesignWeight),]

#declare factors
survey_input_aux[, "T_"] <- factor(survey_input_aux[, "T1"])

#set up survey design
des <- e.svydesign(data = survey_input_aux, ids = ~ SERIAL, weights = ~ OOHDesignWeight)

df.population <- as.data.frame(poprowvec)

pop.template(data = survey_input_aux, calmodel = ~ T_ - 1)
population.check(df.population, data = survey_input_aux, calmodel = ~ T_ - 1)

#call regenesees
survey_input_aux[, "unsamp_traffic_weight"] <- weights(e.calibrate(des, df.population, calmodel = ~ T_ - 1, calfun = "linear", aggregate.stage = 1))

R_UNSAMPLED <- survey_input_aux
R_UNSAMPLED[, "UNSAMP_TRAFFIC_WT"] <- R_UNSAMPLED[, "unsamp_traffic_weight"] / R_UNSAMPLED[, "OOHDesignWeight"]

dbWriteTable(conn = con, name = SQL('R_UNSAMPLED'), value = R_UNSAMPLED, append = TRUE, row.names=F)

dbDisconnect(con)