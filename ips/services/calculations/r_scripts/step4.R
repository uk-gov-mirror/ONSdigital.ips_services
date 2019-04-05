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

poprowvec <- dbReadTable(con, "poprowvec_traffic")
survey_input_aux <- dbReadTable(con, "survey_traffic_aux")

# delete columns not needed
poprowvec$C_group <- NULL
poprowvec$index <- NULL

#delete records with missing design weight
survey_input_aux$trafDesignWeight[survey_input_aux$trafDesignWeight == 0] <- NA
survey_input_aux <- survey_input_aux[complete.cases(survey_input_aux$trafDesignWeight),]

# declare factors
survey_input_aux[, "T_"] <- factor(survey_input_aux[, "T1"])

#set up survey design
des <- e.svydesign(data = survey_input_aux, ids = ~ SERIAL, weights = ~ trafDesignWeight)

df.population <- as.data.frame(poprowvec)

pop.template(data = survey_input_aux, calmodel = ~ T_ - 1)
population.check(df.population, data = survey_input_aux, calmodel = ~ T_ - 1)

#call regenesees
survey_input_aux[, "tw_weight"] <- weights(e.calibrate(des, df.population, calmodel = ~ T_ - 1, calfun = "linear", aggregate.stage = 1))

survey_input_aux[, "TRAFFIC_WT"] <- survey_input_aux[, "tw_weight"] / survey_input_aux[, "trafDesignWeight"]

dbWriteTable(conn = con, name = SQL('r_traffic'), value = survey_input_aux, append = TRUE, row.names=F)

dbDisconnect(con)
