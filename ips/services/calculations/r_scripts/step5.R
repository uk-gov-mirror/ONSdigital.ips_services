#
library(DBI)
library(RMySQL)

# hide annoying regenesees output
if (.Platform$OS.type == "unix") {
    sink("/dev/null")
} else {
    sink("nul:")
}

suppressMessages(library(ReGenesees))
suppressMessages(library(DBI))
suppressMessages(library(RMySQL))

args <- commandArgs(trailingOnly = TRUE)

m <- dbDriver("MySQL")
con <- dbConnect(m, user = args[1], password = args[2], host = args[3], dbname = args[4])

poprowvec <- dbReadTable(con, "POPROWVEC_UNSAMP")
survey_input_aux <- dbReadTable(con, "SURVEY_UNSAMP_AUX")

#delete columns not needed
poprowvec$C_GROUP <- NULL

#delete records with missing design weight
survey_input_aux$OOH_DESIGN_WEIGHT[survey_input_aux$OOHDesignWeight == 0] <- NA
survey_input_aux <- survey_input_aux[complete.cases(survey_input_aux$OOH_DESIGN_WEIGHT),]

# declare factors
survey_input_aux[, "T_"] <- factor(survey_input_aux[, "T1"])

# set up survey design
des <- e.svydesign(data = survey_input_aux, ids = ~ SERIAL, weights = ~ OOH_DESIGN_WEIGHT)

df.population <- as.data.frame(poprowvec)

pop.template(data = survey_input_aux, calmodel = ~ T_ - 1)
population.check(df.population, data = survey_input_aux, calmodel = ~ T_ - 1)

# call regenesees
survey_input_aux[, "UNSAMP_TRAFFIC_WEIGHT"] <- weights(e.calibrate(des, df.population, calmodel = ~ T_ - 1, calfun = "linear", aggregate.stage = 1))

R_UNSAMPLED <- survey_input_aux
R_UNSAMPLED[, "UNSAMP_TRAFFIC_WT"] <- R_UNSAMPLED[, "UNSAMP_TRAFFIC_WEIGHT"] / R_UNSAMPLED[, "OOH_DESIGN_WEIGHT"]

dbWriteTable(conn = con, name = 'R_UNSAMPLED', value = R_UNSAMPLED, append = TRUE, row.names=F)

dbDisconnect(con)