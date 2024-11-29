rm(list = ls())
library(data.table)
library(RMySQL)
library(RJDBC)
library(dplyr)
library(httr)
library(stringr)
library(xlsx)
library(lubridate)
library(xtable)
library(mailR)
library(aws.s3)
library(googlesheets4)
library(iotools)
library(stringi)


#open_month <- read.csv("open_reminder.csv", sep =";")
#open <- read.csv("atlas_open_20210816.csv", sep =";")
#click_month <- read.csv("click_reminder.csv", sep =";")
#click <- read.csv("atlas_click_20210816.csv", sep =";")
Invite_Date <- as.Date('2022-03-02')
#NumberOfDays = as.numeric(Sys.Date() - Invite_Date)

#Ajustar a credenciales propcias
setwd("/rstudio-data/users/sseidler/Panelmanagement")
credentials <- fread("R_keys.csv")

mydb = dbConnect(
  MySQL(),
  user = credentials[dbname == "ppi", user],
  password = credentials[dbname == "ppi", password],
  dbname = credentials[dbname == "ppi", dbname],
  host = 'ppidb-users-rr.us-east-1.pro.netquestapps.com'
)
redshift_user <- credentials[dbname == "ntq", user]
redshift_pwd <- credentials[dbname == "ntq", password]
redshift_driver_url <- credentials[dbname == "ntq", driver]

redshift_driver <- JDBC("com.amazon.redshift.jdbc42.Driver", redshift_driver_url, identifier.quote="`")
redshift_url <- paste0("jdbc:redshift://redshift.bi-pro.netquestapps.com:5439/ntq?user=",redshift_user,"&password=",redshift_pwd)

redshift_conn <- dbConnect(redshift_driver, redshift_url)

query1 <-  dbSendQuery(mydb, paste("select Pane.p_codigo, enc_nombre, evt_fecha_creacion, SEL.pv_valor AS SEL, Pane.p_pais, Pane.p_sexo, Pane.p_fecha_nacimiento,  Pane.p_fecha_registro, Pane.p_estado,Devi.pdev_p_id, Devi.pdev_creation_timestamp, Devi.pdev_last_login_timestamp, Devi.pdev_session_status, Devi.pdev_platform, Devi.pdev_os, panelistas_variables_select.pv_valor, Devi.pdev_version_code, Devi.pdev_version_name
from panelistas Pane
left join eventos_panelista Even on Even.evt_p_id=Pane.p_id and Even.evt_tipo=20
left join encuestas Encu on Even.evt_enc_id=Encu.enc_id
left join panelistas_variables_select on p_id = pv_p_id AND pv_vd_id in (1074412974,1074413449,1074413717,1074414111)
left join panelistas_variables_select SEL on p_id=SEL.pv_p_id and SEL.pv_vd_id in (201148692,492590351,128920256,787050742,201301781)
left join (
select pdev_p_id,MAX(pdev_creation_timestamp) as MAXCrea,MAX(pdev_last_login_timestamp) as MAXLog
from panelistas_devices
group by pdev_p_id ) Recr on Recr.pdev_p_id=Pane.p_id
left join panelistas_devices Devi on Recr.pdev_p_id=Devi.pdev_p_id and Recr.MAXCrea=Devi.pdev_creation_timestamp
where enc_nombre in ('NTQOES_214681_BR_c_Atlas_weekly_recruitment_V2', 'NTQOES_214681_MX_c_Atlas_weekly_recruitment_V2', 'NTQOES_214681_PE_c_Atlas_weekly_recruitment_V3', 'NTQOES_214681_CO_c_Atlas_weekly_recruitment_V3','NTQOES_214681_AR_c_Atlas_weekly_recruitment_V3','NTQOES_214681_ES_c_Atlas_weekly_recruitment_V3', 'NTQOES_214681_MX_c_Atlas_weekly_recruitment_V3_AB')",sep=""))

query1<- dbFetch(query1, n= -1)
query1<-as.data.table(query1)

query1[,panelists:=paste("'",p_codigo,"'",sep="")]
list <-paste(query1[,panelists],collapse=",")


query2 <-  dbSendQuery(mydb, paste("select p_codigo, p_estado,evt_subtipo_participacion
from panelistas
left join eventos_panelista on p_id = evt_p_id
left join encuestas on evt_enc_id = enc_id
where evt_tipo = 21 AND enc_nombre in ('NTQOES_214681_BR_c_Atlas_weekly_recruitment_V2', 'NTQOES_214681_MX_c_Atlas_weekly_recruitment_V2', 'NTQOES_214681_PE_c_Atlas_weekly_recruitment_V3', 'NTQOES_214681_CO_c_Atlas_weekly_recruitment_V3','NTQOES_214681_AR_c_Atlas_weekly_recruitment_V3','NTQOES_214681_ES_c_Atlas_weekly_recruitment_V3', 'NTQOES_214681_MX_c_Atlas_weekly_recruitment_V3_AB') AND evt_subtipo_participacion in ('CO_0', 'CO_1', 'CO_2', 'CO_3')",sep=""))

query2<- dbFetch(query2, n= -1)
query2<-as.data.table(query2)


query3 <- dbGetQuery(redshift_conn, paste("SELECT subpanels.scopesi_daily.panelist_code,subpanels.scopesi_daily.date
from subpanels.scopesi_daily
where subpanels.scopesi_daily.panelist_code in (",list,")",sep=""))

query3<-as.data.table(query3)

#ubicaciones 
query4 <-  dbSendQuery(mydb, paste("select Pane.p_codigo,Pane.p_pais, CL_Reg.nombre AS CL_REG, BR_Reg.nombre AS BR_REG, ES_Reg.nombre AS ES_REG, AR_Reg.nombre AS AR_REG, MX_Reg.nombre AS MX_REG
from panelistas Pane
left join ubicaciones on p_ub_id=ub_id
left join ubicacion_CL_region CL_Reg on ub_nivel_1=CL_Reg.id
left join ubicacion_BR_estado BR_Reg on ub_nivel_1=BR_Reg.id
left join ubicacion_AR_provincia AR_Reg on ub_nivel_1=AR_Reg.id
left join ubicacion_ES_provincia ES_Reg on ub_nivel_1=ES_Reg.id
left join ubicacion_MX_estado MX_Reg on ub_nivel_1=MX_Reg.id
where p_codigo in (",list,")",sep=""))

query4<- dbFetch(query4, n= -1)
query4<-as.data.table(query4)


query7 = dbGetQuery(redshift_conn, paste("SELECT panelist_code,p.status
        FROM panelist_dm pd
        LEFT JOIN source.panelist p ON panelist_code = p.public_id
  WHERE  panelist_code in (",list,")",sep=""))

query7<-as.data.table(query7)

query8 <- dbGetQuery(redshift_conn, paste("select panelist_dm.panelist_code AS p_codigo,panelist_dm.panelist_state, behav_activity_ft.device_key, behav_activity_ft.activity_date_key, behav_activity_ft.activity_time_key, behav_activity_ft.activity_type, device_dm.device_type, device_dm.device_installation_status, device_dm.device_activity_status, device_dm.device_first_activity_at_date_key
from panelist_dm
left join behav_activity_ft on panelist_dm.panelist_key = behav_activity_ft.panelist_key
left join device_dm on behav_activity_ft.device_key = device_dm.device_key
where activity_date_key > 20220301 AND activity_type in ('all') AND  panelist_code in (",list,")",sep=""))

query8<-as.data.table(query8)

query9 <-  dbSendQuery(mydb, paste("select id, panelist_code, state, date
from geoloc_state
where panelist_code in (",list,")",sep=""))

query9<- dbFetch(query9, n= -1)
query9<-as.data.table(query9)


#query10 <- dbGetQuery(redshift_conn, paste("SELECT mail_tags_panelist as panelist_code, mail_timestamp, open_timestamp, mail_tags_project 
#                                            FROM mailing.ses_datalake
#                                            WHERE event_type = 'Open' and mail_tags_project in ('29b3a6f7f76c0ad5', '600598a66541c937', '8272a7146779f5b0', '29b3a6f7f76c0ad5', 'a1d0c322207b02f0', '28f71640632367df', '369833d4566fbb7c')",sep=""))
#
#query10<-as.data.table(query10)

#query11 <- dbGetQuery(redshift_conn, paste("SELECT mail_tags_panelist as panelist_code, mail_timestamp, click_link, click_link_tags, mail_tags_project 
#                                            FROM mailing.ses_datalake
#                                            WHERE event_type = 'Click' and mail_tags_project in ('29b3a6f7f76c0ad5', '600598a66541c937', '8272a7146779f5b0', '29b3a6f7f76c0ad5', 'a1d0c322207b02f0', '28f71640632367df', '369833d4566fbb7c')",sep=""))
#
#query11<-as.data.table(query11)

dbDisconnect(mydb)
dbDisconnect(redshift_conn)

Meter_data_aggr <- as.data.table(query8)
Meter_data_aggr <- as.data.table(Meter_data_aggr[device_type == "smartphone",])
devices <- Meter_data_aggr[, .(device_key,activity_date_key)]
devices <- devices[, .N, by = device_key]
Meter_data_aggr <- merge(Meter_data_aggr,devices,by=c("device_key"),all.x = TRUE)

Meter_data_aggr_2 <- Meter_data_aggr[, .(p_codigo,device_key,N)]
uniques <- unique(Meter_data_aggr_2)

count <- as.data.table(uniques)
count <- setorder(count,p_codigo,-N)
count <- count[!duplicated(p_codigo)]


activity_matrix <- table(query3)

write.csv(activity_matrix, "data_atlas.csv")
activity_matrix <- as.data.table(read.csv("data_atlas.csv", sep =","))

#añdir variable Days con suma de días compartidos
activity_matrix <- lapply(activity_matrix, function(x) {
  if(is.character(x)|is.factor(x)) as.character(as.character(x)) else as.numeric(x)
})
sapply(activity_matrix, class)
activity_matrix <- as.data.table(activity_matrix)
activity_matrix

activity_matrix$Days <- rowSums(activity_matrix[,2:ncol(activity_matrix)])


base <- as.data.table(query1)
#añadir grupo mensual a base

#Añadir variables de app instalado antes de invite y login para respectiva invite Atlas
base[pdev_creation_timestamp < 	"2022-03-02 00:00:01" & p_pais %in% c("MX", "BR") & enc_nombre != "NTQOES_214681_MX_c_Atlas_weekly_recruitment_V3_AB", installed_before := 1]
base[pdev_creation_timestamp >= 	"2022-03-02 00:00:01" & p_pais %in% c("MX", "BR") & enc_nombre != "NTQOES_214681_MX_c_Atlas_weekly_recruitment_V3_AB" |is.na(pdev_creation_timestamp) , installed_before := 0]

base[pdev_last_login_timestamp >= 	"2022-03-02 00:00:01" & p_pais %in% c("MX", "BR") & enc_nombre != "NTQOES_214681_MX_c_Atlas_weekly_recruitment_V3_AB", logged_in_after := 1]
base[pdev_last_login_timestamp < 	"2022-03-02 00:00:01" & p_pais %in% c("MX", "BR") & enc_nombre != "NTQOES_214681_MX_c_Atlas_weekly_recruitment_V3_AB"|is.na(pdev_last_login_timestamp), logged_in_after := 0]

#Para fecha diferente en test AB
base[pdev_creation_timestamp < 	"2022-05-16 00:00:01" &  enc_nombre == "NTQOES_214681_MX_c_Atlas_weekly_recruitment_V3_AB", installed_before := 1]
base[pdev_creation_timestamp >= 	"2022-05-16 00:00:01" &  enc_nombre == "NTQOES_214681_MX_c_Atlas_weekly_recruitment_V3_AB" |is.na(pdev_creation_timestamp) , installed_before := 0]

base[pdev_last_login_timestamp >= 	"2022-05-16 00:00:01" &  enc_nombre == "NTQOES_214681_MX_c_Atlas_weekly_recruitment_V3_AB", logged_in_after := 1]
base[pdev_last_login_timestamp < 	"2022-05-16 00:00:01" &  enc_nombre == "NTQOES_214681_MX_c_Atlas_weekly_recruitment_V3_AB"|is.na(pdev_last_login_timestamp), logged_in_after := 0]


base[pdev_creation_timestamp < 	"2022-03-27 00:00:01" & p_pais %in% c("AR", "PE", "CO", "ES"), installed_before := 1]
base[pdev_creation_timestamp >= 	"2022-03-27 00:00:01" & p_pais %in% c("AR", "PE", "CO", "ES") |is.na(pdev_creation_timestamp) , installed_before := 0]

base[pdev_last_login_timestamp >= 	"2022-03-27 00:00:01" & p_pais %in% c("AR", "PE", "CO", "ES"), logged_in_after := 1]
base[pdev_last_login_timestamp < 	"2022-03-27 00:00:01" & p_pais %in% c("AR", "PE", "CO", "ES")|is.na(pdev_last_login_timestamp), logged_in_after := 0]


base <- as.data.table(base)


#Variable "activated" basada en si han recibido CO de activacion en reclutamiento
query2[, activated:= 1]
query2 <- as.data.table(query2[, .(p_codigo, activated)])

query9 <- query9[, .(panelist_code, state)]
colnames(query9) <- c("p_codigo", "geoloc_state")

#añadiendo open y click rates weekly (faltaría monthly)
base_full <- as.data.table(merge(base, query2,by = "p_codigo" ,all.x = TRUE))
#base_full <- as.data.table(merge(base_full, open,by = "p_codigo" ,all.x = TRUE))
#base_full <- as.data.table(merge(base_full, click,by = "p_codigo" ,all.x = TRUE))
base_full <- as.data.table(merge(base_full, query9,by = "p_codigo" ,all.x = TRUE))

#estado actual de panelista y variable frozen
base_full[p_estado == 81, frozen:= "1"]
base_full[is.na(frozen), frozen:= "0"]
base_full <- as.data.table(base_full)

base_full[evt_fecha_creacion < 	"2022-03-02 00:03:15", Launch := "soft"]
base_full[evt_fecha_creacion >= 	"2022-03-02 00:03:15", Launch := "full"]
base_full$panelists <- NULL

base_full <- as.data.table(merge(base_full, count,by = "p_codigo" ,all.x = TRUE))
colnames(query7) <- c("p_codigo", "meter_status")
base_full <- as.data.table(merge(base_full, query7,by = "p_codigo" ,all.x = TRUE))



#añadir ubicaciones, creando variable única "region"
ubicaciones <- query4
ubicaciones[p_pais != 'CL', CL_REG := 'NA']
ubicaciones[p_pais != 'AR', AR_REG := 'NA']
ubicaciones[p_pais != 'BR', BR_REG := 'NA']
ubicaciones[p_pais != 'MX', MX_REG := 'NA']
ubicaciones[p_pais != 'ES', ES_REG := 'NA']

ubicaciones[p_pais == 'CL', region := CL_REG]
ubicaciones[p_pais == 'AR', region := AR_REG]
ubicaciones[p_pais == 'BR', region := BR_REG]
ubicaciones[p_pais == 'MX', region := MX_REG]
ubicaciones[p_pais == 'ES', region := ES_REG]

ubicaciones <- as.data.table(ubicaciones[, .(p_codigo, region)])
base_full <- as.data.table(merge(base_full, ubicaciones,by = "p_codigo" ,all.x = TRUE))

base_full[is.na(activated), activated:=0]
#base_full[is.na(open), open:=0]
#base_full[is.na(access), access:=0]

base_full$evt_fecha_creacion <- as.Date(base_full$evt_fecha_creacion)
base_full$p_fecha_registro <- as.Date(base_full$p_fecha_registro)
base_full$p_fecha_nacimiento <- as.Date(base_full$p_fecha_nacimiento)

#age
base_full[,age:=as.numeric(floor(difftime(Sys.Date(),base_full$p_fecha_nacimiento)/365.25))]


base_full[age %in% c(16:24), frozen := "16_24"]
base_full[age %in% c(25:34), frozen := "25_34"]
base_full[age %in% c(35:44), frozen := "35_44"]
base_full[age %in% c(45:54), frozen := "45_54"]
base_full[age %in% c(55:64), frozen := "55_64"]
base_full[age > 64, frozen := "65+"]

setnames(base_full, "frozen", "age_range")
base_full <- as.data.table(base_full)


#meter class 1 = perfect smartphone user, 2 = less than max
#base_full[N >= NumberOfDays -1 ,meter_class:= 1]
#base_full[N < NumberOfDays -1 & !is.na(N) ,meter_class:= 2]
#base_full <- as.data.table(base_full)

#month in panel since invite
base_full[, number_of_months := (year(evt_fecha_creacion) - year(base_full$p_fecha_registro)) * 12 + month(evt_fecha_creacion) - month(base_full$p_fecha_registro)]
base_full <- as.data.table(base_full)

#seniority
base_full[number_of_months < 4, seniority := 1]
base_full[number_of_months >= 4 & number_of_months < 12 , seniority := 2]
base_full[number_of_months >= 12 & number_of_months < 36 , seniority := 3]
base_full[number_of_months >= 36 & number_of_months < 72 , seniority := 4]
base_full[number_of_months >= 72 , seniority := 5]
base_full <- as.data.table(base_full)

#Android version
base_full[pdev_os == 31, Android_vers := "12"]
base_full[pdev_os == 30, Android_vers := "11"]
base_full[pdev_os == 29, Android_vers := "10"]
base_full[pdev_os == 28, Android_vers := "9"]
base_full[pdev_os < 28, Android_vers := "Other"]
base_full <- as.data.table(base_full)

base_full$evt_fecha_creacion <- NULL

colnames(activity_matrix)[colnames(activity_matrix) == 'X'] <- 'p_codigo'

clicks <- as.data.table(read.csv("/rstudio-data/shared/PanelManagement/Atlas/Atlas_shurn/click_atlas_sdk.csv"))
opens <- as.data.table(read.csv("/rstudio-data/shared/PanelManagement/Atlas/Atlas_shurn/open_atlas_sdk.csv"))
clicks <- clicks[, clicked := 1]
opens <- opens[, opened := 1]
clicks <- as.data.table(clicks)
opens <- as.data.table(opens)

base_full <- as.data.table(merge(base_full, opens,by = "p_codigo" ,all.x = TRUE))
base_full <- as.data.table(merge(base_full, clicks,by = "p_codigo" ,all.x = TRUE))
base_full[enc_nombre == "NTQOES_214681_MX_c_Atlas_weekly_recruitment_V3_AB", Additional_test := 1]
base_full[enc_nombre != "NTQOES_214681_MX_c_Atlas_weekly_recruitment_V3_AB", Additional_test := 0]

base_full <- as.data.table(merge(base_full, activity_matrix,by = "p_codigo" ,all.x = TRUE))


gs4_auth(
  email = "panel.management@netquest.com",
  path = NULL,
  scopes = "https://www.googleapis.com/auth/spreadsheets",
  cache = TRUE,
  use_oob = gargle::gargle_oob_default(),
  token = NULL
)

#Buscar la pestaña de codigos usados y guardarla como DT "used_codes"

sheet_write(ss ='https://docs.google.com/spreadsheets/d/1DWiUvBWsWAU91qoBa7Iu90_f0w2Vy_y6xkzssr7NVc0/edit#gid=0', data = base_full, sheet = "DATA")


#fwrite(base_full, "Atlas_complete_results_new_sdk.csv",sep = ";", row.names = FALSE, col.names = TRUE, na = "NA")
#fwrite(query11, "click.csv",sep = ";", row.names = FALSE, col.names = TRUE, na = "NA")
#fwrite(query10, "open.csv",sep = ";", row.names = FALSE, col.names = TRUE, na = "NA")

