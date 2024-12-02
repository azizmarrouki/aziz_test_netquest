rm(list = ls())
options(java.parameters = "-Xmx8000m")


library(mailR)
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
library(stringi)
library(ggplot2)
library(purrr)
library(scales)
library(markdown)
library(knitr)
library(kableExtra)
library(reshape2)
library(data.table)
library(aws.s3)
library(mailR)


#Ajustar a credenciales propcias
setwd("/rstudio-data/users/mmarrouki")
credentials <- fread("R_keys.csv")

print("start query")
Sys.time()

redshift_user <- credentials[dbname == "ntq", user]
redshift_pwd <- credentials[dbname == "ntq", password]
redshift_driver_url <- credentials[dbname == "ntq", driver]

redshift_driver <- JDBC("com.amazon.redshift.jdbc42.Driver", redshift_driver_url, identifier.quote="`")
redshift_url <- paste0("jdbc:redshift://redshift.bi-pro.netquestapps.com:5439/ntq?user=",redshift_user,"&password=",redshift_pwd)
redshift_conn <- dbConnect(redshift_driver, redshift_url)


query1b <- dbGetQuery(redshift_conn, paste("
SELECT DISTINCT comp_source_var_name, vd_nombre, pa_nombre, cam_nombre
FROM campanyas c
LEFT JOIN connectors c2 ON c2.co_id = c.cam_primera_encuesta_co_id 
LEFT JOIN connector_mappings cm ON cm.comp_co_id = c2.co_id 
LEFT JOIN variables_definidas vd ON vd.vd_id = cm.comp_destination_vd_id
LEFT JOIN panelistas p ON c.cam_id = p.p_cam_id  
LEFT JOIN panelistas_en_paneles pep ON p_id = pep.ppa_p_id
LEFT JOIN paneles p2 ON pep.ppa_pa_id = p2.pa_id
WHERE p_estado = 70 AND p_pais = 'ES' 
                                           AND p.p_fecha_registro >= DATEADD(MONTH, -1, GETDATE()) 
                                           AND c.cam_primera_encuesta_sm2_desk_title like 'FIRST_ACTIVITY%'", sep=""))



query1b <- as.data.table(query1b)






#TEST: 
#query1b[3, `:=`(vd_nombre = "kids2", pa_nombre = "panel_MX")]

# Print the updated data table to verify the change
print(query1b)

# Step 1: Count occurrences of each variable
query1c <- query1b[, .N, by = .(comp_source_var_name, vd_nombre)]

# Order by comp_source_var_name
setorder(query1c, comp_source_var_name)

# Step 2: Calculate expected count using a reference variable
reference_variable <- query1b[pa_nombre == 'panel_esp', unique(comp_source_var_name)][1]
expected_count <- query1b[pa_nombre == 'panel_esp' & comp_source_var_name == reference_variable, .N]

# Step 3: Count occurrences of each comp_source_var_name
count_data <- query1b[, .(N = .N), by = comp_source_var_name]

# Step 4: Identify variables with different counts
wrongly_connected_variables <- count_data[N != expected_count]

# Step 5: Include the corresponding campaign name
wrongly_connected_variables <- merge(wrongly_connected_variables, 
                                     query1b[, .(comp_source_var_name, cam_nombre)], 
                                     by = 'comp_source_var_name', 
                                     all.x = TRUE)


# Display the dataframe with wrongly connected variables
print(wrongly_connected_variables)



# Assuming query1b is already loaded

# List of all unique campaign names
all_campaigns <- unique(query1b$cam_nombre)

# Initialize an empty list to collect results
missing_campaigns_list <- list()

# Loop through each unique variable and find missing campaigns
for (variable_name in unique(query1b$comp_source_var_name)) {
  variable_campaigns <- unique(query1b[comp_source_var_name == variable_name, cam_nombre])
  missing_campaigns <- setdiff(all_campaigns, variable_campaigns)
  
  # Append the results to the list
  missing_campaigns_list[[variable_name]] <- data.table(
    Variable_Name = variable_name,
    Campaign_with_UnconnectedVariables = paste(missing_campaigns, collapse = ", ")
  )
}

# Combine the results into a single data.table
missing_campaigns_df <- rbindlist(missing_campaigns_list)

# Remove empty campaign entries and convert empty strings to NA
missing_campaigns_df[Campaign_with_UnconnectedVariables == "", Campaign_with_UnconnectedVariables := NA]
missing_campaigns_df <- na.omit(missing_campaigns_df)

# Print the dataframe if there are any results
if (nrow(missing_campaigns_df) > 0) {
  print(missing_campaigns_df)
}




# Assuming query1b is already loaded

# Create an empty data.table to store the results
result_df <- data.table()

# Get unique comp_source_var_names
unique_variable_names <- unique(query1b$comp_source_var_name)
print(unique_variable_names)

# Loop through unique comp_source_var_names
for (variable_name in unique_variable_names) {
  var_rows <- query1b[comp_source_var_name == variable_name]
  
  # Count occurrences of vd_nombre
  unique_vd_counts <- table(var_rows$vd_nombre)
  majority_vd <- names(unique_vd_counts)[which.max(unique_vd_counts)]
  
  # Filter rows with different vd_nombre
  different_rows <- var_rows[vd_nombre != majority_vd]
  
  # Append the different rows to the result data.table if there are any
  if (nrow(different_rows) > 0) {
    result_df <- rbind(result_df, different_rows)
  }
}

# Print the resulting data.table
print(result_df)



# Filter rows with different connections and select distinct values
if (nrow(result_df) > 0) {
  result_df_filtered <- unique(result_df[, .(SM2_Variable = comp_source_var_name, 
                                             wrongly_connected_PPI_variable = vd_nombre, 
                                             Campaign_change_connection = cam_nombre)])
} else {
  result_df_filtered <- data.table(SM2_Variable = character(0), 
                                   wrongly_connected_PPI_variable = character(0), 
                                   Campaign_change_connection = character(0))
}

# Create the first dataframe
output_df1 <- result_df_filtered

# Create the second dataframe
output_df2 <- if (nrow(result_df) > 0) {
  result_df[, .(Change_panel_campaign = first(cam_nombre)), by = pa_nombre]
} else {
  data.table(pa_nombre = character(0), Change_panel_campaign = character(0))
}

output_df2 <- output_df2[, .(Change_panel_campaign, 
                             `current panel` = pa_nombre, 
                             correct_panel = "panel_esp")]

# Print the first dataframe
print(output_df1)

# Print the second dataframe
print(output_df2)

# Timing and saving files
Sys.time()
print("save files")

# ... (Assuming previous code remains unchanged)

query1c <- as.data.table(query1c)

times_conected <- query1c[, .N, by = comp_source_var_name]



# Save files with corrected filenames
fwrite(missing_campaigns_df, paste0("/rstudio-data/users/mmarrouki/shared/PanelManagement/DAM/DAM_ALARM/FIRST_ALARM_EMAIL/Files_2023/missing_campaigns_df_.csv"), sep = ",", row.names = FALSE, col.names = TRUE)

tmp <- tempfile()
on.exit(unlink(tmp))
fwrite(missing_campaigns_df, file = tmp, sep = ",", row.names = FALSE, col.names = TRUE)
put_object(
  file = tmp,
  object = paste0("variable_missing_campaign_df_",format(Sys.Date(),"%Y_%m_%d"),".csv"),
  bucket = "ntq.projects/PanelManagement_Reports/DAM/DAM_ALARM/Files", 
  acl = "public-read",
  headers = c(
    'Content-Disposition' = 'attachment',
    'Content-Type' = 'text/tab-separated-values'
  ),
  key = credentials[dbname == "s3", user],
  secret = credentials[dbname == "s3", password],
  region = "us-east-1"
)

tmp_3 <- tempfile()
on.exit(unlink(tmp_3))
fwrite(output_df1, file = tmp_3, sep = ",", row.names = FALSE, col.names = TRUE)
put_object(
  file = tmp_3,
  object = paste0("output_df1_", format(Sys.Date(),"%Y_%m_%d"), ".csv"),
  bucket = "ntq.projects/PanelManagement_Reports/DAM/DAM_ALARM/Files", 
  acl = "public-read",
  headers = c(
    'Content-Disposition' = 'attachment',
    'Content-Type' = 'text/tab-separated-values'
  ),
  key = credentials[dbname == "s3", user],
  secret = credentials[dbname == "s3", password],
  region = "us-east-1"
)

tmp_2 <- tempfile()
on.exit(unlink(tmp_2))
fwrite(output_df2, file = tmp_2, sep = ",", row.names = FALSE, col.names = TRUE)
put_object(
  file = tmp_2,
  object = paste0("output_df2_", format(Sys.Date(),"%Y_%m_%d"), ".csv"),
  bucket = "ntq.projects/PanelManagement_Reports/DAM/DAM_ALARM/Files", 
  acl = "public-read",
  headers = c(
    'Content-Disposition' = 'attachment',
    'Content-Type' = 'text/tab-separated-values'
  ),
  key = credentials[dbname == "s3", user],
  secret = credentials[dbname == "s3", password],
  region = "us-east-1"
)



# ... (Rest of the code remains unchanged)

Sys.time()
print("send email")


print(missing_campaigns_df)
print(output_df1)
print(output_df2)

source("/rstudio-data/users/mmarrouki/shared/PanelManagement/DAM/DAM_ALARM/FIRST_ALARM_EMAIL/FIRST_ALARM_EMAIL/First_Alarm_Email.R")




dbDisconnect(redshift_conn)


