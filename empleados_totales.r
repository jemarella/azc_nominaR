
carga_resumen_nom <- function (ianio,iquincena,itipo,iarchivo1,iarchivo2)
{
   file_conn = abrir_log()
   escribir_log(file_conn,"Inicio Carga Percepciones y Deducciones....")
   
   tryCatch({
   
      checkini <- list()
      iniFile <- "./cfg_creacheq.ini"
      checkini <- read.ini(iniFile)
    
      # Conectar a la base de datos
      if (length(checkini) > 0) {
         con <- dbConnect(RPostgres::Postgres(),
                       dbname = checkini$Database$dbname,
                       host = checkini$Database$host,
                       port = checkini$Database$port,
                       user = checkini$Database$user,
                       password = checkini$Database$password)
      }	else {
         con <- dbConnect(RPostgres::Postgres(),
                      dbname = "SistemaNomina",
                      host = "192.168.100.215",
                      port = 5432,
                      user = "postgres",
                      password = "Pjmx3840")
      }

# datos prueba ianio = 2024
#iquincena = '05'
#itipo = 'Compuesta'
#iarchivo1 = '52'
#iarchivo2 = '52-azcapotzalco'


      # Leer y preparar los datos del archivo 1

	  ## ambiente desarrollo fidel file1 <- "C:/Users/PJMX/Desktop/Sistema de nomina Alcaldía/archivos originales/archivos originales/nomina base, estructura y n8/52-azcapotzalco/52.xlsx"
      ## ambiente desarrollo filde data1 <- read_excel(file1) %>%
         
	  # Obtener KEY_ctr de la tabla nomina_ctrl 
      key_quincena_query <- 
		sprintf (checkini$Queries$ctrl_nom_pendiente,ianio,iquincena,itipo)

	  escribir_log (file_conn,key_quincena_query)

      key_quincena_data <- dbGetQuery(con, key_quincena_query)
	 
	  ictrl_idx = 0
	  if (nrow (key_quincena_data) > 0) {
	     ictrl_idx = key_quincena_data$idx[1]
 	  } else {
	     stop ("No hay registro en nomina control")
	  }		
				 	 
		 
      root_dir <- checkini$Directory$droot
      file1 <- paste0(root_dir, "nomina ", itipo, "/Respaldos/", ianio, "/", iquincena, "_", iarchivo1, ".xlsx")

      # Verificar si el archivo existe
      if (!file.exists(file1)) {
         stop(paste("Error: El archivo no existe en la ruta especificada:", file1))
      }

      data1 <- read_excel(file1) %>%
	 
      distinct() %>%
      rename_all(tolower) %>%
      select(id_empleado, percepciones, deducciones, liquido)

      # Leer y preparar los datos del archivo 2
      ## ambiente desarrollo fidel file2 <- "C:/Users/PJMX/Desktop/Sistema de nomina Alcaldía/archivos originales/archivos originales/nomina base, estructura y n8/52-azcapotzalco/52-azcapotzalco.xlsx"
      ## ambiente desarrollo fidel data2 <- read_excel(file2) %>%
	  
      file2 <- paste0(root_dir, "nomina ", itipo, "/Respaldos/", ianio, "/", iquincena, "_", iarchivo2, ".xlsx")

      # Verificar si el archivo existe
      if (!file.exists(file2)) {
         stop(paste("Error: El archivo no existe en la ruta especificada:", file2))
      }
      data2 <- read_excel(file2) %>%
	  
      distinct() %>%
      rename_all(tolower) %>%
      mutate(
      fec_pago = dmy(fec_pago),  # Convertir FEC_PAGO a tipo Date
      # ambiente Fidel quincena = case_when(
      # ambiente Fidel    day(fec_pago) <= 15 ~ 1,  # Primera quincena
      # ambiente Fidel   day(fec_pago) > 15 ~ 2    # Segunda quincena
      # ambiente Fidel                      ),
      # ambiente Fidel anio = year(fec_pago)      # Año de la fecha
	  quincena = iquincena,
	  anio = ianio,
	  ctrl_idx = ictrl_idx
            ) %>%
      select(id_empleado, fec_pago, id_programa, id_prog_especial, id_activ_inst, id_grado, id_sector, quincena, anio,ctrl_idx)

      # Verificar nombres de columnas en data1 y data2
      escribir_log(file_conn,"Columnas en data1:")
      escribir_log(file_conn,(names(data1)))

      escribir_log(file_conn,"Columnas en data2:")
      escribir_log(file_conn,names(data2))

      # Realizar el left_join
      data_combined <- data1 %>%
      left_join(data2, by = "id_empleado") %>%
      group_by(id_empleado) %>%
      slice(1) %>%
      ungroup() %>%
      mutate(
         key_quincena = row_number()  # Generar una secuencia de números para KEY_QUINCENA
      )

      # Verificar nombres de columnas en data_combined
      escribir_log(file_conn,"Columnas en data_combined:")
      escribir_log(file_conn,names(data_combined))

      # Mostrar algunas filas de data_combined
      escribir_log(file_conn,"Primeras filas de data_combined:")
      escribir_log(file_conn,head(data_combined))

      # Preparar los datos para la inserción
      # cambio 13_ago_2024  quitar key_quincena Fidel data_to_insert <- data_combined %>%
      #   select(key_quincena, quincena, anio, id_empleado, percepciones, deducciones, liquido, fec_pago, id_programa, id_prog_especial, id_activ_inst, id_grado, id_sector,ctrl_idx)

      data_to_insert <- data_combined %>%
         select(key_quincena,quincena, anio, id_empleado, percepciones, deducciones, liquido, fec_pago, id_programa, id_prog_especial, id_activ_inst, id_grado, id_sector,ctrl_idx)
      
      
      # Verificar los primeros registros de data_to_insert
      escribir_log(file_conn,"Primeras filas de data_to_insert:")
      escribir_log(file_conn,head(data_to_insert))

      # Insertar los datos en la base de datos PostgreSQL
      dbWriteTable(con, "empleados_totales", data_to_insert, append = TRUE, row.names = FALSE)
	  
      # Cerrar la conexión a la base de datos
      dbDisconnect(con)
	   cerrar_log (file_conn) 

  }, error = function(e) {
    mensaje_error <- paste(Sys.time(), ": ", e$message, sep = "")
	escribir_log (file_conn,mensaje_error)	
    cerrar_log (file_conn)
  })
  
}
