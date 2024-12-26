CREATE TABLE IF NOT EXISTS `warm-aegis-419014.Case_Study_Cyclistic.Rides_2019`
AS
SELECT 
  DISTINCT *, #Eliminacion de datos repetidos
  # Calculo de la duracion de cada uno de los viajes
  IF( 
    TIMESTAMP_DIFF(end_time, start_time, DAY) > 0, #Condicion por si la duracion es mayor a un dia
    CONCAT(
      TIMESTAMP_DIFF(end_time, start_time, DAY),
      ' días, ',
      MOD(TIMESTAMP_DIFF(end_time, start_time, HOUR), 24),
      ' h, ',
      MOD(TIMESTAMP_DIFF(end_time, start_time, MINUTE), 60),
      ' min,',
      MOD(TIMESTAMP_DIFF(end_time, start_time, SECOND), 60),
      ' seg'
    ), #Caso contrario, que pasa cuando es menor a un día
    CONCAT(
      MOD(TIMESTAMP_DIFF(end_time, start_time, HOUR), 24),
      ' h, ',
      MOD(TIMESTAMP_DIFF(end_time, start_time, MINUTE), 60),
      ' min,',
      MOD(TIMESTAMP_DIFF(end_time, start_time, SECOND), 60),
      ' seg'
    )
  ) AS ride_length2,
  (
    TIMESTAMP_DIFF(end_time, start_time, DAY) * 24 * 60 * 60 +
    MOD(TIMESTAMP_DIFF(end_time, start_time, HOUR), 24) * 60 * 60 +
    MOD(TIMESTAMP_DIFF(end_time, start_time, MINUTE), 60) * 60 +
    MOD(TIMESTAMP_DIFF(end_time, start_time, SECOND), 60)
  ) AS ride_length_in_sec,
  CAST(start_time AS STRING FORMAT 'DAY') AS day_of_week, #Determinacion del dia de la semana que inicia cada viaje
  REPLACE (usertype,'Subscriber','member') AS member_casual 
FROM 
  `warm-aegis-419014.Case_Study_Cyclistic.Divvy_Trips_2019` 
WHERE
  #Filtrado: eliminacion de datos faltantes, repeticion de estacion inicial y final y viajes de una duracion menor a 1 minuto
  gender IS NOT NULL 
  AND birthyear IS NOT NULL 
  AND from_station_id != to_station_id 
  AND NOT(EXTRACT(HOUR FROM end_time-start_time)=0 
  AND EXTRACT(MINUTE FROM end_time-start_time)=0 
  AND EXTRACT(SECOND FROM end_time-start_time)<60)

ORDER BY
  ride_length2 ASC;

#Creacion de una tabla donde se agrupan la cantidad de dias de la semana se repiten 
CREATE TEMP TABLE Rides_member_2019
AS
SELECT
  day_of_week,
  COUNT(day_of_week) AS total_days, #cuanta la cantidad de dias de la semana
  from_station_name,
  COUNT(from_station_name) AS start_station,
  to_station_name,
  COUNT(to_station_name) AS end_station,

  #Calculo de parametros generales de las duraciones de los viajes
  MIN(ride_length_in_sec) AS min_ride,
  MAX(ride_length_in_sec) AS max_ride,
  AVG(ride_length_in_sec) AS mean_ride,
FROM
  `warm-aegis-419014.Case_Study_Cyclistic.Rides_2019`
WHERE
  member_casual = 'member'
GROUP BY
  from_station_name, to_station_name, day_of_week #agrupa la cantidad de veces que se repite cada dia de la semana
ORDER BY
  start_station DESC; 

#WITH Rides_member_2019 AS (
#  SELECT
#    day_of_week,
#    from_station_name,
#    to_station_name,
#    ROW_NUMBER() OVER (PARTITION BY COUNT(*) ORDER BY COUNT(*) DESC) AS total_viajes, #Ruta mas popular
#    ROW_NUMBER() OVER (PARTITION BY day_of_week ORDER BY COUNT(*) DESC) AS ranking_dia, #Dias mas transitados
#    ROW_NUMBER() OVER (PARTITION BY from_station_name ORDER BY COUNT(*) DESC) AS ranking_salida, #Estacion de salida mas popular
#    ROW_NUMBER() OVER (PARTITION BY to_station_name ORDER BY COUNT(*) DESC) AS ranking_llegada #Estacion de llegada mas pooular
#  FROM
#    `warm-aegis-419014.Case_Study_Cyclistic.Rides_2019`
#  WHERE
#    member_casual = 'member'
#  GROUP BY
#    day_of_week, from_station_name, to_station_name
#)
#SELECT *
#FROM Rides_member_2019
#WHERE ranking_dia = 1 OR ranking_salida = 1 OR ranking_llegada = 1;


#Creacion de una tabla donde se agrupan la cantidad de dias de la semana se repiten 
CREATE TEMP TABLE Rides_casual_2019
AS
SELECT
  day_of_week,
  COUNT(day_of_week) AS total_days, #cuanta la cantidad de dias de la semana
  from_station_name,
  COUNT(from_station_name) AS start_station,
  to_station_name,
  COUNT(to_station_name) AS end_station,

  #Calculo de parametros generales de las duraciones de los viajes
  MIN(ride_length_in_sec) AS min_ride,
  MAX(ride_length_in_sec) AS max_ride,
  AVG(ride_length_in_sec) AS mean_ride,
FROM
  `warm-aegis-419014.Case_Study_Cyclistic.Rides_2019`
WHERE
  member_casual = 'Customer'
GROUP BY
  from_station_name, to_station_name, day_of_week #agrupa la cantidad de veces que se repite cada dia de la semana
ORDER BY
  start_station DESC;
