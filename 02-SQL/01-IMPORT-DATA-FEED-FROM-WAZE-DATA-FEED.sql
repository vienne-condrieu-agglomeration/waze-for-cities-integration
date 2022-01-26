START TRANSACTION;
/********************************************************* ALERTS TABLES *********************************************************/
--DROP TABLE
DROP TABLE IF EXISTS waze.tmp_view_alerts_clustered CASCADE;

--CREATE TABLE
CREATE TABLE waze.tmp_view_alerts_clustered (
	gid serial NOT NULL PRIMARY KEY,
	data_json json NOT NULL
);

--COPIE DU JSON
COPY waze.tmp_view_alerts_clustered (data_json)
  FROM '/opt/data/vca/cron/waze/data/waze-view-alerts-clustered.json';

-- CREATE ALERTS ARCHIVE TABLE
INSERT INTO waze.archive_view_alerts_clustered (
	uuid,
	the_geom,
	alert_type,
	alert_subtype,
	roadtype,
	magvar,
	reportrating,
	confidence,
	reliability,
	street,
	city,
	country,
	longitude,
	latitude,
	waze_ts,
	waze_creation_date,
	waze_alert_age,
	date_fr_format,
	import_ts
)
WITH
	view_alerts_clustered AS (
		SELECT	CAST(json_array_elements(data_json -> 'alerts') ->> 'uuid' AS varchar) AS uuid,
			CAST(json_array_elements(data_json -> 'alerts') ->> 'type' AS varchar) AS alert_type,
			CAST(json_array_elements(data_json -> 'alerts') ->> 'subtype' AS varchar) AS alert_subtype,
			CAST(json_array_elements(data_json -> 'alerts') ->> 'roadType' AS integer) AS roadtype,
			CAST(json_array_elements(data_json -> 'alerts') ->> 'magvar' AS integer) AS magvar,			
			CAST(json_array_elements(data_json -> 'alerts') ->> 'reportRating' AS integer) AS reportrating,
			CAST(json_array_elements(data_json -> 'alerts') ->> 'confidence' AS integer) AS confidence,
			CAST(json_array_elements(data_json -> 'alerts') ->> 'reliability' AS integer) AS reliability,
			CAST(json_array_elements(data_json -> 'alerts') ->> 'street' AS varchar) AS street,
			CAST(json_array_elements(data_json -> 'alerts') ->> 'city' AS varchar) AS city,
			CAST(json_array_elements(data_json -> 'alerts') ->> 'country' AS varchar) AS country,
			CAST(json_array_elements(data_json -> 'alerts') -> 'location' ->> 'x' AS float) AS longitude,
			CAST(json_array_elements(data_json -> 'alerts') -> 'location' ->> 'y' AS float) AS latitude,
			CAST(json_array_elements(data_json -> 'alerts') ->> 'pubMillis' AS int8)/1000 AS waze_ts,
			TO_TIMESTAMP(CAST(json_array_elements(data_json -> 'alerts') ->> 'pubMillis' AS bigint)/1000)::timestamp AS waze_creation_date,
			AGE(now(), to_timestamp(CAST(json_array_elements(data_json -> 'alerts') ->> 'pubMillis' AS bigint)/1000)::timestamp) AS waze_alert_age,
			TO_CHAR(TO_TIMESTAMP(CAST(json_array_elements(data_json -> 'alerts') ->> 'pubMillis' AS bigint)/1000)::timestamp, 'Le dd/mm/yyyy à HH24:MI:SS') AS date_fr_format,			
			json_array_elements(data_json -> 'alerts') AS json_src
		  FROM waze.tmp_view_alerts_clustered
	)
SELECT	view_alerts_clustered.uuid,
	ST_SetSRID(ST_MakePoint(view_alerts_clustered.longitude,view_alerts_clustered.latitude),4326)::geometry(POINT,4326) AS the_geom,
	view_alerts_clustered.alert_type,
	view_alerts_clustered.alert_subtype,
	view_alerts_clustered.roadtype,
	view_alerts_clustered.magvar,	
	view_alerts_clustered.reportrating,
	view_alerts_clustered.confidence,
	view_alerts_clustered.reliability,
	view_alerts_clustered.street,
	view_alerts_clustered.city,
	view_alerts_clustered.country,
	view_alerts_clustered.longitude,
	view_alerts_clustered.latitude,
	view_alerts_clustered.waze_ts,
	view_alerts_clustered.waze_creation_date,
	view_alerts_clustered.waze_alert_age,
	view_alerts_clustered.date_fr_format,
	now()::timestamp AS import_ts
  FROM view_alerts_clustered
  ON CONFLICT (uuid) DO
	UPDATE SET the_geom = archive_view_alerts_clustered.the_geom,
		alert_type 		= archive_view_alerts_clustered.alert_type,
		alert_subtype 	= archive_view_alerts_clustered.alert_subtype,
		roadtype 		= archive_view_alerts_clustered.roadtype,
		magvar 			= archive_view_alerts_clustered.magvar,
		reportrating 	= archive_view_alerts_clustered.reportrating,
		confidence 		= archive_view_alerts_clustered.confidence,
		reliability 	= archive_view_alerts_clustered.reliability,
		street 			= archive_view_alerts_clustered.street,
		city 			= archive_view_alerts_clustered.city,
		country 		= archive_view_alerts_clustered.country,
		longitude 		= archive_view_alerts_clustered.longitude,
		latitude 		= archive_view_alerts_clustered.latitude,
		waze_ts 		= archive_view_alerts_clustered.waze_ts,
		waze_creation_date = archive_view_alerts_clustered.waze_creation_date,
		waze_alert_age 	= archive_view_alerts_clustered.waze_alert_age,
		date_fr_format 	= archive_view_alerts_clustered.date_fr_format;
/******************************************************* END ALERTS TABLES *******************************************************/


/********************************************************* JAMS TABLES ***********************************************************/
--DROP TABLE
DROP TABLE IF EXISTS waze.tmp_view_jams_clustered CASCADE;

--CREATE TABLE
CREATE TABLE waze.tmp_view_jams_clustered (
	gid serial NOT NULL PRIMARY KEY,
	data_json json NOT NULL
);

--COPIE DU JSON
COPY waze.tmp_view_jams_clustered (data_json)
  FROM '/opt/data/vca/cron/waze/data/waze-view-jams-clustered.json';
  
DROP TABLE IF EXISTS waze.view_jams_clustered;
CREATE TABLE waze.view_jams_clustered AS
WITH
	view_jams_clustered_element AS (
		SELECT	CAST(json_array_elements(data_json -> 'jams') ->> 'uuid' AS varchar) AS uuid,
				json_array_elements(data_json -> 'jams') -> 'line' AS line,
				CAST(json_array_elements(data_json -> 'jams') ->> 'type' AS varchar) AS jam_type,
				CAST(json_array_elements(data_json -> 'jams') ->> 'turnType' AS varchar) AS jam_turntype,
				CAST(json_array_elements(data_json -> 'jams') ->> 'level' AS integer) AS jam_level,
				CAST(json_array_elements(data_json -> 'jams') ->> 'roadType' AS integer) AS roadtype,	
				CAST(json_array_elements(data_json -> 'jams') ->> 'speedKMH' AS float) AS speed_kmh,
				CAST(json_array_elements(data_json -> 'jams') ->> 'length' AS float) AS length_m,	
				CAST(json_array_elements(data_json -> 'jams') ->> 'startNode' AS varchar) AS start_node,
				CAST(json_array_elements(data_json -> 'jams') ->> 'endNode' AS varchar) AS end_node,
				CAST(json_array_elements(data_json -> 'jams') ->> 'speed' AS float) AS speed,	
				CAST(json_array_elements(data_json -> 'jams') ->> 'delay' AS integer) AS delay,
				CAST(json_array_elements(data_json -> 'jams') ->> 'street' AS varchar) AS street,
				CAST(json_array_elements(data_json -> 'jams') ->> 'city' AS varchar) AS city,
				CAST(json_array_elements(data_json -> 'jams') ->> 'country' AS varchar) AS country,			
				CAST(json_array_elements(data_json -> 'jams') ->> 'segments' AS varchar) AS segments,
				CAST(json_array_elements(data_json -> 'jams') ->> 'pubMillis' AS int8)/1000 AS waze_ts,
				TO_TIMESTAMP(CAST(json_array_elements(data_json -> 'jams') ->> 'pubMillis' AS bigint)/1000)::timestamp AS waze_creation_date,
				TO_CHAR(TO_TIMESTAMP(CAST(json_array_elements(data_json -> 'jams') ->> 'pubMillis' AS bigint)/1000)::timestamp, 'Le dd/mm/yyyy à HH24:MI:SS') AS date_fr_format,
				AGE(now(), to_timestamp(CAST(json_array_elements(data_json -> 'jams') ->> 'pubMillis' AS bigint)/1000)::timestamp) AS waze_alert_age
		  FROM waze.tmp_view_jams_clustered
		  ORDER BY uuid
	),
	view_jams_clustered_points AS (
		SELECT 	view_jams_clustered_element.uuid,
			json_array_elements(view_jams_clustered_element.line) AS line,
			CAST(json_array_elements(view_jams_clustered_element.line) ->> 'x' AS numeric) AS longitude,
			CAST(json_array_elements(view_jams_clustered_element.line) ->> 'y' AS numeric) AS latitude,
			view_jams_clustered_element.jam_type,
			view_jams_clustered_element.jam_turntype,
			view_jams_clustered_element.jam_level,
			view_jams_clustered_element.roadtype,
			view_jams_clustered_element.speed_kmh,
			view_jams_clustered_element.length_m,
			view_jams_clustered_element.start_node,
			view_jams_clustered_element.end_node,
			view_jams_clustered_element.speed,
			view_jams_clustered_element.delay,
			view_jams_clustered_element.street,
			view_jams_clustered_element.city,
			view_jams_clustered_element.country,
			view_jams_clustered_element.segments,
			view_jams_clustered_element.waze_ts,
			view_jams_clustered_element.waze_creation_date,
			view_jams_clustered_element.date_fr_format,
			view_jams_clustered_element.waze_alert_age
		  FROM view_jams_clustered_element
	),
	view_jams_clustered_line AS (
		SELECT	row_number() OVER(ORDER BY view_jams_clustered_points.uuid) AS point_order,
				view_jams_clustered_points.uuid,
				ST_SetSRID(ST_MAKEPOINT(view_jams_clustered_points.longitude, view_jams_clustered_points.latitude),4326)::geometry(POINT,4326) AS the_geom,
				view_jams_clustered_points.longitude,
				view_jams_clustered_points.latitude,
				view_jams_clustered_points.jam_type,
				view_jams_clustered_points.jam_turntype,
				view_jams_clustered_points.jam_level,
				view_jams_clustered_points.roadtype,
				view_jams_clustered_points.speed_kmh,
				view_jams_clustered_points.length_m,
				view_jams_clustered_points.start_node,
				view_jams_clustered_points.end_node,
				view_jams_clustered_points.speed,
				view_jams_clustered_points.delay,
				view_jams_clustered_points.street,
				view_jams_clustered_points.city,
				view_jams_clustered_points.country,
				view_jams_clustered_points.segments,
				view_jams_clustered_points.waze_ts,
				view_jams_clustered_points.waze_creation_date,
				view_jams_clustered_points.date_fr_format,
				view_jams_clustered_points.waze_alert_age,
				view_jams_clustered_points.line
		  FROM view_jams_clustered_points
		  ORDER BY view_jams_clustered_points.uuid
	)
SELECT	row_number() OVER(ORDER BY view_jams_clustered_line.uuid) AS gid,
		view_jams_clustered_line.uuid,
		ST_SetSRID(ST_MakeLine(view_jams_clustered_line.the_geom ORDER BY view_jams_clustered_line.point_order),4326)::geometry(LINESTRING,4326) AS the_geom,				
		view_jams_clustered_line.jam_type,
		view_jams_clustered_line.jam_turntype,
		view_jams_clustered_line.jam_level,
		view_jams_clustered_line.roadtype,
		view_jams_clustered_line.speed_kmh,
		view_jams_clustered_line.length_m,
		view_jams_clustered_line.start_node,
		view_jams_clustered_line.end_node,
		view_jams_clustered_line.speed,
		view_jams_clustered_line.delay,
		view_jams_clustered_line.street,
		view_jams_clustered_line.city,
		view_jams_clustered_line.country,
		view_jams_clustered_line.waze_ts,
		view_jams_clustered_line.waze_creation_date,
		view_jams_clustered_line.date_fr_format,
		view_jams_clustered_line.waze_alert_age,
		now()::timestamp AS import_ts
  FROM view_jams_clustered_line
  GROUP BY uuid,jam_type,jam_turntype,jam_level,roadtype,speed_kmh,length_m,start_node,end_node,speed,delay,street,country,city,waze_ts,waze_creation_date,date_fr_format,waze_alert_age
  ORDER BY view_jams_clustered_line.uuid;

ALTER TABLE waze.view_jams_clustered ADD PRIMARY KEY(gid);
CREATE INDEX view_jams_clustered_geom_idx ON waze.view_jams_clustered USING gist (the_geom);
CREATE UNIQUE INDEX view_jams_clustered_uuid_idx ON waze.view_jams_clustered USING btree (uuid);
COMMENT ON TABLE waze.view_jams_clustered IS 'WAZE - Table des linéaires d''embouteillages générés par WAZE';

-- Column comments
COMMENT ON COLUMN waze.view_jams_clustered.gid IS 'PK.';
COMMENT ON COLUMN waze.view_jams_clustered.uuid IS 'ID.';
COMMENT ON COLUMN waze.view_jams_clustered.the_geom IS 'Géométrie';
COMMENT ON COLUMN waze.view_jams_clustered.jam_type IS 'Type d''embouteillage';
COMMENT ON COLUMN waze.view_jams_clustered.jam_turntype IS 'Type de virage';
COMMENT ON COLUMN waze.view_jams_clustered.jam_level IS 'Niveau de congestion du trafic (0 = flux libre - 5 = bloqué)';
COMMENT ON COLUMN waze.view_jams_clustered.roadtype IS 'Type de route';
COMMENT ON COLUMN waze.view_jams_clustered.speed_kmh IS 'Vitesse moyenne actuelle sur les segments bloqués en kmh';
COMMENT ON COLUMN waze.view_jams_clustered.length_m IS 'Longueur de l''embouteillage en mètres';
COMMENT ON COLUMN waze.view_jams_clustered.start_node IS 'Jonction/rue/ville la plus proche du début du blocage (fourni lorsque disponible)';
COMMENT ON COLUMN waze.view_jams_clustered.end_node IS 'Jonction/rue/ville la plus proche de la fin du blocage (fournie si disponible)';
COMMENT ON COLUMN waze.view_jams_clustered.speed IS 'Vitesse moyenne actuelle sur les segments bloqués en m/s';
COMMENT ON COLUMN waze.view_jams_clustered.delay IS 'Délai d''embouteillage (en secondes) par rapport à la vitesse d''écoulement libre (en cas de blocage, -1)';
COMMENT ON COLUMN waze.view_jams_clustered.street IS 'Nom de la voie';
COMMENT ON COLUMN waze.view_jams_clustered.city IS 'Ville';
COMMENT ON COLUMN waze.view_jams_clustered.country IS 'Pays';
COMMENT ON COLUMN waze.view_jams_clustered.waze_ts IS 'Horodatage de l''embouteillage signalé';
COMMENT ON COLUMN waze.view_jams_clustered.waze_creation_date IS 'Créé le';
COMMENT ON COLUMN waze.view_jams_clustered.date_fr_format IS 'Créé le (libellé)';
COMMENT ON COLUMN waze.view_jams_clustered.waze_alert_age IS 'Age';
COMMENT ON COLUMN waze.view_jams_clustered.import_ts IS 'Horodatage de l''import de l''embouteillage';

/******************************************************* END JAMS TABLES *********************************************************/

/**************************************************** IRREGULARITIES TABLES ******************************************************/
--DROP TABLE
DROP TABLE IF EXISTS waze.tmp_view_irregularities_clustered CASCADE;

--CREATE TABLE
CREATE TABLE waze.tmp_view_irregularities_clustered (
	gid serial NOT NULL PRIMARY KEY,
	data_json json NOT NULL
);

--COPIE DU JSON
COPY waze.tmp_view_irregularities_clustered (data_json)
  FROM '/opt/data/vca/cron/waze/data/waze-view-irregularities-clustered.json';

/**************************************************** IRREGULARITIES TABLES ******************************************************/

COMMIT;