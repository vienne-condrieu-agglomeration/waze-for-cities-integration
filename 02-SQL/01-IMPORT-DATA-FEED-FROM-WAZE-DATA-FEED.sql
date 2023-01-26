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
  
-- CREATE JAMS ARCHIVE TABLE
INSERT INTO waze.archive_view_jams_clustered (
	uuid,
	blocking_alert_uuid,
	the_geom,
	jam_type,
	jam_turntype,
	jam_level,
	roadtype,
	speed_kmh,
	length_m,
	start_node,
	end_node,
	speed,
	delay,
	street,
	city,
	country,
	waze_ts,
	waze_creation_date,
	date_fr_format,
	waze_alert_age,
	import_ts
)
WITH
	view_jams_clustered_element AS (
		SELECT	CAST(json_array_elements(data_json -> 'jams') ->> 'uuid' AS varchar) AS uuid,
				CAST(json_array_elements(data_json -> 'jams') ->> 'blockingAlertUuid' AS varchar) AS blocking_alert_uuid,
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
			view_jams_clustered_element.blocking_alert_uuid,
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
				view_jams_clustered_points.blocking_alert_uuid,
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
SELECT	--row_number() OVER(ORDER BY view_jams_clustered_line.uuid) AS gid,
		view_jams_clustered_line.uuid,
		view_jams_clustered_line.blocking_alert_uuid,
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
  GROUP BY uuid,blocking_alert_uuid,jam_type,jam_turntype,jam_level,roadtype,speed_kmh,length_m,start_node,end_node,speed,delay,street,country,city,waze_ts,waze_creation_date,date_fr_format,waze_alert_age
  ON CONFLICT (uuid) DO
	UPDATE SET the_geom	 	= archive_view_jams_clustered.the_geom,
		jam_type 			= archive_view_jams_clustered.jam_type,
		jam_turntype		= archive_view_jams_clustered.jam_turntype,
		jam_level 			= archive_view_jams_clustered.jam_level,
		roadtype 			= archive_view_jams_clustered.roadtype,
		speed_kmh 			= archive_view_jams_clustered.speed_kmh,
		length_m 			= archive_view_jams_clustered.length_m,
		start_node 			= archive_view_jams_clustered.start_node,
		end_node 			= archive_view_jams_clustered.end_node,
		speed 				= archive_view_jams_clustered.speed,
		delay 				= archive_view_jams_clustered.delay,
		street 				= archive_view_jams_clustered.street,
		city 				= archive_view_jams_clustered.city,
		country 			= archive_view_jams_clustered.country,
		waze_ts 			= archive_view_jams_clustered.waze_ts,
		waze_creation_date 	= archive_view_jams_clustered.waze_creation_date,
		date_fr_format 		= archive_view_jams_clustered.date_fr_format,
		waze_alert_age 		= archive_view_jams_clustered.waze_alert_age;
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

/***************************************************** TRAFFIC VIEW TABLES *******************************************************/
--waze_traffic_view_feed_spec
--DROP TABLE
DROP TABLE IF EXISTS waze.tmp_view_traffic_view_watchlist CASCADE;

--CREATE TABLE
CREATE TABLE waze.tmp_view_traffic_view_watchlist (
	gid serial NOT NULL PRIMARY KEY,
	data_json json NOT NULL
);

--COPIE DU JSON
COPY waze.tmp_view_traffic_view_watchlist (data_json)
  FROM '/opt/data/vca/cron/waze/data/waze-traffic-view-feed.json';
  
-- Watchlist
DROP TABLE IF EXISTS waze.vca_waze_traffic_view_watchlist CASCADE;
CREATE TABLE waze.vca_waze_traffic_view_watchlist AS
WITH 
    users_on_jams AS (
        SELECT  CAST(json_array_elements(data_json -> 'usersOnJams') ->> 'wazersCount' AS float4) AS wazers_count,
                CAST(json_array_elements(data_json -> 'usersOnJams') ->> 'jamLevel' AS integer) AS jam_level
                -- json_array_elements(data_json -> 'usersOnJams')
          FROM waze.tmp_view_traffic_view_watchlist
    ),
    routes AS (
        SELECT  CAST(json_array_elements(data_json -> 'routes') ->> 'id' AS varchar) AS id,
                json_array_elements(data_json -> 'routes') -> 'line' AS line,                
                CAST(json_array_elements(data_json -> 'routes') ->> 'toName' AS varchar) AS to_name,
                CAST(json_array_elements(data_json -> 'routes') ->> 'historicTime' AS varchar) AS historic_time,
                CAST(json_array_elements(data_json -> 'routes') ->> 'subRoutes' AS varchar) AS sub_routes,
                CAST(json_array_elements(data_json -> 'routes') ->> 'bbox' AS varchar) AS bbox,
                CAST(json_array_elements(data_json -> 'routes') ->> 'name' AS varchar) AS name,
                CAST(json_array_elements(data_json -> 'routes') ->> 'fromName' AS varchar) AS from_name,
                CAST(json_array_elements(data_json -> 'routes') ->> 'length' AS varchar) AS length,
                CAST(json_array_elements(data_json -> 'routes') ->> 'jamLevel' AS varchar) AS jam_level,                
                CAST(json_array_elements(data_json -> 'routes') ->> 'time' AS varchar) AS time_inseconds,
                CAST(json_array_elements(data_json -> 'routes') ->> 'type' AS varchar) AS type
                -- json_array_elements(data_json -> 'routes')
          FROM waze.tmp_view_traffic_view_watchlist
    ),
    routes_point AS (
       SELECT   routes.id,
                json_array_elements(routes.line) AS line,
                CAST(json_array_elements(routes.line) ->> 'x' AS numeric) AS longitude,
                CAST(json_array_elements(routes.line) ->> 'y' AS numeric) AS latitude,
                routes.to_name,
                routes.historic_time,
                routes.sub_routes,
                routes.bbox,
                routes.name,
                routes.from_name,
                routes.length,
                routes.jam_level,
                routes.time_inseconds,
                routes.type
         FROM routes
    ),
    routes_line AS (
        SELECT  row_number() OVER(ORDER BY routes_point.id) AS point_order,
                routes_point.id,
                ST_SetSRID(ST_MAKEPOINT(routes_point.longitude, routes_point.latitude),4326)::geometry(POINT,4326) AS the_geom,
                routes_point.line,
                routes_point.longitude,
                routes_point.latitude,
                routes_point.to_name,
                routes_point.historic_time,
                routes_point.sub_routes,
                routes_point.bbox,
                routes_point.name,
                routes_point.from_name,
                routes_point.length,
                routes_point.jam_level,
                routes_point.time_inseconds,
                routes_point.type       
          FROM routes_point
    )
SELECT  row_number() OVER(ORDER BY routes_line.id) AS gid,
        routes_line.id,
        ST_SetSRID(ST_MakeLine(routes_line.the_geom ORDER BY routes_line.point_order),4326)::geometry(LINESTRING,4326) AS the_geom,
        routes_line.to_name,
        routes_line.historic_time,
        routes_line.sub_routes,
        routes_line.bbox,
        routes_line.name,
        routes_line.from_name,
        routes_line.length,
        routes_line.jam_level,
        routes_line.time_inseconds,
        routes_line.type,
        now()::timestamp AS import_ts
  FROM routes_line
  GROUP BY routes_line.id,
        routes_line.to_name,
        routes_line.historic_time,
        routes_line.sub_routes,
        routes_line.bbox,
        routes_line.name,
        routes_line.from_name,
        routes_line.length,
        routes_line.jam_level,
        routes_line.time_inseconds,
        routes_line.TYPE
  ORDER BY routes_line.id;
  

ALTER TABLE waze.vca_waze_traffic_view_watchlist ADD PRIMARY KEY(gid);
CREATE INDEX vca_waze_traffic_view_watchlist_geom_idx ON waze.vca_waze_traffic_view_watchlist USING gist (the_geom);
CREATE UNIQUE INDEX vca_waze_traffic_view_watchlist_id_idx ON waze.vca_waze_traffic_view_watchlist USING btree (id);
COMMENT ON TABLE waze.vca_waze_traffic_view_watchlist IS 'WAZE - Table du flux d''affichage du trafic Waze - Watchlist';

-- Column comments
COMMENT ON COLUMN waze.vca_waze_traffic_view_watchlist.gid IS 'PK.';
COMMENT ON COLUMN waze.vca_waze_traffic_view_watchlist.id IS 'ID.';
COMMENT ON COLUMN waze.vca_waze_traffic_view_watchlist.the_geom IS 'Géométrie';
COMMENT ON COLUMN waze.vca_waze_traffic_view_watchlist.to_name IS 'Nom fourni par le propriétaire du flux, décrit l''itinéraire';
COMMENT ON COLUMN waze.vca_waze_traffic_view_watchlist.historic_time IS 'Temps en secondes qu''il faut habituellement pour traverser cette itinéraire le jour de la semaine et l''heure actuel';
COMMENT ON COLUMN waze.vca_waze_traffic_view_watchlist.sub_routes IS 'Liste des sous-routes';
COMMENT ON COLUMN waze.vca_waze_traffic_view_watchlist.bbox IS 'Enveloppe de la géométrie';
COMMENT ON COLUMN waze.vca_waze_traffic_view_watchlist.name IS 'Désignation du tronçon à surveiller';
COMMENT ON COLUMN waze.vca_waze_traffic_view_watchlist.from_name IS 'Nom fourni par le propriétaire du flux, décrit l''itinéraire';
COMMENT ON COLUMN waze.vca_waze_traffic_view_watchlist.length IS 'Longueur du parcours en mètres';
COMMENT ON COLUMN waze.vca_waze_traffic_view_watchlist.jam_level IS 'Niveau d''embouteillage total de l''itinéraire : 0=Pas d''embouteillage à 4=arrêt';
COMMENT ON COLUMN waze.vca_waze_traffic_view_watchlist.time_inseconds IS 'Temps en secondes qu''il faut pour traverser l''itinéraire en ce moment';
COMMENT ON COLUMN waze.vca_waze_traffic_view_watchlist."type" IS 'Type';
COMMENT ON COLUMN waze.vca_waze_traffic_view_watchlist.import_ts IS 'Horodatage de l''import de l''alerte';
/***************************************************** TRAFFIC VIEW TABLES *******************************************************/


COMMIT;
