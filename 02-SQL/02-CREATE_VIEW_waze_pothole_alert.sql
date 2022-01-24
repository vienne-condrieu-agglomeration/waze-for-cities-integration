--DROP VIEW IF EXISTS waze.waze_pothole_alert;
--CREATE OR EPLACE VIEW waze.waze_pothole_alert AS
WITH
	waze_road_pot_hole AS (
		SELECT	waze.archive_view_alerts_clustered.*
		  FROM waze.archive_view_alerts_clustered
		  WHERE waze.archive_view_alerts_clustered.alert_type = 'WEATHERHAZARD'
		  AND waze.archive_view_alerts_clustered.alert_subtype = 'HAZARD_ON_ROAD_POT_HOLE'
		  AND waze.archive_view_alerts_clustered.roadtype != 3
		  AND AGE(now(), waze.archive_view_alerts_clustered.waze_creation_date) < '30 days'::interval
		  ORDER BY waze.archive_view_alerts_clustered.waze_creation_date DESC
	),
	waze_road_pot_hole_bdtopo_route AS (
		SELECT	DISTINCT waze_road_pot_hole.uuid,
			waze_road_pot_hole.the_geom,
			waze_road_pot_hole.alert_type,
			waze_road_pot_hole.alert_subtype,
			waze_road_pot_hole.roadtype,
			waze_road_pot_hole.magvar,
			waze_road_pot_hole.reportrating,
			waze_road_pot_hole.confidence,
			waze_road_pot_hole.reliability,
			waze_road_pot_hole.street,
			waze_road_pot_hole.city,
			waze_road_pot_hole.country,
			waze_road_pot_hole.longitude,
			waze_road_pot_hole.latitude,
			waze_road_pot_hole.waze_ts,
			waze_road_pot_hole.waze_creation_date,
			waze_road_pot_hole.waze_alert_age,
			waze_road_pot_hole.date_fr_format,
			waze_road_pot_hole.import_ts,
			bdtopo_2019.bdtopo_vca_transport_troncon_route.id AS id_bdtopo,
			bdtopo_2019.bdtopo_vca_transport_troncon_route.nature AS nature_voie,
			bdtopo_2019.bdtopo_vca_transport_troncon_route.nom_1_g AS nom_voie,
			bdtopo_2019.bdtopo_vca_transport_troncon_route.sens AS sens,
			bdtopo_2019.bdtopo_vca_transport_troncon_route.cl_admin AS cl_admin,
			ST_DISTANCE(ST_TRANSFORM(waze_road_pot_hole.the_geom, 2154), bdtopo_2019.bdtopo_vca_transport_troncon_route.the_geom) AS distance_m
		  FROM waze_road_pot_hole
		  INNER JOIN bdtopo_2019.bdtopo_vca_transport_troncon_route
		    ON ST_DWithin(ST_TRANSFORM(waze_road_pot_hole.the_geom, 2154), bdtopo_2019.bdtopo_vca_transport_troncon_route.the_geom, 25)
	),
	min_distance AS (
		SELECT	waze_road_pot_hole_bdtopo_route.uuid,
				MIN(waze_road_pot_hole_bdtopo_route.distance_m) AS distance_m
		  FROM waze_road_pot_hole_bdtopo_route
		  GROUP BY waze_road_pot_hole_bdtopo_route.uuid
	)
SELECT	waze_road_pot_hole_bdtopo_route.uuid AS id_pothole,
	waze_road_pot_hole_bdtopo_route.the_geom,
	waze_road_pot_hole_bdtopo_route.alert_type,
	waze_road_pot_hole_bdtopo_route.alert_subtype,
	waze_road_pot_hole_bdtopo_route.roadtype,
	waze_road_pot_hole_bdtopo_route.magvar AS azimuth,
	waze_road_pot_hole_bdtopo_route.reportrating,
	waze_road_pot_hole_bdtopo_route.confidence,
	waze_road_pot_hole_bdtopo_route.reliability,
	waze_road_pot_hole_bdtopo_route.street,
	waze_road_pot_hole_bdtopo_route.city,
	waze_road_pot_hole_bdtopo_route.country,
	waze_road_pot_hole_bdtopo_route.longitude,
	waze_road_pot_hole_bdtopo_route.latitude,
	waze_road_pot_hole_bdtopo_route.waze_ts,
	waze_road_pot_hole_bdtopo_route.waze_creation_date,
	waze_road_pot_hole_bdtopo_route.waze_alert_age,
	waze_road_pot_hole_bdtopo_route.date_fr_format,
	waze_road_pot_hole_bdtopo_route.import_ts,
	waze_road_pot_hole_bdtopo_route.id_bdtopo,
	waze_road_pot_hole_bdtopo_route.nature_voie,
	waze_road_pot_hole_bdtopo_route.nom_voie,
	waze_road_pot_hole_bdtopo_route.sens,
	waze_road_pot_hole_bdtopo_route.cl_admin,
	ROUND(waze_road_pot_hole_bdtopo_route.distance_m::numeric, 2) AS distance_m
  FROM waze_road_pot_hole_bdtopo_route
  INNER JOIN min_distance
    ON min_distance.uuid = waze_road_pot_hole_bdtopo_route.uuid
	AND min_distance.distance_m = waze_road_pot_hole_bdtopo_route.distance_m;
	
	
-- RESTE A DETERMINER LES DOUBLONS DE CREATION DANS WAZE