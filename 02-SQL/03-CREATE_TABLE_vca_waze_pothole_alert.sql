DROP TABLE IF EXISTS waze.vca_waze_pothole_alert;
CREATE TABLE waze.vca_waze_pothole_alert AS
WITH
    waze_road_pot_hole AS (
        SELECT  waze.archive_view_alerts_clustered.gid,
            waze.archive_view_alerts_clustered.uuid,
            waze.archive_view_alerts_clustered.the_geom,
            waze.archive_view_alerts_clustered.alert_type,
            waze.archive_view_alerts_clustered.alert_subtype,
            waze.archive_view_alerts_clustered.roadtype,
            waze.archive_view_alerts_clustered.magvar,
            waze.archive_view_alerts_clustered.reportrating,
            waze.archive_view_alerts_clustered.confidence,
            waze.archive_view_alerts_clustered.reliability,
            waze.archive_view_alerts_clustered.street,
            waze.archive_view_alerts_clustered.city,
            bdtopo.bdtopo_auvergnerhonealpes_administratif_commune.insee_com AS code_insee,
            bdtopo.bdtopo_auvergnerhonealpes_administratif_commune.nom AS commune,
            waze.archive_view_alerts_clustered.country,
            waze.archive_view_alerts_clustered.longitude,
            waze.archive_view_alerts_clustered.latitude,
            waze.archive_view_alerts_clustered.waze_ts,
            waze.archive_view_alerts_clustered.waze_creation_date,
            --waze.archive_view_alerts_clustered.waze_alert_age,
            waze.archive_view_alerts_clustered.date_fr_format,
            --waze.archive_view_alerts_clustered.import_ts,
            --('epoch'::timestamptz + '1800 seconds'::interval * (EXTRACT(epoch FROM waze.archive_view_alerts_clustered.waze_creation_date::timestamptz)::int4 / 1800))::timestamp AS expiry_interval,
            TO_CHAR(('epoch'::timestamptz + '1800 seconds'::interval * (EXTRACT(epoch FROM waze.archive_view_alerts_clustered.waze_creation_date::timestamptz)::int4 / 1800))::timestamp , 'Le dd/mm/yyyy à HH24:MI:SS') AS expiry_interval
          FROM waze.archive_view_alerts_clustered
          INNER JOIN bdtopo.bdtopo_auvergnerhonealpes_administratif_commune
            ON ST_CONTAINS(bdtopo.bdtopo_auvergnerhonealpes_administratif_commune.the_geom, ST_TRANSFORM(waze.archive_view_alerts_clustered.the_geom, 2154))
          WHERE waze.archive_view_alerts_clustered.alert_type = 'WEATHERHAZARD'
          AND waze.archive_view_alerts_clustered.alert_subtype = 'HAZARD_ON_ROAD_POT_HOLE'
          AND bdtopo.bdtopo_auvergnerhonealpes_administratif_commune.insee_com IN ('38087','38107','38110','38131','38157','38160','38199','38215','38232','38238','38318','38336','38459','38480','38484','38487','38544','38558','69007','69064','69080','69097','69118','69119','69189','69193','69235','69236','69252','69253')
          ORDER BY waze.archive_view_alerts_clustered.waze_creation_date DESC
    )
-- Être à moins de 25 mètres d'au moins 1 autres points
SELECT  ST_ClusterDBSCAN(ST_TRANSFORM(the_geom, 2154), eps := 10, minPoints := 1) OVER (PARTITION BY street) AS cluster_id,
        waze_road_pot_hole.*
   FROM waze_road_pot_hole;
