DROP TABLE IF EXISTS waze.vca_waze_accident_alert;
CREATE TABLE waze.vca_waze_accident_alert AS
WITH
    waze_accidents AS (
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
            'epoch'::timestamptz + '1800 seconds'::interval * (EXTRACT(epoch FROM waze.archive_view_alerts_clustered.waze_creation_date::timestamptz)::int4 / 1800) AS expiry_interval_15mins
          FROM waze.archive_view_alerts_clustered
          INNER JOIN bdtopo.bdtopo_auvergnerhonealpes_administratif_commune
            ON ST_CONTAINS(bdtopo.bdtopo_auvergnerhonealpes_administratif_commune.the_geom, ST_TRANSFORM(waze.archive_view_alerts_clustered.the_geom, 2154))
          WHERE alert_type = 'ACCIDENT'
          AND bdtopo.bdtopo_auvergnerhonealpes_administratif_commune.insee_com IN ('38087','38107','38110','38131','38157','38160','38199','38215','38232','38238','38318','38336','38459','38480','38484','38487','38544','38558','69007','69064','69080','69097','69118','69119','69189','69193','69235','69236','69252','69253')
          ORDER BY waze.archive_view_alerts_clustered.waze_creation_date DESC
    )
SELECT  ST_COLLECT(waze_accidents.the_geom) AS the_geom,
    waze_accidents.alert_type,
    STRING_AGG(TRIM(BOTH FROM waze_accidents.alert_subtype), ',') AS alert_subtype_list,
    COUNT(waze_accidents.expiry_interval_15mins) AS nb_accident,
    STRING_AGG(waze_accidents.uuid, ',') AS uuid_list,
    STRING_AGG(waze_accidents.date_fr_format, ', ') AS waze_creation_date_list,
    waze_accidents.expiry_interval_15mins
  FROM waze_accidents
  --WHERE waze_accidents.uuid IN ('1cb59f45-3bde-4e7a-9f78-999ab3224108','6346d656-4682-47fa-9b8d-119e40352680')
  GROUP BY waze_accidents.expiry_interval_15mins, waze_accidents.alert_type;
