DROP TABLE IF EXISTS waze.archive_view_alerts_clustered;
CREATE TABLE waze.archive_view_alerts_clustered (
	gid serial4 NOT NULL, -- PK.
	uuid varchar(37) NOT NULL DEFAULT uuid_generate_v4()::text, -- ID.
	the_geom geometry(point, 4326) NULL, -- Géométrie
	alert_type varchar NULL, -- Type d'alerte
	alert_subtype varchar NULL, -- Sous-type d'alerte
	roadtype int4 NULL, -- Type de route
	magvar int4 NULL, -- Azimuth
	reportrating int4 NULL, -- Classement de l'utilisateur entre 1 et 6 (6 = utilisateur le mieux classé)
	confidence int4 NULL, -- Confiance dans l'alerte basée sur les réactions des autres utilisateurs (pouce levé)
	reliability int4 NULL, -- Fiabilité - Confiance dans l'alerte basée sur la saisie de l'utilisateur (par exemple, pouce levé) et l'évaluation du rapport
	street varchar NULL, -- Nom de la voie
	city varchar NULL, -- Ville
	country varchar NULL, -- Pays
	longitude float8 NULL, -- Longitude
	latitude float8 NULL, -- Latitude
	waze_ts int8 NULL, -- Horodatage de l'alerte signalée
	waze_creation_date timestamp NULL, -- Créé le
	waze_alert_age interval NULL, -- Age
	date_fr_format text NULL, -- Créé le (libellé)
	import_ts varchar NOT NULL, -- Horodatage de l'import de l'alerte
	CONSTRAINT archive_view_alerts_clustered_pk PRIMARY KEY (gid)
);
CREATE INDEX archive_view_alerts_clustered_geom_idx ON waze.archive_view_alerts_clustered USING gist (the_geom);
CREATE UNIQUE INDEX archive_view_alerts_clustered_uuid_idx ON waze.archive_view_alerts_clustered USING btree (uuid);
COMMENT ON TABLE waze.archive_view_alerts_clustered IS 'WAZE - Table d''archivage de toutes les alertes moisonnées depuis le début de la mise en place des données Waze';

-- Column comments

COMMENT ON COLUMN waze.archive_view_alerts_clustered.gid IS 'PK.';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.uuid IS 'ID.';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.the_geom IS 'Géométrie';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.alert_type IS 'Type d''alerte';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.alert_subtype IS 'Sous-type d''alerte';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.roadtype IS 'Type de route';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.magvar IS 'Azimuth';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.reportrating IS 'Classement de l''utilisateur entre 1 et 6 (6 = utilisateur le mieux classé)';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.confidence IS 'Confiance dans l''alerte basée sur les réactions des autres utilisateurs (pouce levé)';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.reliability IS 'Fiabilité - Confiance dans l''alerte basée sur la saisie de l''utilisateur (par exemple, pouce levé) et l''évaluation du rapport';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.street IS 'Nom de la voie';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.city IS 'Ville';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.country IS 'Pays';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.longitude IS 'Longitude';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.latitude IS 'Latitude';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.waze_ts IS 'Horodatage de l''alerte signalée';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.waze_creation_date IS 'Créé le';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.waze_alert_age IS 'Age';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.date_fr_format IS 'Créé le (libellé)';
COMMENT ON COLUMN waze.archive_view_alerts_clustered.import_ts IS 'Horodatage de l''import de l''alerte';

--Reference guide for alerts, jams and irregularities
DROP TABLE IF EXISTS waze.waze_reference_alert_type;
CREATE TABLE waze.waze_reference_alert_type (
	gid serial NOT NULL, -- PK.
	alert_type varchar NOT NULL, -- Type d'alerte
	alert_subtype varchar NOT NULL, -- Soust-type d'alerte
	label_fr varchar NOT NULL, -- Libellé en français
	CONSTRAINT waze_reference_alert_type_pk PRIMARY KEY (gid)
);
CREATE INDEX waze_reference_alert_type_idx ON waze.waze_reference_alert_type USING btree (alert_type);
CREATE INDEX waze_reference_alert_subtype_idx ON waze.waze_reference_alert_type USING btree (alert_subtype);
COMMENT ON TABLE waze.waze_reference_alert_type IS 'WAZE - Table de référence des types et sous-types d''alertes - Guide de référence pour les alertes, embouteillages et irrégularités';

-- Column comments
COMMENT ON COLUMN waze.waze_reference_alert_type.gid IS 'PK.';
COMMENT ON COLUMN waze.waze_reference_alert_type.alert_type IS 'Type d''alerte';
COMMENT ON COLUMN waze.waze_reference_alert_type.alert_subtype IS 'Soust-type d''alerte';
COMMENT ON COLUMN waze.waze_reference_alert_type.label_fr IS 'Libellé en français';

INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'ACCIDENT', 'ACCIDENT_MINOR', 'ACCIDENT MINEUR');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'ACCIDENT', 'ACCIDENT_MAJOR', 'ACCIDENT MAJEUR');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'ACCIDENT', 'NO_SUBTYPE', 'AUCUN SOUS-TYPE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'JAM', 'JAM_MODERATE_TRAFFIC', 'EMBOUTEILLAGE DU TRAFIC - MODÉRÉ');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'JAM', 'JAM_HEAVY_TRAFFIC', 'EMBOUTEILLAGE DU TRAFIC - LOURD');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'JAM', 'JAM_STAND_STILL_TRAFFIC', 'EMBOUTEILLAGE DU TRAFIC - ARRÊT TOTAL DE LA CIRCULATION');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'JAM', 'JAM_LIGHT_TRAFFIC', 'EMBOUTEILLAGE DU TRAFIC - LÉGER');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'JAM', 'NO_SUBTYPE', 'AUCUN SOUS-TYPE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_ON_ROAD', 'DANGER SUR LA ROUTE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_ON_SHOULDER', 'DANGER SUR L''ACCOTEMENT');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_WEATHER', 'TEMPS DANGEREUX');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_ON_ROAD_OBJECT', 'DANGER SUR LA ROUTE - OBJET ROUTIER');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_ON_ROAD_POT_HOLE', 'DANGER SUR LA ROUTE - NID DE POULE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_ON_ROAD_ROAD_KILL', 'DANGER SUR LA ROUTE - MORT SUR LA ROUTE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_ON_SHOULDER_CAR_STOPPED', 'DANGER SUR L''ACCOTEMENT - VOITURE À L''ARRÊT');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_ON_SHOULDER_ANIMALS', 'DANGER SUR L''ACCOTEMENT - ANIMAL');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_ON_SHOULDER_MISSING_SIGN', 'DANGER SUR L''ACCOTEMENT - SIGNALISATION MANQUANTE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_WEATHER_FOG', 'TEMPS DANGEREUX - BROUILLARD');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_WEATHER_HAIL', 'TEMPS DANGEREUX - GRÊLE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_WEATHER_HEAVY_RAIN', 'TEMPS DANGEREUX - FORTE PLUIE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_WEATHER_HEAVY_SNOW', 'TEMPS DANGEREUX - FORTE NEIGE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_WEATHER_FLOOD', 'TEMPS DANGEREUX - INONDATION');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_WEATHER_MONSOON', 'TEMPS DANGEREUX - MOUSSON');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_WEATHER_TORNADO', 'TEMPS DANGEREUX - TORNADE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_WEATHER_HEAT_WAVE', 'TEMPS DANGEREUX - VAGUE DE CHALEUR');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_WEATHER_HURRICANE', 'TEMPS DANGEREUX - OURAGAN');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_WEATHER_FREEZING_RAIN', 'TEMPS DANGEREUX - PLUIE VERGLACANTE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_ON_ROAD_LANE_CLOSED', 'DANGER SUR LA ROUTE - VOIE ROUTIÈRE FERMÉE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_ON_ROAD_OIL', 'DANGER SUR LA ROUTE - TACHE D''HUILE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_ON_ROAD_ICE', 'DANGER SUR LA ROUTE - ROUTE VERGLACÉE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_ON_ROAD_CONSTRUCTION', 'DANGER SUR LA ROUTE - TRAVAUX');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_ON_ROAD_CAR_STOPPED', 'DANGER SUR LA ROUTE - VOITURE À L''ARRET');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'HAZARD_ON_ROAD_TRAFFIC_LIGHT_FAULT', 'DANGER SUR LA ROUTE - DÉFAILLANCE DES FEUX DE CIRCULATION ROUTIÈRE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'WEATHERHAZARD-HAZARD', 'NO_SUBTYPE', 'AUCUN SOUS-TYPE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'MISC ', 'NO_SUBTYPE', 'AUCUN SOUS-TYPE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'CONSTRUCTION', 'NO_SUBTYPE', 'AUCUN SOUS-TYPE');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'ROAD_CLOSED ', 'ROAD_CLOSED_HAZARD', 'ROUTE FERMÉE - DANGER');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'ROAD_CLOSED ', 'ROAD_CLOSED_CONSTRUCTION', 'ROUTE FERMÉE - TRAVAUX');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'ROAD_CLOSED ', 'ROAD_CLOSED_EVENT', 'ROUTE FERMÉE - MANIFESTATION');
INSERT INTO waze.waze_reference_alert_type VALUES (DEFAULT, 'ROAD_CLOSED ', 'NO_SUBTYPE', 'AUCUN SOUS-TYPE');


--Reference guide for alerts, jams and irregularities
DROP TABLE IF EXISTS waze.waze_reference_road_type;
CREATE TABLE waze.waze_reference_road_type (
	gid serial4 NOT NULL, -- PK.
	road_type integer NOT NULL, -- Type de route
	label_en varchar NOT NULL, -- Libellé EN
	label_fr varchar NOT NULL, -- Libellé FR
	CONSTRAINT waze_reference_road_type_pk PRIMARY KEY (gid)
);
CREATE INDEX waze_reference_road_type_idx ON waze.waze_reference_road_type USING btree (road_type);
COMMENT ON TABLE waze.waze_reference_road_type IS 'WAZE - Table de référence des différents types de route - Guide de référence pour les alertes, embouteillages et irrégularités';

-- Column comments
COMMENT ON COLUMN waze.waze_reference_road_type.gid IS 'PK.';
COMMENT ON COLUMN waze.waze_reference_road_type.road_type IS 'Type de route';
COMMENT ON COLUMN waze.waze_reference_road_type.label_en IS 'Libellé EN';
COMMENT ON COLUMN waze.waze_reference_road_type.label_fr IS 'Libellé FR';

INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 1, 'Street', 			'RUE');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 2, 'Primary Street', 	'RUE PRINCIPALE');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 3, 'Freeway', 			'AUTOROUTE');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 4, 'Ramp', 				'RAMPE');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 5, 'Trail', 				'PISTE');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 6, 'Primary Street', 	'RUE PRINCIPALE');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 7, 'Secondary Street', 	'RUE SECONDAIRE');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 8, '4X4 Trails', 		'PISTES 4X4');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 9, 'Walkway', 			'PASSERELLE');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 10, 'Pedestrian', 		'VOIE PIÉTONNE');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 11, 'Exit', 				'SORTIE');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 14, '4X4 Trails', 		'PISTES 4X4');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 15, 'Ferry crossing', 	'TRAVERSÉE EN FERRY');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 16, 'Stairway', 			'ESCALIER');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 17, 'Private road', 		'CHEMIN PRIVÉ');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 18, 'Railroads', 		'CHEMIN DE FER');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 19, 'Runway/Taxiway', 	'PISTE/VOIE DE CIRCULATION');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 20, 'Parking lot road', 	'VOIE DE PARKING');
INSERT INTO waze.waze_reference_road_type VALUES (DEFAULT, 21, 'Service road', 		'SERVICE ROUTIER');


-- waze.archive_view_jams_clustered definition
DROP TABLE IF EXISTS waze.archive_view_jams_clustered;
CREATE TABLE waze.archive_view_jams_clustered (
	gid serial8 NOT NULL, -- PK.
	uuid varchar NULL, -- ID.
	blocking_alert_uuid varchar NULL, -- ID. BLOCKING ALERT
	the_geom public.geometry(linestring, 4326) NULL, -- Géométrie
	jam_type varchar NULL, -- Type d'embouteillage
	jam_turntype varchar NULL, -- Type de virage
	jam_level int4 NULL, -- Niveau de congestion du trafic (0 = flux libre - 5 = bloqué)
	roadtype int4 NULL, -- Type de route
	speed_kmh float8 NULL, -- Vitesse moyenne actuelle sur les segments bloqués en kmh
	length_m float8 NULL, -- Longueur de l'embouteillage en mètres
	start_node varchar NULL, -- Jonction/rue/ville la plus proche du début du blocage (fourni lorsque disponible)
	end_node varchar NULL, -- Jonction/rue/ville la plus proche de la fin du blocage (fournie si disponible)
	speed float8 NULL, -- Vitesse moyenne actuelle sur les segments bloqués en m/s
	delay int4 NULL, -- Délai d'embouteillage (en secondes) par rapport à la vitesse d'écoulement libre (en cas de blocage, -1)
	street varchar NULL, -- Nom de la voie
	city varchar NULL, -- Ville
	country varchar NULL, -- Pays
	waze_ts int8 NULL, -- Horodatage de l'embouteillage signalé
	waze_creation_date timestamp NULL, -- Créé le
	date_fr_format text NULL, -- Créé le (libellé)
	waze_alert_age interval NULL, -- Age
	import_ts timestamp NULL, -- Horodatage de l'import de l'embouteillage
	CONSTRAINT archive_view_jams_clustered_pkey PRIMARY KEY (gid)
);
CREATE INDEX archive_view_jams_clustered_geom_idx ON waze.archive_view_jams_clustered USING gist (the_geom);
CREATE UNIQUE INDEX archive_view_jams_clustered_uuid_idx ON waze.archive_view_jams_clustered USING btree (uuid);
COMMENT ON TABLE waze.archive_view_jams_clustered IS 'WAZE - Table d''archivage des linéaires d''embouteillages générés par WAZE';

-- Column comments

COMMENT ON COLUMN waze.archive_view_jams_clustered.gid IS 'PK.';
COMMENT ON COLUMN waze.archive_view_jams_clustered.uuid IS 'ID.';
COMMENT ON COLUMN waze.archive_view_jams_clustered.blocking_alert_uuid IS 'ID. BLOCKING ALERT';
COMMENT ON COLUMN waze.archive_view_jams_clustered.the_geom IS 'Géométrie';
COMMENT ON COLUMN waze.archive_view_jams_clustered.jam_type IS 'Type d''embouteillage';
COMMENT ON COLUMN waze.archive_view_jams_clustered.jam_turntype IS 'Type de virage';
COMMENT ON COLUMN waze.archive_view_jams_clustered.jam_level IS 'Niveau de congestion du trafic (0 = flux libre - 5 = bloqué)';
COMMENT ON COLUMN waze.archive_view_jams_clustered.roadtype IS 'Type de route';
COMMENT ON COLUMN waze.archive_view_jams_clustered.speed_kmh IS 'Vitesse moyenne actuelle sur les segments bloqués en kmh';
COMMENT ON COLUMN waze.archive_view_jams_clustered.length_m IS 'Longueur de l''embouteillage en mètres';
COMMENT ON COLUMN waze.archive_view_jams_clustered.start_node IS 'Jonction/rue/ville la plus proche du début du blocage (fourni lorsque disponible)';
COMMENT ON COLUMN waze.archive_view_jams_clustered.end_node IS 'Jonction/rue/ville la plus proche de la fin du blocage (fournie si disponible)';
COMMENT ON COLUMN waze.archive_view_jams_clustered.speed IS 'Vitesse moyenne actuelle sur les segments bloqués en m/s';
COMMENT ON COLUMN waze.archive_view_jams_clustered.delay IS 'Délai d''embouteillage (en secondes) par rapport à la vitesse d''écoulement libre (en cas de blocage, -1)';
COMMENT ON COLUMN waze.archive_view_jams_clustered.street IS 'Nom de la voie';
COMMENT ON COLUMN waze.archive_view_jams_clustered.city IS 'Ville';
COMMENT ON COLUMN waze.archive_view_jams_clustered.country IS 'Pays';
COMMENT ON COLUMN waze.archive_view_jams_clustered.waze_ts IS 'Horodatage de l''embouteillage signalé';
COMMENT ON COLUMN waze.archive_view_jams_clustered.waze_creation_date IS 'Créé le';
COMMENT ON COLUMN waze.archive_view_jams_clustered.date_fr_format IS 'Créé le (libellé)';
COMMENT ON COLUMN waze.archive_view_jams_clustered.waze_alert_age IS 'Age';
COMMENT ON COLUMN waze.archive_view_jams_clustered.import_ts IS 'Horodatage de l''import de l''embouteillage';

-- Permissions
ALTER TABLE waze.archive_view_jams_clustered OWNER TO dyndb;
