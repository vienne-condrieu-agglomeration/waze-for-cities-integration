#!/bin/bash

#ON SE PLACE DANS LE DOSSIER DE TELECHARGEMENT ET ON TELECHARGE LES FICHIERS
cd /opt/data/vca/cron/waze/data
#ON SUPPRIME TOUTES LES DONNEES AVANT
sudo rm -Rf /opt/data/vca/cron/waze/data/*
echo "READY TO DOWNLOAD WAZE DATA !"

#TELECHARGEMENT DES ZONAGES DU GPU
echo "1/X : DATA DOWNLOAD FROM WAZE DATA FEED - ALERTS - Données sur les conditions de circulation mises à jour toutes les 2 minutes"
wget "https://world-georss.waze.com/rtserver/web/TGeoRSS?tk=ccp_partner&ccp_partner_name=partner_VienneCondrieuAgglomeration&format=JSON&types=alerts&polygon=4.64857000739179,45.4316591278279;4.64857000739179,45.6131681796003;5.08746662497162,45.6131681796003;5.08746662497162,45.4316591278279;4.64857000739179,45.4316591278279" \
	-O waze-view-alerts-clustered.json

echo "2/X : DATA DOWNLOAD FROM WAZE DATA FEED - TRAFFIC - Données sur les conditions de circulation mises à jour toutes les 2 minutes"
wget "https://world-georss.waze.com/rtserver/web/TGeoRSS?tk=ccp_partner&ccp_partner_name=partner_VienneCondrieuAgglomeration&format=JSON&types=traffic&polygon=4.64857000739179,45.4316591278279;4.64857000739179,45.6131681796003;5.08746662497162,45.6131681796003;5.08746662497162,45.4316591278279;4.64857000739179,45.4316591278279" \
	-O waze-view-jams-clustered.json
	
echo "3/X : DATA DOWNLOAD FROM WAZE DATA FEED - IRREGULARITIES - Données sur les conditions de circulation mises à jour toutes les 2 minutes"
wget "https://world-georss.waze.com/rtserver/web/TGeoRSS?tk=ccp_partner&ccp_partner_name=partner_VienneCondrieuAgglomeration&format=JSON&types=irregularities&polygon=4.64857000739179,45.4316591278279;4.64857000739179,45.6131681796003;5.08746662497162,45.6131681796003;5.08746662497162,45.4316591278279;4.64857000739179,45.4316591278279" \
	-O waze-view-irregularities-clustered.json

#ON MODIFIE LES DROITS D'ACCES AUX FICHIERS JSON
sudo chmod -Rf 777 /opt/data/vca/cron/waze/data/*
	
#IMPORT DES DONNEES DANS LA BDD PGSQL
echo "4/X : IMPORT DES DONNEES DANS LA BDD PGSQL - ALERTS"
sudo -u postgres -H -- psql -d vca -f "../SQL/01-IMPORT-DATA-FROM-WAZE-DATA-FEED.sql"

#ON REVIENT DANS LE DOSSIER DE TRAVAIL
cd /opt/data/vca/cron/waze
