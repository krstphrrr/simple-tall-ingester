-- TABLES WITH UNIQUE PRIMARYKEY
-- geoIndicators 
-- dataHeader

-- NO DATA

-- dataDustDeposition
-- dataPlotCharacterization
-- dataSoilHorizons




-- DATAGAP

ALTER TABLE public_test."dataGap" 
ADD CONSTRAINT unique_datagapprimarykey_column 
UNIQUE ("PrimaryKey","LineKey", "RecKey", "SeqNo", "Gap", "RecType");

-- select * from public_test."dataGap" 
-- WHERE
-- "PrimaryKey" = 'ID_SHFO_LUP_2017_BWD_148_V12022-09-01'
-- AND "LineKey" = 'ID_SHFO_LUP_2017_BWD_148_V1_2'
-- AND "RecKey" = 'ID_SHFO_LUP_2017_BWD_148_V1_2'
-- AND "SeqNo" = '315'
-- AND "Gap" = '20'

-- DATAHEIGHT


-- ALTER TABLE public_test."dataHeight" 
-- ADD CONSTRAINT unique_dataheightprimarykey_column 
-- UNIQUE ("PrimaryKey","LineKey", "RecKey", "PointLoc", "PointNbr", "type");

select * from public_test."dataHeight" 
WHERE
"PrimaryKey" = '16020914473216332015-09-21'
AND "LineKey" = '1602091447357911'
AND "RecKey" = '1604071232156483'
AND "PointLoc" = '86'
AND "PointNbr" = '86'

-- DATA HORIZONTAL FLUX
-- DUPLICATES FOUND

-- ALTER TABLE public_test."dataHorizontalFlux" 
-- ADD CONSTRAINT unique_datahorizontalfluxprimarykey_column 
-- UNIQUE ("PrimaryKey","BoxID", "StackID");

select * from public_test."dataHorizontalFlux" 
WHERE
"PrimaryKey" = '15120315082630972016-04-13'
AND "BoxID" = '1702110901209268'
AND "StackID" = '1702110901203785'

-- DATA LPI
-- DUPLICATES FOUND

-- ALTER TABLE public_test."dataLPI" 
-- ADD CONSTRAINT unique_datalpiprimarykey_column 
-- UNIQUE ("PrimaryKey","LineKey", "RecKey", "layer", "code", "PointLoc");

select * from public_test."dataLPI" 
WHERE
"PrimaryKey" = '19020816041349872021-03-29'
AND "LineKey" = '190208160414117'
AND "RecKey" = '2208061304094802'
AND "layer" = 'Lower1'
AND "code" = 'TRAE'
AND "PointLoc" = '13'


-- data soilstability

ALTER TABLE public_test."dataSoilStability" 
ADD CONSTRAINT unique_datasoilstabilityprimarykey_column 
UNIQUE ("PrimaryKey","LineKey", "RecKey", "Position","Pos", "Veg");

-- select * from public_test."dataSoilStability" 
-- WHERE
-- "PrimaryKey" = 'MTB07000_OverSample2018_BHW-12-S2018-09-01'
-- AND "LineKey" = 'MTB07000_OverSample2018_BHW-12-S'
-- AND "RecKey" = 'MTB07000_OverSample2018_BHW-12-S'
-- AND "Pos" = '8'
-- AND "Veg" = 'G'


-- DATA species inventory


ALTER TABLE public_test."dataSpeciesInventory" 
ADD CONSTRAINT unique_dataspeciesinventoryprimarykey_column 
UNIQUE ("PrimaryKey","LineKey", "RecKey", "Species");

-- geoSpecies


-- ALTER TABLE public_test."geoSpecies" 
-- ADD CONSTRAINT unique_geospeciesprimarykey_column 
-- UNIQUE ("PrimaryKey","DBKey", "ProjectKey", "Species");


select * from public_test."geoSpecies" 
WHERE
"PrimaryKey" = '2020327199101B1'
AND "DBKey" = '2020'
AND "ProjectKey" = 'BLM_AIM'
AND "Species" = 'LEPTO22'
-- Only differ in duration
