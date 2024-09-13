-- change column names (repeat per schema)

DO $$ 
DECLARE
    old_column_name VARCHAR;
    new_column_name VARCHAR;
BEGIN
    
    FOR old_column_name, new_column_name IN 
        VALUES 
        ('AH_AnnGrassCover', 'AH_AnnGraminoidCover'),

        ('AH_GrassCover', 'AH_GraminoidCover'),
        ('AH_PerenForbGrassCover', 'AH_PerenForbGraminoidCover'),
        ('AH_PerenGrassCover', 'AH_PerenGraminoidCover')
    LOOP
        EXECUTE format('ALTER TABLE public_test."geoIndicators" RENAME COLUMN %I TO %I;', old_column_name, new_column_name);
    END LOOP;
END $$;

-- add new column
ALTER TABLE public_test.geoIndicators
ADD COLUMN "AH_AnnForbGraminoidCover" double precision;