USE DENUE;

CREATE TABLE IF NOT EXISTS UNIDADES_ECONOMICAS_STG AS
SELECT * FROM UNIDADES_ECONOMICAS_RAW LIMIT 0;

INSERT INTO UNIDADES_ECONOMICAS_RAW
SELECT * FROM UNIDADES_ECONOMICAS_STG;

UPDATE UNIDADES_ECONOMICAS_STG SET ID = NULL WHERE TRIM(ID) = '' OR ID = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET CLEE = NULL WHERE TRIM(CLEE) = '' OR CLEE = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET NOM_ESTAB = NULL WHERE TRIM(NOM_ESTAB) = '' OR NOM_ESTAB = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET RAZ_SOCIAL = NULL WHERE TRIM(RAZ_SOCIAL) = '' OR RAZ_SOCIAL = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET CODIGO_ACT = NULL WHERE TRIM(CODIGO_ACT) = '' OR CODIGO_ACT = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET NOMBRE_ACT = NULL WHERE TRIM(NOMBRE_ACT) = '' OR NOMBRE_ACT = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET PER_OCU = NULL WHERE TRIM(PER_OCU) = '' OR PER_OCU = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET TIPO_VIAL = NULL WHERE TRIM(TIPO_VIAL) = '' OR TIPO_VIAL = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET NOM_VIAL = NULL WHERE TRIM(NOM_VIAL) = '' OR NOM_VIAL = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET TIPO_V_E_1 = NULL WHERE TRIM(TIPO_V_E_1) = '' OR TIPO_V_E_1 = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET NOM_V_E_1 = NULL WHERE TRIM(NOM_V_E_1) = '' OR NOM_V_E_1 = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET TIPO_V_E_2 = NULL WHERE TRIM(TIPO_V_E_2) = '' OR TIPO_V_E_2 = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET NOM_V_E_2 = NULL WHERE TRIM(NOM_V_E_2) = '' OR NOM_V_E_2 = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET TIPO_V_E_3 = NULL WHERE TRIM(TIPO_V_E_3) = '' OR TIPO_V_E_3 = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET NOM_V_E_3 = NULL WHERE TRIM(NOM_V_E_3) = '' OR NOM_V_E_3 = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET NUMERO_EXT = NULL WHERE TRIM(NUMERO_EXT) = '' OR NUMERO_EXT = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET LETRA_EXT = NULL WHERE TRIM(LETRA_EXT) = '' OR LETRA_EXT = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET EDIFICIO = NULL WHERE TRIM(EDIFICIO) = '' OR EDIFICIO = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET EDIFICIO_E = NULL WHERE TRIM(EDIFICIO_E) = '' OR EDIFICIO_E = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET NUMERO_INT = NULL WHERE TRIM(NUMERO_INT) = '' OR NUMERO_INT = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET LETRA_INT = NULL WHERE TRIM(LETRA_INT) = '' OR LETRA_INT = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET TIPO_ASENT = NULL WHERE TRIM(TIPO_ASENT) = '' OR TIPO_ASENT = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET NOMB_ASENT = NULL WHERE TRIM(NOMB_ASENT) = '' OR NOMB_ASENT = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET TIPOCENCOM = NULL WHERE TRIM(TIPOCENCOM) = '' OR TIPOCENCOM = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET NOM_CENCOM = NULL WHERE TRIM(NOM_CENCOM) = '' OR NOM_CENCOM = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET NUM_LOCAL = NULL WHERE TRIM(NUM_LOCAL) = '' OR NUM_LOCAL = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET COD_POSTAL = NULL WHERE TRIM(COD_POSTAL) = '' OR COD_POSTAL = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET CVE_ENT = NULL WHERE TRIM(CVE_ENT) = '' OR CVE_ENT = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET ENTIDAD = NULL WHERE TRIM(ENTIDAD) = '' OR ENTIDAD = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET CVE_MUN = NULL WHERE TRIM(CVE_MUN) = '' OR CVE_MUN = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET MUNICIPIO = NULL WHERE TRIM(MUNICIPIO) = '' OR MUNICIPIO = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET CVE_LOC = NULL WHERE TRIM(CVE_LOC) = '' OR CVE_LOC = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET LOCALIDAD = NULL WHERE TRIM(LOCALIDAD) = '' OR LOCALIDAD = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET AGEB = NULL WHERE TRIM(AGEB) = '' OR AGEB = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET MANZANA = NULL WHERE TRIM(MANZANA) = '' OR MANZANA = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET TELEFONO = NULL WHERE TRIM(TELEFONO) = '' OR TELEFONO = 'NaN' OR TELEFONO RLIKE '[a-zA-Z ]+';
UPDATE UNIDADES_ECONOMICAS_STG SET CORREOELEC = NULL WHERE TRIM(CORREOELEC) = '' OR CORREOELEC = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET WWW = NULL WHERE TRIM(WWW) = '' OR WWW = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET TIPOUNIECO = NULL WHERE TRIM(TIPOUNIECO) = '' OR TIPOUNIECO = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET LATITUD = NULL WHERE TRIM(LATITUD) = '' OR LATITUD = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET LONGITUD = NULL WHERE TRIM(LONGITUD) = '' OR LONGITUD = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET FECHA_ALTA = NULL WHERE TRIM(FECHA_ALTA) = '' OR FECHA_ALTA = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET SOURCE_FILE = NULL WHERE TRIM(SOURCE_FILE) = '' OR SOURCE_FILE = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET DATE_FILE = NULL WHERE TRIM(DATE_FILE) = '' OR DATE_FILE = 'NaN';
UPDATE UNIDADES_ECONOMICAS_STG SET CATEGORIA = NULL WHERE TRIM(CATEGORIA) = '' OR CATEGORIA = 'NaN';


-- ID, CATEGORIA, DATE_FILE
INSERT INTO UNIDADES_ECONOMICAS(ID, CLEE, NOM_ESTAB, RAZ_SOCIAL, CODIGO_ACT, NOMBRE_ACT, PER_OCU, TIPO_VIAL, NOM_VIAL, TIPO_V_E_1, NOM_V_E_1, TIPO_V_E_2, NOM_V_E_2, TIPO_V_E_3, NOM_V_E_3, NUMERO_EXT, LETRA_EXT, EDIFICIO, EDIFICIO_E, NUMERO_INT, LETRA_INT, TIPO_ASENT, NOMB_ASENT, TIPOCENCOM, NOM_CENCOM, NUM_LOCAL, COD_POSTAL, CVE_ENT, ENTIDAD, CVE_MUN, MUNICIPIO, CVE_LOC, LOCALIDAD, AGEB, MANZANA, TELEFONO, CORREOELEC, WWW, TIPOUNIECO, LATITUD, LONGITUD, FECHA_ALTA, SOURCE_FILE, DATE_FILE, CATEGORIA)
SELECT
    ID, CLEE, NOM_ESTAB, RAZ_SOCIAL, CODIGO_ACT, NOMBRE_ACT, PER_OCU, TIPO_VIAL, NOM_VIAL, TIPO_V_E_1, NOM_V_E_1, TIPO_V_E_2, NOM_V_E_2, TIPO_V_E_3, NOM_V_E_3, NUMERO_EXT, LETRA_EXT, EDIFICIO, EDIFICIO_E, NUMERO_INT, LETRA_INT, TIPO_ASENT, NOMB_ASENT, TIPOCENCOM, NOM_CENCOM, NUM_LOCAL, COD_POSTAL, CVE_ENT, ENTIDAD, CVE_MUN, MUNICIPIO, CVE_LOC, LOCALIDAD, AGEB, MANZANA, TELEFONO, CORREOELEC, WWW, TIPOUNIECO, LATITUD, LONGITUD
    , CASE
        WHEN FECHA_ALTA RLIKE '^\\d{4}-[\\d]{2}$' THEN CONCAT(FECHA_ALTA, '-01')
        WHEN FECHA_ALTA RLIKE '^\\d{4} [\\d]{2}$' THEN CONCAT(REPLACE(FECHA_ALTA, ' ', '-'), '-01')
        ELSE FECHA_ALTA
    END AS FECHA_ALTA
    , SOURCE_FILE, DATE_FILE, CATEGORIA
FROM UNIDADES_ECONOMICAS_STG
WHERE TRUE
    AND ID IS NOT NULL
    AND CLEE IS NOT NULL
    AND SOURCE_FILE IS NOT NULL
    AND DATE_FILE IS NOT NULL
    AND (ID IS NULL OR LENGTH(ID) <= 10)
    AND (CLEE IS NULL OR LENGTH(CLEE) <= 75)
    AND (NOM_ESTAB IS NULL OR LENGTH(NOM_ESTAB) <= 150)
    AND (RAZ_SOCIAL IS NULL OR LENGTH(RAZ_SOCIAL) <= 150)
    AND (CODIGO_ACT IS NULL OR LENGTH(CODIGO_ACT) <= 6)
    AND (NOMBRE_ACT IS NULL OR LENGTH(NOMBRE_ACT) <= 250)
    AND (PER_OCU IS NULL OR LENGTH(PER_OCU) <= 20)
    AND (TIPO_VIAL IS NULL OR LENGTH(TIPO_VIAL) <= 25)
    AND (NOM_VIAL IS NULL OR LENGTH(NOM_VIAL) <= 100)
    AND (TIPO_V_E_1 IS NULL OR LENGTH(TIPO_V_E_1) <= 40)
    AND (NOM_V_E_1 IS NULL OR LENGTH(NOM_V_E_1) <= 100)
    AND (TIPO_V_E_2 IS NULL OR LENGTH(TIPO_V_E_2) <= 40)
    AND (NOM_V_E_2 IS NULL OR LENGTH(NOM_V_E_2) <= 100)
    AND (TIPO_V_E_3 IS NULL OR LENGTH(TIPO_V_E_3) <= 40)
    AND (NOM_V_E_3 IS NULL OR LENGTH(NOM_V_E_3) <= 100)
    AND (NUMERO_EXT IS NULL OR LENGTH(NUMERO_EXT) <= 7)
;


DELETE STG
FROM
    UNIDADES_ECONOMICAS_STG STG
    INNER JOIN UNIDADES_ECONOMICAS PROD
        ON PROD.DATE_FILE = STG.DATE_FILE
        AND PROD.CATEGORIA = STG.CATEGORIA
        AND PROD.SOURCE_FILE = STG.SOURCE_FILE
        AND PROD.ID = STG.ID
;

INSERT INTO UNIDADES_ECONOMICAS_PREPREOCESS
SELECT * FROM UNIDADES_ECONOMICAS_STG;

DROP TABLE UNIDADES_ECONOMICAS_STG;

-- SELECT * FROM UNIDADES_ECONOMICAS_PREPREOCESS;