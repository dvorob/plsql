create or replace PACKAGE CMS_AGREGATOR_PKG AS 
  
  /* Agregator of separate inquiries to clusters by geographical closeness.
     It is raw and never been used in practice and most interesting here is geographic methods in PLSQL 
  */ 
  
  PROCEDURE cms_agr_sp_p(V_DATE_FROM VARCHAR2, V_DATE_TO VARCHAR2);
  ---
  PROCEDURE cms_agregator;
  ---
  PROCEDURE cms_e_buf_p(V_DATE IN VARCHAR2);
  ---
  PROCEDURE cms_inquiries_buf_p(V_DATE_FROM IN VARCHAR2, V_DATE_TO IN VARCHAR2);


END CMS_AGREGATOR_PKG;



create or replace PACKAGE BODY CMS_AGREGATOR_PKG AS
--------------------------------------------------------------------------------
PROCEDURE cms_agr_sp_p(V_DATE_FROM VARCHAR2, V_DATE_TO VARCHAR2) AS
  BEGIN
 INSERT INTO cms_agr_sp
 SELECT * FROM (
   SELECT 'SP_' || TO_CHAR(tt.site_id) AS sp_id,
          tt.site_id AS site_id_agr,
          cms.site_id AS site_id_cms, 
          cms.inquiry_id,
          cms.latitude,
          cms.longitude,
          cms.registration_date,
          tt.radius,
          tt.distance,
          tt.bearing
     FROM (
        SELECT * FROM cms_inquiries
                  WHERE registration_date >= to_date(V_DATE_FROM, 'dd.mm.yyyy hh24:mi:ss') 
                    AND registration_date <  to_date(V_DATE_TO, 'dd.mm.yyyy hh24:mi:ss')
                    AND inquiry_id NOT IN (SELECT inquiry_id FROM cms_agr_sp)
       ) cms
   LEFT JOIN
    (
      SELECT inquiry_id, site_id, e, radius, distance, bearing FROM (
      SELECT ta.inquiry_id, ta.site_id, ta.radius, ta.distance, ta.bearing, tb.e, 
            row_number() OVER (PARTITION BY inquiry_id ORDER BY e DESC) AS rn
      FROM cms_inquiries_buf ta
      LEFT JOIN cms_e_buf tb
      ON ta.site_id = tb.site_id
      )
      WHERE rn = 1
    ) tt
    ON cms.inquiry_id = tt.inquiry_id
    WHERE e > ( SELECT percentile_cont(0.9) within group (order by e) AS mediana FROM     (  SELECT inquiry_id, site_id, e, radius, distance, bearing FROM (
          SELECT ta.inquiry_id, ta.site_id, ta.radius, ta.distance, ta.bearing, tb.e, 
                row_number() OVER (PARTITION BY inquiry_id ORDER BY e DESC) AS rn
          FROM cms_inquiries_buf ta
          LEFT JOIN cms_e_buf tb
          ON ta.site_id = tb.site_id
          ) WHERE rn = 1))
   );
   END;
   
--------------------------------------------------------------------------------
PROCEDURE cms_agregator
 AS
 BEGIN
 
 DELETE FROM cms_e_buf;
 DELETE FROM cms_inquiries_buf;
 DELETE FROM cms_agr_sp;
 
 INSERT INTO cms_inquiries_buf
  SELECT * 
       FROM  (
       SELECT /*+ USE_HASH(cms, bl)  PARALLEL(bl, 12) */
              cms.inquiry_id, 
              bl.site_id, 
              bl.cell_id, 
              bl.lac, 
              bl.width,
              bl.azimuth,
              bl.AVG_MS_BS_DISTANCE*2 AS radius,
              --- Расстояние между БС и жалобой
              6371 * ACOS(
                (sin(NVL(bl.latitude,0) * 0.0174532925) * SIN(NVL(TO_NUMBER(cms.latitude),0) * 0.0174532925)) +
                (
                  COS(NVL(bl.latitude,0) * 0.0174532925) * COS(NVL(TO_NUMBER(cms.latitude),0) * 0.0174532925) *
                  COS(NVL(TO_NUMBER(cms.longitude),0) * 0.0174532925 - NVL(bl.longitude,0)* 0.0174532925)
                )
              ) AS distance,
              ROUND(sysdate - registration_date) AS age,
              --- Вычисление угла между сектором БС и жалобой
              MOD(sdo_util.convert_unit(
                  (atan2(sin(bl.longitude*sdo_util.convert_unit(1,'Degree','Radian')-TO_NUMBER(cms.longitude)*sdo_util.convert_unit(1,'Degree','Radian'))*cos(bl.latitude*sdo_util.convert_unit(1,'Degree','Radian')),
                  cos(TO_NUMBER(cms.latitude)*sdo_util.convert_unit(1,'Degree','Radian'))*sin(bl.latitude*sdo_util.convert_unit(1,'Degree','Radian'))
                  -sin(TO_NUMBER(cms.latitude)*sdo_util.convert_unit(1,'Degree','Radian'))*cos(bl.latitude*sdo_util.convert_unit(1,'Degree','Radian'))*cos(bl.longitude*sdo_util.convert_unit(1,'Degree','Radian')-
                  TO_NUMBER(cms.longitude)*sdo_util.convert_unit(1,'Degree','Radian'))))                
                  ,'Radian','Degree') + 180, 360) 
              AS bearing
            FROM cms_inquiry_prod_test_2 cms,
                 main.bts_ll bl
            WHERE bl.width > 0
      )
    WHERE radius > distance
      AND ABS(bearing - azimuth) - width/2 < 0;
  
  COMMIT;
 
 MERGE INTO cms_e_buf old_e USING (
    SELECT ta.site_id,
         E*cmyk AS E,
         inquiries
    FROM
    (
      SELECT  /*+ PARALLEL(8) */ site_id, 
             SUM(age / (distance/radius)) AS E,
             count(*) AS inquiries
            -- SUM(age ) AS E
        FROM  cms_inquiries_buf
        GROUP BY site_id
    ) ta
    --- Привязка коэффициента CMYK (поскольку в формуле он стоит в множителе, его можно вынести за скобку, т.е. домножение производить в конце
    LEFT JOIN (
      SELECT bts.site_id,
           NVL(cg.color_priority, 0) AS cmyk
      FROM (SELECT distinct site_id,latitude, longitude FROM  main.bts_ll WHERE length(site_id) <6) bts
      LEFT JOIN (SELECT ta.label, ta.lat1, ta.lat2, ta.lon1, ta.lon2, 
                        tb.color_priority
                   FROM spartacus.cem_grid ta,
                        spartacus.cem_grid_values tb
                  WHERE ta.label = tb.label) cg
          ON   bts.latitude   > cg.lat1 
           AND bts.latitude  <= cg.lat2 
           AND bts.longitude  > cg.lon1 
           AND bts.longitude <= cg.lon2 
    ) tb
    ON ta.site_id = tb.site_id
 ) new_e
  ON (new_e.site_id = old_e.site_id)
  WHEN NOT matched THEN
  INSERT VALUES
  (
    new_e.site_id,
    new_e.E,
    new_e.inquiries
  )
  WHEN MATCHED THEN
  UPDATE
  SET
    old_e.E = old_e.E + new_e.E,
    old_e.inquiries = old_e.inquiries + new_e.inquiries
  ;
 COMMIT;
  
 INSERT INTO cms_agr_sp
   SELECT 'SP_' || TO_CHAR(tt.site_id) AS sp_id,
          tt.site_id AS site_id_agr,
          inq.site_id AS site_id_cms, 
          inq.inquiry_id,
          inq.latitude,
          inq.longitude,
          inq.registration_date
   FROM cms_inquiry_prod_test_2 inq
   LEFT JOIN
    (
      SELECT inquiry_id, site_id, e FROM (
      SELECT ta.inquiry_id, ta.site_id, tb.e, 
            row_number() OVER (PARTITION BY inquiry_id ORDER BY e DESC) AS rn
      FROM cms_inquiries_buf ta
      LEFT JOIN cms_e_buf tb
      ON ta.site_id = tb.site_id
      )
      WHERE rn = 1
    ) tt
    ON inq.inquiry_id = tt.inquiry_id;
  
  COMMIT;
 END;
 
 -------------------------------------------------------------------------------
PROCEDURE cms_e_buf_p(V_DATE IN VARCHAR2) AS
BEGIN
 INSERT INTO cms_e_buf 
    SELECT ta.site_id,
         E AS E,
         inquiries
    FROM
    (
        SELECT  site_id, 
             SUM(pre_e) AS E,
             count(*) AS inquiries
        FROM  
            (
            SELECT site_id, 
                   inquiry_id, 
                   age/(distance/radius) AS pre_e 
            FROM (
              SELECT site_id, inquiry_id, radius, distance, ROUND(to_date(V_DATE, 'dd.mm.yyyy hh24:mi:ss') - registration_date) + 1 AS age, 
                     row_number() over (partition by site_id, inquiry_id order by ( ROUND(to_date(V_DATE, 'dd.mm.yyyy hh24:mi:ss') - registration_date) + 1 )/radius desc) AS rn
                     FROM cms_inquiries_buf
                     WHERE registration_date < to_date(V_DATE, 'dd.mm.yyyy hh24:mi:ss')
                       AND inquiry_id NOT IN (SELECT inquiry_id FROM cms_agr_sp)
             )
             WHERE rn = 1
          )
        GROUP BY site_id
    ) ta
    --- Привязка коэффициента CMYK (поскольку в формуле он стоит в множителе, его можно вынести за скобку, т.е. домножение производить в конце
    LEFT JOIN (
      SELECT * FROM (
        SELECT bts.site_id,
               NVL(cg.color_priority, 0) AS cmyk, 
               row_number() over (partition by bts.site_id order by NVL(cg.color_priority, 0) desc) AS rn
        FROM (SELECT distinct site_id,latitude, longitude FROM  main.bts_ll WHERE length(site_id) <6) bts
        LEFT JOIN (SELECT ta.label, ta.lat1, ta.lat2, ta.lon1, ta.lon2, 
                          tb.color_priority
                     FROM spartacus.cem_grid ta,
                          spartacus.cem_grid_values tb
                    WHERE ta.label = tb.label) cg
            ON   bts.latitude   > cg.lat1 
             AND bts.latitude  <= cg.lat2 
             AND bts.longitude  > cg.lon1 
             AND bts.longitude <= cg.lon2 
        ) WHERE rn = 1
    ) tb
    ON ta.site_id = tb.site_id;
    
  END;
  
-------------------------------------------------------------------------------
PROCEDURE cms_inquiries_buf_p(V_DATE_FROM IN VARCHAR2, V_DATE_TO IN VARCHAR2) 
 AS
 BEGIN
 INSERT INTO lab.cms_inquiries_buf
 SELECT * FROM (
  SELECT inquiry_id, site_id, radius, distance, registration_date, bearing
       FROM  (
       SELECT /*+ USE_HASH(cms, bl)  PARALLEL(bl, 16) */
              cms.inquiry_id, bl.site_id, bl.width, bl.azimuth,
              CASE WHEN bl.AVG_MS_BS_DISTANCE > 2 THEN 4
                   ELSE bl.AVG_MS_BS_DISTANCE*2
              END radius
              --- Расстояние между БС и жалобой
              ,6371 * ACOS(
                (sin(NVL(bl.latitude,0) * 0.0174532925) * SIN(NVL(TO_NUMBER(cms.latitude),0) * 0.0174532925)) +
                (
                  COS(NVL(bl.latitude,0) * 0.0174532925) * COS(NVL(TO_NUMBER(cms.latitude),0) * 0.0174532925) *
                  COS(NVL(TO_NUMBER(cms.longitude),0) * 0.0174532925 - NVL(bl.longitude,0)* 0.0174532925)
                )
              ) AS distance
              ,registration_date
              --- Вычисление угла между сектором БС и жалобой
              ,MOD(sdo_util.convert_unit(
                  (atan2(sin(bl.longitude*sdo_util.convert_unit(1,'Degree','Radian')-TO_NUMBER(cms.longitude)*sdo_util.convert_unit(1,'Degree','Radian'))*cos(bl.latitude*sdo_util.convert_unit(1,'Degree','Radian')),
                  cos(TO_NUMBER(cms.latitude)*sdo_util.convert_unit(1,'Degree','Radian'))*sin(bl.latitude*sdo_util.convert_unit(1,'Degree','Radian'))
                  -sin(TO_NUMBER(cms.latitude)*sdo_util.convert_unit(1,'Degree','Radian'))*cos(bl.latitude*sdo_util.convert_unit(1,'Degree','Radian'))*cos(bl.longitude*sdo_util.convert_unit(1,'Degree','Radian')-
                  TO_NUMBER(cms.longitude)*sdo_util.convert_unit(1,'Degree','Radian'))))                
                  ,'Radian','Degree') + 180, 360)
              AS bearing
            FROM (
                  SELECT * FROM cms_inquiries_test 
                  WHERE registration_date >= to_date(V_DATE_FROM, 'dd.mm.yyyy hh24:mi:ss') 
                    AND registration_date <  to_date(V_DATE_TO, 'dd.mm.yyyy hh24:mi:ss')
                    AND inquiry_id NOT IN (SELECT inquiry_id FROM cms_agr_sp)
                 )cms,
                 bts_ll bl
      )
    WHERE radius >= distance
      AND ABS(MOD(bearing - azimuth + 180, 360) - 180) - width <= 0
  ) ta
  UNION ALL
   SELECT sp.inquiry_id, sp.site_id_agr AS site_id, radius, distance, registration_date, bearing 
   FROM lab.cms_agr_sp sp;
   
END;
END CMS_AGREGATOR_PKG;