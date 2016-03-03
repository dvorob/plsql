/* User types 
create or replace TYPE ROWGETVOICE AS OBJECT 
( 
  "id" NUMBER, 
  msc VARCHAR2(24), 
  msisdn VARCHAR2(24), 
  prev_b_num VARCHAR2(24),
  rec_type NUMBER(2,0),
  "start" DATE,
  "end" DATE,
  imei VARCHAR2(40), 
  imsi VARCHAR2(40),
  model VARCHAR2(80), 
  duration VARCHAR2(10), 
  called_number VARCHAR2(24), 
  number_c VARCHAR2(24), 
  cause_for_termination VARCHAR2(10), 
  thelac VARCHAR2(12), \
  cellid VARCHAR2(12), 
  first_lac VARCHAR2(12),
  first_cellid VARCHAR2(12),
  category VARCHAR2(10), 
  longitude VARCHAR2(20), 
  latitude VARCHAR2(20), 
  site_id VARCHAR2(10), 
  bs_type VARCHAR2(15),
  bs_band VARCHAR2(20),
  description_full VARCHAR2(1000), 
  band VARCHAR2(1000), 
  "alarms" VARCHAR2(20), 
  problem_desc_short VARCHAR2(20),
  problem_desc VARCHAR2(200),
  "unix_time" VARCHAR2(15),
  weight NUMBER
)

create or replace TYPE tblgetvoice IS TABLE OF Rowgetvoice;
* /

create or replace FUNCTION GET_VOICE_F(
      v_msisdn    IN VARCHAR2 DEFAULT NULL,
      v_time_from IN VARCHAR2 DEFAULT NULL,
      v_time_to   In Varchar2 Default Null) 
      RETURN tblGetVoice 
  AS
    l_data_table tblGetVoice := tblGetVoice();
BEGIN
  For Curr In (
---------- Select --------
----------- Subquery block
WITH breaks As
    (SELECT 
      NVL(msc, 'Не определен') AS msc,
      NVL(msisdn, 'Не определен') As msisdn,
      "start",
      "start" + DURATION/86400 AS "end",
      NVL(imei, 'Не определен') AS imei,
      NVL(imsi, 'Не определен') AS imsi,
      NVL(SUBSTR(marketing_name,1,79), 'Не определена') AS model,
      duration,
      called_number,
      number_c,
      cause_for_termination,
      NVL(thelac, 00000) AS thelac,
      NVL(cellid, 00000) AS cellid,
      first_lac,
      first_cellid,
      category,
      longitude,
      rec_type,
      latitude,
      site_id,
      NVL(bs_type, '-') AS bs_type,
      NVL(bs_band, '-') AS bs_band, 
      NVL(description_full, 'Отсутствует') as description_full,
      NVL(band, 'Не определен') as band
    FROM
      (SELECT ttb.*,
        CONCAT(CONCAT(SUBSTR(marketing_name, 1,48), ' '), SUBSTR(model_name, 1, 30)) as marketing_name,
        band
      FROM
        (SELECT tta.*,
          TO_CHAR(bp.latitude) latitude,
          TO_CHAR(bp.longitude) longitude,
        bp.site_id,
        bp.bs_type AS bs_type,
        bp.band AS bs_band
          FROM
          (Select /*+ INDEX(cv CV_MSISDN_DATE_IND) */
            cv.exchange_id AS msc,
            CASE
              WHEN rec_type <> '3'
              THEN
                CASE
                  WHEN SUBSTR(cv.calling_number,0,1)='7'
                  THEN SUBSTR(cv.calling_number,2)
                  ELSE cv.calling_number
                END
              Else 
                CASE WHEN ORig_Calling_Number Is Null
                     Then Cv.msisdn
                     Else ORig_Calling_Number
                END
            END msisdn,
            cv.Event_Date As "start",
            CASE
              WHEN cv.rec_type = '1'
              THEN cv.calling_imei
              WHEN cv.rec_type = '2'
              THEN cv.called_imei
              WHEN cv.rec_type = '3'
              THEN cv.forwarding_imei
              ELSE cv.calling_imei
            END AS imei,
            CASE
              WHEN cv.rec_type = '1'
              THEN cv.calling_imsi
              WHEN cv.rec_type = '2'
              THEN cv.called_imsi
              WHEN cv.rec_type = '3'
              THEN cv.forwarding_imsi
              ELSE cv.calling_imsi
            END AS imsi,
            CASE
              WHEN cv.mcz_duration IS NULL
              THEN NVL(cv.az_duration,0)
              ELSE NVL(cv.mcz_duration,0)
            END duration,
            CASE
              WHEN cv.cellid IS NULL
              THEN decode(cv.forwarding_first_ci, 65535, 0, cv.forwarding_first_ci)
              ELSE cv.cellid
            END cellid,
            CASE
              WHEN cv.lac IS NULL
              THEN decode(cv.forwarding_first_lac, 65535, 0, cv.forwarding_first_lac)
              ELSE cv.lac
            END thelac,
            calling_subs_first_ci as first_cellid,
            calling_subs_first_lac as first_lac,
            CASE
              WHEN rec_type <> '3'
              THEN
                CASE
                  WHEN SUBSTR(called_number,0,1)='7'
                  THEN SUBSTR(called_number,2)
                  ELSE called_number
                END
              ELSE forwarding_number
            END called_number,
            CASE
              WHEN rec_type = '3'
              THEN forwarded_to_number
              ELSE '-'
            END number_c,
            cv.cause_for_termination,
            cv.rec_type,
            ROW_NUMBER() OVER(PARTITION BY cv.EVENT_DATE ORDER BY cv.exchange_id ASC) AS rn,
            mcc.category,
            MCC.description_full
          FROM voice.cem_voice cv
          LEFT OUTER JOIN main.cem_clearcodes mcc
          ON Cv.Cause_For_Termination = Mcc.Clearcode
          Where Cv.msisdn             = V_MSISDN
          AND cv.event_date BETWEEN to_date(V_TIME_FROM, 'dd.mm.yyyy hh24:mi:ss') AND to_date(V_TIME_TO, 'dd.mm.yyyy hh24:mi:ss')
          ) tta
        LEFT OUTER JOIN main.bts_ll bp
        ON tta.cellid  = bp.cell_id
        AND Tta.Thelac = Bp.Lac
        WHERE tta.rn  <> 2
        AND SUBSTR(called_number,0,1) <> 'B'
        ) ttb
      LEFT OUTER JOIN main.tac
      ON SUBSTR(imei,1,8)=tac.tac
      )
    ORDER BY "start" DESC
    )
----------- Subquery block END
SELECT  rnum, rec_type, "id", msc, msisdn, prev_b_num, "start", "end", imei, imsi, MODEL, DURATION, called_number, number_c, cause_for_termination, thelac, cellid, first_lac, first_cellid, category, longitude, latitude, site_id, bs_band, bs_type, description_full, band, "alarms", problem_desc_short, problem_desc, "unix_time", 
        DECODE(rownum,1,10000,TRUNC(TO_NUMBER(("start" - fw)*60*60*24),0)) AS weight FROM (
SELECT  first_value("end") OVER (ORDER BY "end" ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) fw, 
        first_value(decode(rec_type,1,called_number,2,msisdn,3,number_c,11,msisdn,12,called_number,called_number)) OVER (ORDER BY "start" ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as prev_b_num,
        rnum, rec_type, "id", msc, msisdn, "start", "end", imei, imsi, MODEL, DURATION, called_number, number_c, cause_for_termination, thelac, cellid, first_lac, first_cellid, category, longitude, latitude, site_id, bs_band, bs_type, description_full, band, "alarms", problem_desc_short, problem_desc, ROUND(("start"-TO_DATE('01011970','ddmmyyyy'))*24*60*60) AS "unix_time"
   FROM 
  (
    ---------- Grouping by row_number() AND joining Alarms toggle
    SELECT ROWNUM AS "id", NVL(tsd.problem_desc_short, '-') as problem_desc_short, tsd.problem_desc, breaks.*, 
    ROW_NUMBER() OVER(PARTITION BY breaks."start" ORDER BY breaks.rec_type ASC, breaks."start" ASC) AS rnum,
    first_value(breaks."end") OVER (ORDER BY "end" ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FV,
    last_value(breaks."start") OVER (ORDER BY "start" ASC ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) LV,
     CASE
        WHEN ta.site_id IS NULL
        THEN 'Нет'
        ELSE 'Авария'
      END "alarms"
    FROM breaks
    LEFT JOIN
      (
        SELECT 
          ba.site_id,
          ba.alarm_time_start,
          CASE
            WHEN ba.prdv_time_stop_webmap IS NULL
            THEN ba.alarm_time_stop
            ELSE 
              CASE  WHEN ba.alarm_time_stop IS NULL
                THEN ba.prdv_time_stop_webmap
              ELSE
                ba.alarm_time_stop
              END
          END alarm_time_stop
      FROM main.bts_alarms ba
      LEFT JOIN breaks
      ON TO_CHAR(ba.Site_Id) = TO_CHAR(breaks.site_id)
      ) ta 
      ON TO_CHAR(breaks.site_id) = TO_CHAR(ta.site_id)
       AND (((breaks."end") BETWEEN ta.alarm_time_start AND ta.alarm_time_stop)
       OR (breaks."start" BETWEEN ta.alarm_time_start AND ta.alarm_time_stop))      
    LEFT OUTER JOIN main.ts_sars Ts
        ON TO_CHAR(breaks.site_id) = TO_CHAR(ts.site)
    LEFT OUTER JOIN main.ts_sars_dict tsd
        ON NVL(ts.problem_index, 0) = tsd.problem_index
    )  
---------- Duplicates filtration
WHERE rnum =1     -- Убирает дубли с одинаковым EVENT_DATE
  AND NOT (rec_type IN (11, 12) 
      AND (TO_NUMBER(("start" - FV)*60*60*24) <= 5
       OR (TO_NUMBER((lv - "end")*60*60*24) <= 2 AND DURATION < 5)
       OR (TO_NUMBER((lv - "start")*60*60*24) <= 5 AND DURATION > 5)
       ))   -- Убирает записи, начавшиеся прежде, чем закрылась предыдущая запись. Убирает плечи POC и PTC из вызова.
  AND Not (TO_NUMBER(("start" - Fv)*60*60*24) <= 5 AND rec_type = 17)   -- Убирает дубли при неуспешном вызове, когда записи формируются несколькими коммутаторами
  OR (ROWNUM = 1 AND rec_type NOT IN (11, 12) AND LV - "end" >= 0)      -- Возвращает первую запись в выборке (иначе она вылетит по предыдущему условию)
ORDER BY "start"
)
      ---------- Select ending --------
 )
      LOOP
        L_Data_Table.Extend;
        L_Data_Table(L_Data_Table.Count) := (Rowgetvoice(Curr."id", Curr.msc, Curr.msisdn, Curr.prev_b_num, curr.rec_type, Curr."start", Curr."end", Curr.Imei, Curr.imsi, Curr.Model, Curr.Duration, Curr.Called_Number, Curr.Number_C, Curr.Cause_For_Termination, Curr.Thelac, Curr.Cellid, Curr.First_Lac, Curr.First_Cellid, Curr.Category, Curr.Longitude, Curr.Latitude, Curr.site_id, Curr.bs_type, Curr.bs_band, Curr.Description_Full, Curr.Band, Curr."alarms", Curr.problem_desc_short, Curr.problem_desc, Curr."unix_time", Curr.Weight)) ;
      END LOOP;
    RETURN L_Data_Table;
END GET_VOICE_F;