create or replace PACKAGE     FORBS AUTHID CURRENT_USER IS
  
  /* Package is used to agregating statistics of users activities in the context of unique Base Stations each 3 hours.
  It contains 3 block of statistics: VLR, MSC CDR, SGW/PGW CDR, all of which are compiled into one table:
      Name            Null Type         
    --------------- ---- ------------ 
    REPORT_TIME          DATE         
    BRANCH               VARCHAR2(20) 
    SITE_ID              NUMBER       
    LAC                  NUMBER       
    CELLID               NUMBER       
    BS_TYPE              VARCHAR2(4)  
    LATITUDE             NUMBER       
    LONGITUDE            NUMBER       
    VLR_COUNT            NUMBER       
    DATA_COUNT           NUMBER       
    VOICE_COUNT          NUMBER       
    OLD_SUBS_COUNT       NUMBER       
    PHONES               NUMBER       
    SMARTPHONES          NUMBER       
    MODEMS               NUMBER       
    ROUTERS              NUMBER       
    TABLETS              NUMBER       
    DATA_CARDS           NUMBER       
    NETWORK_DEVICES      NUMBER       
    LAPTOPS              NUMBER       
    OTHERS               NUMBER
    
  All statistics jobs are scheduled by stack-like mechanism which repeatedly catch exec_string from cem_task table and executing this immediatly:
  
      Name        Null Type          
    ----------- ---- ------------- 
    REPORT_TIME      DATE          
    REPORT_NAME      VARCHAR2(40)  
    JOB              VARCHAR2(40)  
    CALLED_PROC      VARCHAR2(40)  
    STATUS           VARCHAR2(20)  
    START_TIME       DATE          
    STOP_TIME        DATE          
    EXEC_STRING      VARCHAR2(200) 
    COMMENTS         VARCHAR2(200) 
    FLAG             NUMBER

  */ 
  
  /* Procedures for VLR stats collecting */ 
  PROCEDURE LOCATION_REPORT_PROC (V_DATE_FROM IN VARCHAR2, V_DATE_TO IN VARCHAR2);
  --
  PROCEDURE LOCATION_MANUAL (V_START_TIME IN VARCHAR2, V_STOP_TIME IN VARCHAR2);
  --
  PROCEDURE LOCATION_FOR_3_HOURS;

  /* Procedures for SGW/PGW CDR stats collecting */ 
  PROCEDURE DATA_REPORT_PROC (V_DATE_FROM IN VARCHAR2, V_DATE_TO IN VARCHAR2);
  --
  PROCEDURE DATA_MANUAL (V_START_TIME IN VARCHAR2, V_STOP_TIME IN VARCHAR2);
  --
  PROCEDURE DATA_FOR_3_HOURS;

  /* Procedures for MSC CDR stats collecting */ 
  PROCEDURE VOICE_REPORT_PROC (V_DATE_FROM IN VARCHAR2, V_DATE_TO IN VARCHAR2);
  --
  PROCEDURE VOICE_MANUAL (V_START_TIME IN VARCHAR2, V_STOP_TIME IN VARCHAR2);
  --
  PROCEDURE VOICE_FOR_3_HOURS;
  
  /* Utilities */
  -- Create main report in VLR_REPORT table --
  PROCEDURE REPORT_COLLECT (V_REPORT_TIME IN VARCHAR2);
  -- Create 3 reports - in vlr, data and voice databases and put ticket into CEM_TASK -
  PROCEDURE REPORT_MANUAL (V_START_TIME IN VARCHAR2, V_STOP_TIME IN VARCHAR2);
  -- Collect whole report for 3 hours
  PROCEDURE REPORT_COLLECT_FOR_3_HOURS;
  -- Filling holes in reports, slightly intelligent 
  PROCEDURE REPORT_HOLES(v_start IN VARCHAR2, v_end IN VARCHAR2, v_step IN NUMBER, recollect IN NUMBER);
  -- Delete reports from all bases and clear task  --  
  PROCEDURE DELETE_ALL_REPORTS (V_REPORT_TIME IN VARCHAR2);
  -- Creating string in task table for scheduling one new report job
  PROCEDURE JOB_CREATE_FORBS(V_REPORT_TIME IN VARCHAR2, 
              V_REPORT_NAME IN VARCHAR2,
              V_JOB_INITIATOR IN VARCHAR2,
              V_CALLED_PROC IN VARCHAR2, 
              V_STATUS IN VARCHAR2, 
              V_START_TIME IN VARCHAR2, 
              V_STOP_TIME IN VARCHAR2,
              V_COMMENTS IN VARCHAR2,
              V_FLAG IN NUMBER
              );
  
END FORBS;



create or replace PACKAGE BODY     FORBS AS

  ------------------------------------------------------------------------------
  --           1. Locations from VLR
  ------------------------------------------------------------------------------
  -- 1.1. Location report proc
  
  PROCEDURE     LOCATION_REPORT_PROC (V_DATE_FROM IN VARCHAR2, V_DATE_TO IN VARCHAR2) AS
    STR VARCHAR2(10000);
    COUNTER Number;
  BEGIN
      dbms_output.enable(buffer_size => NULL);
      str := q'[
        TRUNCATE TABLE report.forbs_location_report_buf
      ]';
      EXECUTE IMMEDIATE str;
      str := q'[
      INSERT INTO report.forbs_location_report
      SELECT TO_DATE(']' || V_DATE_TO || q'[', 'dd.mm.yyyy hh24:mi') AS report_time,
              vlr.lac, 
              vlr.cellid, 
              vlr.Phones,
              vlr.Smartphones,
              vlr.Modems,
              vlr.Routers,
              vlr.Tablets,
              vlr.Network_devices,
              vlr.Data_cards,
              vlr.Laptops,
              vlr.Others,
              vlr.vlr_count, 
              0 as old_subs_count
              FROM  ( 
                    SELECT /*+ PARALLEL(4) */ lac, cellid,
                          SUM( DECODE(device_type, 'Phone', 1,  0) ) AS Phones, 
                          SUM( DECODE(device_type, 'Smartphone', 1, 'SmartPhone', 1, 'Mobile Phone/Feature phone', 1, 0) ) AS Smartphones,
                          SUM( DECODE(device_type, 'USB modem', 1, 'Modem', 1, 0) ) AS Modems,
                          SUM( DECODE(device_type, 'WLAN Router', 1, 'Router', 1, 0) ) AS Routers,
                          SUM( DECODE(device_type, 'Tablet', 1, 0) ) AS Tablets,
                          SUM( DECODE(device_type, 'Network device', 1, 0) ) AS Network_devices,
                          SUM( DECODE(device_type, 'Data card', 1, 0) ) as Data_cards,
                          SUM( DECODE(device_type, 'Laptop', 1, 0) ) as Laptops,
                          SUM( DECODE(device_type, 'Phone', 0, 'Smartphone', 0, 'SmartPhone', 0, 'Mobile Phone/Feature phone', 0, 'Data card', 0, 'USB modem', 0, 'Modem', 0, 'WLAN Router', 0, 'Router', 0, 'Network device', 0, 'Tablet', 0, 'Laptop', 0, 1) ) as Others,
                          COUNT(msisdn) as vlr_count
                          FROM
                          (SELECT xr2.*, device_type
                            FROM 
                            (
                              SELECT *
                              FROM 
                              (
                                SELECT 
                                    lac,
                                    cellid,
                                    msisdn,
                                    imei,
                                    Row_Number() Over(Partition By Msisdn Order By Date_Occurrence Desc) as rn
                                    FROM voice.cem_location xr
                                    WHERE date_occurrence >= TO_DATE(']' || V_DATE_FROM || q'[', 'dd.mm.yyyy hh24:mi') 
                                      AND date_occurrence <  TO_DATE(']' || V_DATE_TO || q'[', 'dd.mm.yyyy hh24:mi')    
                              )
                              WHERE rn =1
                            ) xr2
                            LEFT JOIN 
                              (select /*+ INDEX(ctt TAC_TAC_IND) */ tac, device_type from main.tac ctt) ct
                            ON SUBSTR(xr2.imei,1,8) = ct.tac
                          )
                          GROUP BY lac, cellid
              ) vlr]';
      dbms_output.put_line(str);
      EXECUTE IMMEDIATE str;
      
  END;
  
  -- 1.2. For manual report collecting -----------------------------------------
  PROCEDURE     LOCATION_MANUAL (V_START_TIME IN VARCHAR2, V_STOP_TIME IN VARCHAR2) AS 
    JOB_TIME_BEGIN VARCHAR2(50);
    JOB_TIME_END VARCHAR2(50);
  Begin
    execute immediate 'alter session set nls_date_format=''dd.mm.yyyy hh24:mi'' ';
    JOB_TIME_BEGIN := TO_CHAR(sysdate);
    FORBS.LOCATION_REPORT_PROC(V_START_TIME, V_STOP_TIME);
    JOB_TIME_END := TO_CHAR(sysdate);
    FORBS.JOB_CREATE_FORBS(V_STOP_TIME, 'FORBS_REPORT', '-', '-', 'WAIT', JOB_TIME_BEGIN, JOB_TIME_END, '1 - data, 10 - vlr, 100 - voice', 10);
  END LOCATION_MANUAL;
  
  -- 1.3. For scheduling -------------------------------------------------------
  PROCEDURE     LOCATION_FOR_3_HOURS AS 
    V_START_TIME VARCHAR2(50);
    V_STOP_TIME VARCHAR2(50);
  Begin
    execute immediate 'alter session set nls_date_format=''dd.mm.yyyy hh24:mi'' ';
    V_START_TIME := To_Char(Trunc(Sysdate, 'HH24') - 3/24);
    V_STOP_TIME := To_Char(Trunc(Sysdate, 'HH24'));
    FORBS.LOCATION_REPORT_PROC(V_START_TIME, V_STOP_TIME);
    FORBS.JOB_CREATE_FORBS(V_STOP_TIME, 'FORBS_REPORT', '-', '-', 'WAIT', V_START_TIME, V_STOP_TIME, '1 - data, 10 - vlr, 100 - voice', 10);
  END LOCATION_FOR_3_HOURS;
  
  ------------------------------------------------------------------------------
  --           2. Data from P-GW/S-GW CDRs
  ------------------------------------------------------------------------------
  -- 2.1. Data report proc
  PROCEDURE DATA_REPORT_PROC (V_DATE_FROM IN VARCHAR2, V_DATE_TO IN VARCHAR2) AS
    STR VARCHAR2(10000);
    COUNTER Number;
  BEGIN
      str := q'[
        INSERT INTO report.forbs_data_report
        SELECT  TO_DATE(']' || V_DATE_TO || q'[', 'dd.mm.yyyy hh24:mi') as report_time,
                lac,
                cellid,
                data_count
        FROM (  
          WITH temp_t AS (           
            SELECT MAX(open_time) as max_time, 
                   msisdn 
            FROM data.cem_data
            WHERE open_time >= TO_DATE(']' || V_DATE_FROM || q'[', 'dd.mm.yyyy hh24:mi') 
              AND open_time <  TO_DATE(']' || V_DATE_TO || q'[', 'dd.mm.yyyy hh24:mi')
            GROUP BY msisdn
          )
          SELECT  lac, cellid,
                count(msisdn) as data_count 
          FROM 
          (
            SELECT distinct dr.lac, dr.cellid, dr.msisdn 
            FROM 
            (
                SELECT  
                      t1.lac,
                      t1.cellid,
                      t1.msisdn,
                      t1.open_time
                FROM data.cem_data t1
            ) dr
            INNER JOIN temp_t 
            ON dr.msisdn = temp_t.msisdn
            AND dr.open_time = temp_t.max_time
          )  
          GROUP BY lac, cellid
        )]';
      
      EXECUTE IMMEDIATE str;
      COMMIT;
  END;
  
  -- 2.2. For manual report collecting -----------------------------------------
  PROCEDURE DATA_MANUAL (V_START_TIME IN VARCHAR2, V_STOP_TIME IN VARCHAR2) AS 
    JOB_TIME_BEGIN VARCHAR2(50);
    JOB_TIME_END VARCHAR2(50);
  BEGIN
    execute immediate 'alter session set nls_date_format=''dd.mm.yyyy hh24:mi'' ';
    JOB_TIME_BEGIN := TO_CHAR(sysdate);
    DATA_REPORT_PROC(V_START_TIME, V_STOP_TIME);
    JOB_TIME_END := TO_CHAR(sysdate);
    FORBS.JOB_CREATE_FORBS(V_STOP_TIME, 'FORBS_REPORT', '-', '-', 'WAIT', JOB_TIME_BEGIN, JOB_TIME_END, '1 - data, 10 - vlr, 100 - voice', 1);
  END DATA_MANUAL;
  
  -- 2.3. For scheduling -------------------------------------------------------
  PROCEDURE DATA_FOR_3_HOURS AS 
    V_START_TIME VARCHAR2(50);
    V_STOP_TIME VARCHAR2(50);
  BEGIN
    execute immediate 'alter session set nls_date_format=''dd.mm.yyyy hh24:mi'' ';
    V_START_TIME := To_Char(Trunc(Sysdate, 'HH24') - 3/24);
    V_STOP_TIME := To_Char(Trunc(Sysdate, 'HH24'));
    DATA_REPORT_PROC(V_START_TIME, V_STOP_TIME);
    FORBS.JOB_CREATE_FORBS(V_STOP_TIME, 'FORBS_REPORT', '-', '-', 'WAIT', V_START_TIME, V_STOP_TIME, '1 - data, 10 - vlr, 100 - voice', 1);
  END DATA_FOR_3_HOURS;

  ------------------------------------------------------------------------------
  --           3. Data from MSC CDRs
  ------------------------------------------------------------------------------
  -- 3.1. Voice report proc
  PROCEDURE VOICE_REPORT_PROC (V_DATE_FROM IN VARCHAR2, V_DATE_TO IN VARCHAR2) AS
    STR VARCHAR2(10000);
    COUNTER Number;
  BEGIN
      str := q'[
      INSERT INTO report.forbs_voice_report
      SELECT TO_DATE(']' || V_DATE_TO || q'[', 'dd.mm.yyyy hh24:mi') as report_time,
             voice.lac,
             voice.cellid,
             voice.voice_count 
      FROM
              (
          SELECT lac, cellid, count(msisdn) as voice_count FROM (
              SELECT
                    lac,
                    cellid,
                    msisdn,
                    Row_Number() Over(Partition By Msisdn Order By Event_date Desc) as rn
                    FROM voice.cem_voice
                    WHERE Event_date >= TO_DATE(']' || V_DATE_FROM || q'[', 'dd.mm.yyyy hh24:mi') 
                      AND Event_date < TO_DATE(']' || V_DATE_TO || q'[', 'dd.mm.yyyy hh24:mi')
                      AND rec_type in (1, 2)
          )
          WHERE rn = 1
          GROUP BY lac, cellid
         ) voice]';
      
      EXECUTE IMMEDIATE str;
      COMMIT;
      
  END;

  -- 2.2. For manual report collecting -----------------------------------------
  PROCEDURE VOICE_MANUAL (V_START_TIME IN VARCHAR2, V_STOP_TIME IN VARCHAR2) AS 
    JOB_TIME_BEGIN VARCHAR2(50);
    JOB_TIME_END VARCHAR2(50);
  BEGIN
    execute immediate 'alter session set nls_date_format=''dd.mm.yyyy hh24:mi'' ';
    JOB_TIME_BEGIN := TO_CHAR(sysdate);
    VOICE_REPORT_PROC(V_START_TIME, V_STOP_TIME);
    JOB_TIME_END := TO_CHAR(sysdate);
    FORBS.JOB_CREATE_FORBS(V_STOP_TIME, 'FORBS_REPORT', '-', '-', 'WAIT', JOB_TIME_BEGIN, JOB_TIME_END, '1 - data, 10 - vlr, 100 - voice', 100);
  END VOICE_MANUAL;
  
  -- 2.3. For scheduling -------------------------------------------------------
  PROCEDURE VOICE_FOR_3_HOURS AS 
    V_START_TIME VARCHAR2(50);
    V_STOP_TIME VARCHAR2(50);
  BEGIN
    execute immediate 'alter session set nls_date_format=''dd.mm.yyyy hh24:mi'' ';
    V_START_TIME := To_Char(Trunc(Sysdate, 'HH24') - 3/24);
    V_STOP_TIME := To_Char(Trunc(Sysdate, 'HH24'));
    VOICE_REPORT_PROC(V_START_TIME, V_STOP_TIME);
    FORBS.JOB_CREATE_FORBS(V_STOP_TIME, 'FORBS_REPORT', '-', '-', 'WAIT', V_START_TIME, V_STOP_TIME, '1 - data, 10 - vlr, 100 - voice', 100);
  END VOICE_FOR_3_HOURS;

  ------------------------------------------------------------------------------
  --           4. Common procedures
  ------------------------------------------------------------------------------
  -- 4.1. Create main report in REPORT table --
  PROCEDURE REPORT_COLLECT (V_REPORT_TIME IN VARCHAR2) AS 
    SQL_TEXT VARCHAR2(3000);
  BEGIN
    dbms_output.enable(buffer_size => NULL);
    EXECUTE IMMEDIATE 'alter session set nls_date_format=''dd.mm.yyyy hh24:mi'' ';
  
    sql_text := ' 
    INSERT INTO report.forbs_report
    SELECT  common_bs.common_time, 
            ''СтФ'' AS branch,
            bts.site_id,
            common_bs.lac, 
            common_bs.cellid, 
            bts.bs_type,
            bts.latitude,
            bts.longitude,
            nvl(loc.vlr_count, 0) AS vlr_count,
            nvl(data.data_count, 0) AS data_count, 
            nvl(voice.voice_count, 0) AS voice_count, 
            nvl(loc.old_subs_count, 0) AS old_subs_count,
            nvl(loc.phones, 0) AS phones,
            nvl(loc.smartphones, 0) AS smartphones, 
            nvl(loc.modems, 0) AS modems,
            nvl(loc.routers, 0) AS routers,
            nvl(loc.tablets, 0) AS tablets,
            nvl(loc.data_cards, 0) AS data_cards,
            nvl(loc.network_devices, 0) AS network_devices,
            nvl(loc.laptops, 0) AS laptops,
            nvl(loc.others, 0) AS others
    FROM 
      (
        SELECT to_date(''' || V_REPORT_TIME || ''', ''dd.mm.yyyy hh24:mi'') AS common_time, 
               lac, 
               cellid 
        FROM (
          SELECT TO_NUMBER(lac) AS lac , TO_NUMBER(cellid) AS cellid FROM report.forbs_location_report WHERE report_time = to_date(''' || V_REPORT_TIME || ''', ''dd.mm.yyyy hh24:mi'')
          UNION
          SELECT lac, cellid FROM
          (
            SELECT TO_NUMBER(lac) AS lac, 
                   TO_NUMBER(cellid) AS cellid 
            FROM report.forbs_data_report 
            JOIN 
            (SELECT lac AS la, cell_id AS ci2 FROM main.bts_ll) ta
            ON lac = la AND cellid = ci2
            WHERE report_time = to_date(''' || V_REPORT_TIME || ''', ''dd.mm.yyyy hh24:mi'')
          )
          UNION 
          SELECT TO_NUMBER(lac) AS lac, 
                 TO_NUMBER(cellid) AS ci 
          FROM report.forbs_voice_report 
          WHERE report_time = to_date(''' || V_REPORT_TIME || ''', ''dd.mm.yyyy hh24:mi'')
        ) 
      ) common_bs
      LEFT JOIN 
        (SELECT * FROM report.forbs_location_report WHERE report_time = to_date(''' || V_REPORT_TIME || ''', ''dd.mm.yyyy hh24:mi'')) loc
        ON (common_bs.common_time = loc.report_time AND common_bs.lac = loc.lac AND common_bs.cellid = loc.cellid)
        LEFT JOIN
        (SELECT * FROM report.forbs_data_report WHERE report_time = to_date(''' || V_REPORT_TIME || ''', ''dd.mm.yyyy hh24:mi'')) data
        ON (common_bs.common_time = data.report_time AND common_bs.lac = data.lac AND common_bs.cellid = data.cellid)
        LEFT JOIN 
        (SELECT * FROM report.forbs_voice_report WHERE report_time = to_date(''' || V_REPORT_TIME || ''', ''dd.mm.yyyy hh24:mi'')) voice
        ON (common_bs.common_time = voice.report_time AND common_bs.lac = voice.lac AND common_bs.cellid = voice.cellid)
        LEFT JOIN
        (SELECT site_id, lac, cell_id, bs_type, latitude, longitude FROM main.bts_ll) bts
        ON (common_bs.cellid = bts.cell_id AND common_bs.lac = bts.lac)
    ';
--    dbms_output.put_line(sql_text);
    EXECUTE IMMEDIATE sql_text;
    COMMIT;

  END REPORT_COLLECT;


  -- 4.2. Create 3 reports - in vlr, data and voice databases and put ticket into CEM_TASK --
  PROCEDURE REPORT_MANUAL(V_START_TIME IN VARCHAR2, V_STOP_TIME IN VARCHAR2) AS 
    V_VLR_JOB VARCHAR2(200);
  BEGIN
    EXECUTE IMMEDIATE 'alter session set nls_date_format=''dd.mm.yyyy hh24:mi'' ';
    dbms_scheduler.create_job(job_name => dbms_scheduler.generate_job_name('VB_P1_'),
      job_type => 'PLSQL_BLOCK',
      job_action => 'BEGIN FORBS.location_manual(''' || V_START_TIME  || ''', ''' || V_STOP_TIME || '''); end;',
      comments => 'Thread 1 to refresh employees',
      enabled => true,
      auto_drop => true);
     
    dbms_scheduler.create_job(job_name => dbms_scheduler.generate_job_name('VB_P2_'),
      job_type => 'PLSQL_BLOCK',
      job_action => 'BEGIN FORBS.data_manual(''' || V_START_TIME  || ''', ''' || V_STOP_TIME || '''); end;',
      comments => 'Thread 2 to refresh employees',
      enabled => true,
      auto_drop => true);
     
    dbms_scheduler.create_job(job_name => dbms_scheduler.generate_job_name('VB_P3_'),
      job_type => 'PLSQL_BLOCK',
      job_action => 'BEGIN FORBS.voice_manual(''' || V_START_TIME  || ''', ''' || V_STOP_TIME || '''); end;',
      comments => 'Thread 3 to refresh employees',
      enabled => true,
      auto_drop => true);
     
  END REPORT_MANUAL;
  
  -- 4.3. Delete reports from all tables and clear task  --
  PROCEDURE DELETE_ALL_REPORTS (V_REPORT_TIME IN VARCHAR2) AS 
    SQL_TEXT VARCHAR2(2000);
  BEGIN
    dbms_output.enable(buffer_size => NULL);
    EXECUTE IMMEDIATE 'alter session set nls_date_format=''dd.mm.yyyy hh24:mi'' ';
  
    sql_text := 'DELETE FROM report.forbs_location_report WHERE report_time = to_date(''' || V_REPORT_TIME || ''', ''dd.mm.yyyy hh24:mi'')';
    EXECUTE IMMEDIATE sql_text;
    COMMIT;
    sql_text := 'DELETE FROM report.forbs_data_report WHERE report_time = to_date(''' || V_REPORT_TIME || ''', ''dd.mm.yyyy hh24:mi'')';
    EXECUTE IMMEDIATE sql_text;
    COMMIT;
    sql_text := 'DELETE FROM report.forbs_voice_report WHERE report_time = to_date(''' || V_REPORT_TIME || ''', ''dd.mm.yyyy hh24:mi'')';
    EXECUTE IMMEDIATE sql_text;
    COMMIT;
    sql_text := 'DELETE FROM report.forbs_report WHERE report_time = to_date(''' || V_REPORT_TIME || ''', ''dd.mm.yyyy hh24:mi'')';
    EXECUTE IMMEDIATE sql_text;
    COMMIT;
    sql_text := 'DELETE FROM main.cem_task WHERE report_time = to_date(''' || V_REPORT_TIME || ''', ''dd.mm.yyyy hh24:mi'')';
    EXECUTE IMMEDIATE sql_text;
    COMMIT;

  END DELETE_ALL_REPORTS;

  -- 4.4. Collect whole report for 3 hours
  PROCEDURE REPORT_COLLECT_FOR_3_HOURS AS
    V_START_TIME VARCHAR2(50);
    V_STOP_TIME VARCHAR2(50);
  BEGIN
    execute immediate 'alter session set nls_date_format=''dd.mm.yyyy hh24:mi'' ';
    V_START_TIME := TO_CHAR(TRUNC(Sysdate, 'HH24') - 4/24);
    V_STOP_TIME  := TO_CHAR(TRUNC(Sysdate, 'HH24') - 1/24);
    FORBS.REPORT_MANUAL(V_START_TIME, V_STOP_TIME);
  END REPORT_COLLECT_FOR_3_HOURS;
  
  ------------------------------------------------------------------------------
  --           5. Other tools
  ------------------------------------------------------------------------------
  -- 5.1. Creating job in CEM_TASK 
  PROCEDURE     JOB_CREATE_FORBS (
              V_REPORT_TIME IN VARCHAR2, 
              V_REPORT_NAME IN VARCHAR2,
              V_JOB_INITIATOR IN VARCHAR2,
              V_CALLED_PROC IN VARCHAR2, 
              V_STATUS IN VARCHAR2, 
              V_START_TIME IN VARCHAR2, 
              V_STOP_TIME IN VARCHAR2,
              V_COMMENTS IN VARCHAR2,
              V_FLAG IN NUMBER
              ) AS
      sql_text VARCHAR2(2000);
      V_EXEC_STRING VARCHAR2(200);
  BEGIN
    DBMS_OUTPUT.ENABLE (buffer_size => NULL);
    sql_text := 'alter session set nls_date_format = ''dd.mm.yyyy hh24:mi:ss''';
    V_EXEC_STRING := 'REPORT.FORBS.REPORT_COLLECT(''''' || V_REPORT_TIME || ''''')';
    EXECUTE IMMEDIATE sql_text;
    sql_text := q'[
         MERGE INTO main.cem_task ct
          USING (SELECT TO_DATE(']' || V_REPORT_TIME || ''', ''dd.mm.yyyy hh24:mi'') AS report_time,'''
                     || V_REPORT_NAME || ''' AS report_name,'''       
                     || V_JOB_INITIATOR || ''' AS job_initiator,'''
                     || V_CALLED_PROC || ''' AS called_proc,'''
                     || V_STATUS || ''' AS status, TO_DATE('''   
                     || V_START_TIME || ''', ''dd.mm.yyyy hh24:mi'') AS start_time, TO_DATE('''
                     || V_STOP_TIME || ''', ''dd.mm.yyyy hh24:mi'') AS stop_time,'''
                     || V_EXEC_STRING || ''' AS exec_string,'''
                     || V_COMMENTS || ''' AS comments,'
                     || V_FLAG || q'[ AS flag FROM DUAL) tt
          ON (ct.report_time = tt.report_time AND ct.report_name = tt.report_name)
          WHEN MATCHED THEN
            UPDATE SET ct.status = CASE 
                                      WHEN ct.flag + tt.flag = 111 THEN 'READY'
                                      ELSE 'WAIT'
                                   END,
                       ct.flag = ct.flag + tt.flag
          WHEN NOT MATCHED THEN
            INSERT VALUES (tt.report_time, tt.report_name, tt.job_initiator, tt.called_proc, tt.status, tt.start_time, tt.stop_time, tt.exec_string, tt.comments, tt.flag)]';
    DBMS_OUTPUT.PUT_LINE(sql_text);
    EXECUTE IMMEDIATE sql_text;
    COMMIT;
  END JOB_CREATE_FORBS;
  
  -- 5.2. Filling holes in report
  PROCEDURE report_holes(v_start IN VARCHAR2, v_end IN VARCHAR2, v_step IN NUMBER, recollect IN NUMBER) AS
    v_sql VARCHAR2(1000);
    v_flag NUMBER;
    v_flag_inv NUMBER;
    v_report_time VARCHAR2(30);
    v_first_report VARCHAR2(30);
    v_rep_cnt NUMBER;
    v_ringer NUMBER;
  BEGIN
      execute immediate 'alter session set nls_date_format=''dd.mm.yyyy hh24:mi'' ';
      dbms_output.enable(buffer_size => NULL);
      v_ringer := 300;
      
      v_first_report := TO_CHAR(TRUNC(TO_DATE(v_start, 'dd.mm.yyyy hh24:mi')));
      
      v_rep_cnt := TRUNC((TO_DATE(v_end, 'dd.mm.yyyy hh24:mi') - TO_DATE(v_first_report, 'dd.mm.yyyy hh24:mi'))*24/v_step);
      dbms_output.put_line('First report time: ' || v_first_report);
      dbms_output.put_line('Count of reports: ' || v_rep_cnt);
      
      FOR v_rep_num IN 0..v_rep_cnt LOOP
        v_report_time := SUBSTR(TO_CHAR(TO_DATE(v_first_report, 'dd.mm.yyyy hh24:mi') + (v_step * v_rep_num)/24, 'dd.mm.yyyy hh24:mi'), 1, 16);
        dbms_output.put_line('Processing report at: ' || v_report_time);
        v_sql := 'BEGIN SELECT NULLIF(flag, 0) INTO :v_flag 
                        FROM main.cem_task cct
                        WHERE cct.report_time = 
                              TO_DATE(''' || v_report_time || ''', ''dd.mm.yyyy hh24:mi'');
                  EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                           :v_flag := 0; 
                  END;';
        EXECUTE IMMEDIATE v_sql USING out v_flag;
        IF recollect = 1 THEN
          FORBS.DELETE_ALL_REPORTS(v_report_time);
          dbms_output.put_line('Report was deleted: ' || v_report_time);
          
          FORBS.REPORT_MANUAL(TO_CHAR(TO_DATE(v_report_time, 'dd.mm.yyyy hh24:mi') - v_step/24, 'dd.mm.yyyy hh24:mi'), v_report_time);
          ------------------------------------------------------------------------
          WHILE v_ringer > 0 LOOP
            dbms_output.put_line('Going to sleep');
            dbms_lock.sleep (10);
            EXECUTE IMMEDIATE v_sql USING out v_flag;
            IF v_flag <> 111 THEN
              v_ringer := v_ringer - 1;
            ELSE
              v_ringer := 0;
            END IF;
            dbms_output.put_line('Ringer is: ' ||v_ringer || '    Flag is: ' || v_flag);
          END LOOP;
          v_ringer := 300;
          ------------------------------------------------------------------------
        ELSIF recollect = 0 THEN
        dbms_output.put_line('Flag: ' || v_flag);
  
          IF v_flag = 0 THEN
              dbms_output.put_line('BAD' || ' '|| v_report_time || ' ' || v_flag);
              FORBS.DELETE_ALL_REPORTS(v_report_time);
              FORBS.REPORT_MANUAL(TO_CHAR(TO_DATE(v_report_time, 'dd.mm.yyyy hh24:mi') - v_step/24, 'dd.mm.yyyy hh24:mi'), v_report_time);
            
              ------------------------------------------------------------------------
              WHILE v_ringer > 0 LOOP
                dbms_output.put_line('Going to sleep');
                dbms_lock.sleep (10);
                EXECUTE IMMEDIATE v_sql USING out v_flag;
                IF v_flag <> 111 THEN
                  v_ringer := v_ringer - 1;
                ELSE
                  v_ringer := 0;
                END IF;
                dbms_output.put_line('Ringer is: ' ||v_ringer || '    Flag is: ' || v_flag);
              END LOOP;
              v_ringer := 300;
              ------------------------------------------------------------------------
          ELSIF v_flag <> 0 AND v_flag <> 111 THEN
              v_flag_inv := 111 - v_flag;
              IF v_flag_inv/100 >= 1 THEN 
                dbms_output.put_line('Flag include 100: ' || v_flag_inv);
                FORBS.VOICE_MANUAL(TO_CHAR(TO_DATE(v_report_time, 'dd.mm.yyyy hh24:mi') - v_step/24, 'dd.mm.yyyy hh24:mi'), v_report_time);
                
              END IF;
              IF (v_flag_inv - TRUNC(v_flag_inv, -2))/10 >= 1 THEN
                dbms_output.put_line('Flag include 10: ' || v_flag_inv);
                FORBS.LOCATION_MANUAL(TO_CHAR(TO_DATE(TO_DATE(v_report_time, 'dd.mm.yyyy hh24:mi') - v_step/24), 'dd.mm.yyyy hh24:mi'), v_report_time);
              END IF;
              IF (v_flag_inv - TRUNC(v_flag_inv, -2) - TRUNC(v_flag_inv - TRUNC(v_flag_inv, -2), -1)) >= 1 THEN 
                dbms_output.put_line('Flag include 1: ' || v_flag_inv);
                FORBS.DATA_MANUAL(TO_CHAR(TO_DATE(v_report_time, 'dd.mm.yyyy hh24:mi') - v_step/24, 'dd.mm.yyyy hh24:mi'), v_report_time);
              END IF;
              ------------------------------------------------------------------------
              WHILE v_ringer > 0 LOOP
                dbms_output.put_line('Going to sleep');
                dbms_lock.sleep (10);
                EXECUTE IMMEDIATE v_sql USING out v_flag;
                IF v_flag <> 111 THEN
                  v_ringer := v_ringer - 1;
                ELSE
                  v_ringer := 0;
                END IF;
                dbms_output.put_line('Ringer is: ' ||v_ringer || '    Flag is: ' || v_flag);
              END LOOP;
              v_ringer := 300;
              ------------------------------------------------------------------------
  
              
          ELSE
            dbms_output.put_line('GOOD' || ' '|| v_report_time || ' ' || v_flag);
          END IF;
          
        ELSE
          dbms_output.put_line('Something went wrong');
        END IF;
      END LOOP;
  END report_holes;
  
END FORBS;