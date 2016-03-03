create or replace PACKAGE     LCCN AS 
  
  /* This package is used for aggregating and summarizing calls statistics from huge historical tables */
  
  /* Check status [Ok, null] of all preparations for a whole month from log table lab.lccn_days by field name
      Name                Null Type         
    ------------------- ---- ------------ 
    DDAY                     DATE         
    STATUS_CALLS             VARCHAR2(20) 
    STATUS_ZEROCALLS         VARCHAR2(20) 
    IS_STATS_MERGED          VARCHAR2(20) 
    IS_STATS_TRANSFERED      VARCHAR2(20) 
  */ 
  FUNCTION check_lccn_days (mmonth IN VARCHAR2, field IN VARCHAR2) RETURN NUMBER;
  
  -- Merging stats from zero_calls and calls into buffer table lccn_calls_stats_buf --
  PROCEDURE call_stats_merging_month(mmonth IN VARCHAR2);
  
  -- Inserting all succ and drop calls from lccn_calls_stats_buf into result table in HistDB via DB-Link --
  PROCEDURE calls_stats_transfer(mmonth IN VARCHAR2);
  
  -- Monthly scheduling task
  PROCEDURE stats_monthly;
  
END LCCN;



create or replace PACKAGE BODY LCCN AS

  FUNCTION check_lccn_days (mmonth IN VARCHAR2, field IN VARCHAR2) 
  RETURN number
  IS
    sql_text VARCHAR2(2000);
    status NUMBER;
  BEGIN
    execute immediate 'alter session set nls_date_format=''dd.mm.yyyy hh24:mi'' ';
    DBMS_OUTPUT.ENABLE (buffer_size => NULL);
    sql_text := '
      SELECT SUM(status) FROM 
        (SELECT DECODE(' || field || ', ''Ok'', 0, 1) AS status FROM lab.lccn_days
         WHERE TRUNC(dday, ''MON'') = TRUNC(TO_DATE(''' || mmonth || ''', ''dd.mm.yyyy hh24:mi''), ''MON'')
        )
    ';
    EXECUTE IMMEDIATE sql_text INTO status;
    RETURN status;
  END check_lccn_days;

--------------------------------------------------------------------------------

  PROCEDURE call_stats_merging_month(mmonth IN VARCHAR2) AS
    sql_text VARCHAR2(5000);
  BEGIN
    execute immediate 'alter session set nls_date_format=''dd.mm.yyyy hh24:mi'' ';
    DBMS_OUTPUT.ENABLE (buffer_size => NULL);
    sql_text := '
      INSERT /*+ append */ INTO lab.lccn_calls_stats_buf ( mmonth, msisdn, cellid, lac, succ_calls, drop_calls, zero_calls )
      SELECT mmonth, msisdn, cellid, lac, sum(succ_calls) as succ_calls, sum(drop_calls) as drop_calls, sum(zero_calls) as zero_calls 
      FROM (
          SELECT mmonth, msisdn, cellid, lac, 0 as succ_calls, 0 as drop_calls, zero_calls FROM (
              SELECT TRUNC(TO_DATE(''' || mmonth || ''', ''dd.mm.yyyy hh24:mi''), ''MON'') as mmonth, 
                     direction_number as Msisdn, 
                     cell_id as cellid, 
                     a_lac as lac, 
                     count(direction_number) as zero_calls 
              FROM voice.zero_calls
              WHERE call_start_time >= TRUNC(TO_DATE(''' || mmonth || ''', ''dd.mm.yyyy hh24:mi''), ''MON'') 
                AND call_start_time <  ADD_MONTHS(TRUNC(TO_DATE(''' || mmonth || ''', ''dd.mm.yyyy hh24:mi''), ''MON''), 1)
              GROUP BY direction_number, a_lac, cell_id
          ) zeroes
          UNION ALL
          SELECT 
                 TRUNC(TO_DATE(''' || mmonth || ''', ''dd.mm.yyyy hh24:mi''), ''MON'') AS mmonth,
                 Msisdn,
                 cellid,
                 lac,
                 Succ_Calls,
                 Drop_calls,
                 NULL AS zero_calls
          FROM
          (
                SELECT * FROM
                (
                SELECT /*+ PARALLEL(6) */ 
                      Msisdn, cellid, lac, 
                      COUNT(msisdn) AS succ_calls,
                      0 AS drop_calls
                FROM voice.calls ac
                WHERE Duration > 0
                 AND event_date >= TO_DATE(''' || mmonth || ''', ''dd.mm.yyyy hh24:mi'') 
                 AND event_date <  ADD_MONTHS(TO_DATE(''' || mmonth || ''', ''dd.mm.yyyy hh24:mi''), 1)
                 AND Category  IN (''Ok'', ''error_Sub'', ''error_Other'')
                 AND rec_type  IN (1, 2, 3)
                GROUP BY msisdn,
                        lac,
                        cellid
                ) Succ
                UNION ALL
                SELECT * FROM
                (
                  SELECT /*+ PARALLEL(6) */ 
                      Msisdn, cellid, lac,
                      0 AS succ_calls,
                      COUNT(Msisdn) AS Drop_calls
                  FROM voice.calls ac
                  WHERE Duration > 0
                    AND event_date >= TO_DATE(''' || mmonth || ''', ''dd.mm.yyyy hh24:mi'') 
                    AND event_date <  ADD_MONTHS(TO_DATE(''' || mmonth || ''', ''dd.mm.yyyy hh24:mi''), 1)
                    AND Category  IN (''error_Bss'', ''error_Nss'', ''error_Ext'')
                    AND rec_type  IN (1, 2, 3, 17)
                  GROUP BY Msisdn,
                        lac,
                        cellid
                ) Drops
          )
      ) 
      GROUP BY mmonth, msisdn, cellid, lac
    ';
    dbms_output.put_line(sql_text);
    EXECUTE IMMEDIATE sql_text;
    COMMIT;
    UPDATE lab.lccn_days
      SET is_stats_merged = 'Ok'
      WHERE trunc(dday, 'MON') = TRUNC(TO_DATE(mmonth, 'dd.mm.yyyy hh24:mi'), 'MON');
    COMMIT;
    END call_stats_merging_month;
       
--------------------------------------------------------------------------------

  PROCEDURE calls_stats_transfer(mmonth IN VARCHAR2) AS
    sql_text VARCHAR2(2000);
  BEGIN
    execute immediate 'alter session set nls_date_format=''dd.mm.yyyy hh24:mi'' ';
    DBMS_OUTPUT.ENABLE (buffer_size => NULL);
    sql_text := '
      INSERT /*+ APPEND */ INTO lccn_calls_stats@histdb cs (MMONTH, MSISDN, CELLID, LAC, SUCC_CALLS, DROP_CALLS, ZERO_CALLS)
        SELECT 
               mmonth, 
               msisdn, 
               cellid, 
               lac, 
               succ_calls,
               drop_calls,
               zero_calls
        FROM lab.lccn_calls_stats_buf
      ';
        
      EXECUTE IMMEDIATE sql_text;
      COMMIT;
      
      sql_text := 'TRUNCATE TABLE lab.lccn_calls_stats_buf';
      EXECUTE IMMEDIATE sql_text;
      COMMIT;
        
    UPDATE lab.lccn_days
      SET is_stats_transfered = 'Ok'
      WHERE trunc(dday, 'MON') = TRUNC(TO_DATE(mmonth, 'dd.mm.yyyy hh24:mi'), 'MON');
    COMMIT;
  END calls_stats_transfer;
    
--------------------------------------------------------------------------------

  PROCEDURE stats_monthly
  AS
   V_MONTH VARCHAR2(30);
  BEGIN
    execute immediate 'alter session set nls_date_format=''dd.mm.yyyy hh24:mi'' ';
    V_MONTH := SUBSTR(TO_CHAR( TRUNC(add_months(sysdate, -1), 'MON')),1,16);
    -- Check stats for a month
    IF ( lccn.check_lccn_days(V_MONTH, 'STATUS_CALLS') = 0 AND lccn.check_lccn_days(V_MONTH, 'STATUS_ZEROCALLS') = 0 ) THEN
    -- Calculate and merging stats to LCCN_CALLS_STATS_BUF
      dbms_output.put_line('Calls stats merging');
      lccn.call_stats_merging_month(V_MONTH);
    END IF;
    
    IF ( lccn.check_lccn_days(V_MONTH, 'IS_STATS_MERGED') = 0 ) THEN
      -- Transfering stats to HistDB
      dbms_output.put_line('Stats transfering');
      lccn.calls_stats_transfer(V_MONTH);
    END IF;
  END stats_monthly;
  

END LCCN;