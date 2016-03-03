create or replace PACKAGE PARTITIONING AS 
  
  /* All params are stored in table PARTITION_CONFIG: 
        Name             Null Type         
    ---------------- ---- ------------ 
    TABLE_OWNER           VARCHAR2(50) 
    TABLE_NAME            VARCHAR2(50) 
    TABLE_PREFIX          VARCHAR2(50)  // All objects must be named like '[A-Z]{1,}_'
    KEEP_DAYS             NUMBER        // Before drop
    PARTITION_PERIOD      VARCHAR2(25)  // [MINUTE, HOUR, DAY, MONTH]
    MOVE_AFTER_DAYS       NUMBER        // For shared tables
    TABLESPACE_NEW        VARCHAR2(50) 
    PART_GRAN             NUMBER        // Using with MINUTE partition period for declaring interval (ex: if GRAN=2 partitions will be created each 2 minutes)
    DAYS_AHEAD            NUMBER        // For future dates
    COPY_STATS            VARCHAR2(20)  // [YES, NO] - Copying stats from older partitions to newer
  
  */
  -- Adding new partitions
  PROCEDURE add_partitions;
  -- Drop partition with dates older than KEEP_DAYS
  PROCEDURE drop_partitions;
  -- Moving partitions between tablespaces
  PROCEDURE move_partitions;
  -- Exec auto_task for adding and dropping automaticaly
  PROCEDURE auto_task;

END PARTITIONING;



create or replace PACKAGE BODY PARTITIONING AS

-- Sleep Imimtation --
  PROCEDURE dbms_lock_sleep_imitation
  AS
    cnt NUMBER;
  BEGIN
    SELECT COUNT(*) INTO cnt FROM  (SELECT LEVEL FROM dual CONNECT BY LEVEL < 250000);
  END;
--=====================================================================================================================================================================--

-- Safe execute of long operations -- 
  FUNCTION safe_exec_long_op (v_mod IN VARCHAR2, v_str IN VARCHAR2, v_tries IN number default 1000) return number
  AS
    resource_busy EXCEPTION;
    PRAGMA EXCEPTION_INIT(resource_busy, -54);
    var_exc VARCHAR2(2000);
    cnt_err NUMBER := 0;
    var_MaxBusyErr CONSTANT NUMBER := v_tries;
  BEGIN
    -- Try execute until table is free (but only var_MaxBusyErr times)
    WHILE (cnt_err < var_MaxBusyErr) LOOP
      BEGIN
        EXECUTE IMMEDIATE v_str;
        RETURN 0;

      EXCEPTION
        -- When table is locked go to sleep
        WHEN resource_busy THEN
          cnt_err := cnt_err + 1;
          dbms_lock_sleep_imitation;
        -- When other errors was happened 
        WHEN OTHERS THEN
          cnt_err := var_MaxBusyErr;
          var_exc := sqlerrm;
          INSERT INTO tm_exceptions (module, errm, ts) VALUES(v_mod, var_exc, sysdate);
          COMMIT;

          RETURN -2;
      END;
    END LOOP;

    -- If too many busy errors 
    INSERT INTO tm_exceptions (module,errm,ts) VALUES(v_mod, 'too many busy errors', sysdate);
    COMMIT;
    RETURN -1;
  END;
--============================================================================--

--------------------------------------------------------------------------------
--                               Add partitions
--------------------------------------------------------------------------------
  PROCEDURE add_partitions
  AS
    str VARCHAR2(512);
    str2 VARCHAR2(512);
    tblsp_count NUMBER;
    currDt DATE;
    var_exc VARCHAR2(2000);
    resource_busy EXCEPTION;
    PRAGMA EXCEPTION_INIT(resource_busy, -54);
    isOK NUMBER(1);
    var_MaxBusyErr CONSTANT NUMBER := 100;
    partsrc VARCHAR2(50);
  BEGIN
    DBMS_OUTPUT.ENABLE (buffer_size => NULL);
    FOR tbls IN
    (
        -- Table with parameters to do --
        SELECT distinct cfg.table_owner, cfg.table_name, table_prefix, cfg.part_gran, cfg.partition_period, cfg.days_ahead, cfg.copy_stats,
              CASE 
                WHEN cfg.partition_period='MONTH' THEN
                  CASE WHEN INSTR(partition_name, 'MAX')>0 THEN to_char(ADD_MONTHS(sysdate,1), 'yyyymm')
                    ELSE to_char(ADD_MONTHS(to_date(SUBSTR(partition_name, INSTR(partition_name, '_', -1)+1), 'yyyymm'), 1), 'yyyymm')
                  END
                WHEN cfg.partition_period='DAY' THEN
                  CASE WHEN INSTR(partition_name, 'MAX')>0 THEN to_char(sysdate+1, 'yyyymmdd')
                    ELSE to_char(to_date(SUBSTR(partition_name, INSTR(partition_name, '_')+1), 'yyyymmdd')+1, 'yyyymmdd')
                  END
                WHEN cfg.partition_period='HOUR' THEN
                  CASE WHEN INSTR(partition_name, 'MAX')>0 THEN to_char(sysdate+1/24, 'yyyymmdd_hh24')
                    ELSE to_char(to_date(SUBSTR(partition_name, INSTR(partition_name, '_')+1), 'yyyymmdd_hh24')+1/24, 'yyyymmdd_hh24')
                  END
                WHEN cfg.partition_period='MINUTE' THEN
                  CASE WHEN INSTR(partition_name, 'MAX')>0 THEN to_char(sysdate+1/24, 'yyyymmdd_hh24_mi')
                    ELSE to_char(to_date(SUBSTR(partition_name, INSTR(partition_name, '_')+1), 'yyyymmdd_hh24_mi')+cfg.part_gran/1440, 'yyyymmdd_hh24_mi')
                  END
              END next_date
        FROM main.partition_config cfg INNER JOIN all_tab_partitions p ON p.table_name=cfg.table_name
        WHERE
        ( cfg.table_name, SUBSTR(partition_name, INSTR(partition_name, '_')+1) ) =
          (
            SELECT distinct table_name,
            REPLACE(MAX(REPLACE(SUBSTR(partition_name, INSTR(partition_name, '_')+1), 'MAX', '000')) OVER(PARTITION BY table_name), '000', 'MAX') p
            FROM all_tab_partitions WHERE table_name = p.table_name
          )
    )
    LOOP
    DBMS_OUTPUT.put_line(tbls.table_name);

------------------>  SPLIT PARTITIONS BY MONTH  <------------------
    IF (tbls.partition_period = 'MONTH') THEN
      BEGIN
        currDt := to_date(tbls.next_date, 'yyyymm');
        DBMS_OUTPUT.put_line('Current date: ' || currDt);
        WHILE(currDt <= trunc(sysdate) + tbls.days_ahead) LOOP -- X days ahead from current date
          BEGIN
            isOK := 0;
            str  := 'ALTER TABLE ' || tbls.table_owner || '.' || tbls.table_name || ' SPLIT PARTITION ' || tbls.table_prefix || '_max 
                      AT (to_date(''' || to_char(ADD_MONTHS(currDt, 1), 'yyyymm') || ''', ''yyyymm'')) ' ||
                    ' INTO (PARTITION ' || tbls.table_prefix || '_' || to_char(currDt, 'yyyymm') ||
                         ', PARTITION ' || tbls.table_prefix || '_max)';
            isOK := safe_exec_long_op ('partitioning.add_partitions', str, 100); -- Safe execute of str

            IF (tbls.copy_stats = 'YES') THEN
              partsrc := tbls.table_prefix || '_' || TO_CHAR(ADD_MONTHS(trunc(sysdate), -1), 'yyyymm'); -- One month later
              DBMS_STATS.COPY_TABLE_STATS( tbls.table_owner, tbls.table_name, partsrc, tbls.table_prefix || '_' || to_char(currDt, 'yyyymm'), force => true );
            END IF;
            
            IF(isOK < 0) THEN
              DBMS_OUTPUT.put_line('Exited by safe exec is NOT Ok');
              EXIT;
            END IF;

            currDt := ADD_MONTHS(currDt, 1);
            
          EXCEPTION
            WHEN OTHERS THEN
              var_exc := sqlerrm;
              INSERT INTO tm_exceptions(module, errm, ts) VALUES('partitioning.add_partitions', var_exc, SYSDATE);
              COMMIT;
              EXIT;
          END;
        END LOOP;
      END;
------------------>  SPLIT PARTITIONS BY DAY  <------------------
    ELSIF(tbls.partition_period = 'DAY') THEN
      BEGIN
        currDt := to_date(tbls.next_date, 'yyyymmdd');
        DBMS_OUTPUT.put_line('Current date: ' || currDt);
        
        WHILE(currDt <= trunc(sysdate) + tbls.days_ahead) LOOP -- X days ahead from current date
          BEGIN
            isOK := 0;
            str  := 'ALTER TABLE ' || tbls.table_owner || '.' || tbls.table_name || ' SPLIT PARTITION ' || tbls.table_prefix || '_max 
                      AT (to_date(''' || to_char(currDt + 1, 'yyyymmdd') || ''', ''yyyymmdd'')) ' ||
                    ' INTO (PARTITION ' || tbls.table_prefix || '_' || to_char(currDt, 'yyyymmdd') ||
                         ', PARTITION ' || tbls.table_prefix || '_max)';
            isOK := safe_exec_long_op ('partitioning.add_partitions', str, 100); -- Safe execute of str

            IF(isOK < 0) THEN
              DBMS_OUTPUT.put_line('Exited by safe exec is NOT Ok');
              EXIT;
            END IF;

            IF (tbls.copy_stats = 'YES') THEN
              partsrc := tbls.table_prefix || '_' || TO_CHAR(currDt - (TRUNC(tbls.days_ahead/7)*7 + 7), 'yyyymmdd'); -- One week later
              DBMS_STATS.COPY_TABLE_STATS( tbls.table_owner, tbls.table_name, partsrc, tbls.table_prefix || '_' || to_char(currDt, 'yyyymmdd'), force => true );
            END IF;
            
            currDt := currDt + 1;
            
          EXCEPTION
            WHEN OTHERS THEN
              var_exc := sqlerrm;
              INSERT INTO tm_exceptions(module, errm, ts) VALUES('partitioning.add_partitions', var_exc, SYSDATE);
              COMMIT;
              EXIT;
          END;
        END LOOP;
      END;
------------------>  SPLIT PARTITIONS BY HOUR  <------------------      
    ELSIF(tbls.partition_period = 'HOUR') THEN
      BEGIN
        currDt := to_date(tbls.next_date, 'yyyymmdd_hh24');
        DBMS_OUTPUT.put_line('Current date: ' || currDt);
        
        WHILE(currDt <= trunc(sysdate, 'HH24') + tbls.days_ahead) LOOP  -- X days ahead from current date
          BEGIN
            isOK := 0;
            str  := 'ALTER TABLE ' || tbls.table_owner || '.' || tbls.table_name || ' SPLIT PARTITION ' || tbls.table_prefix || '_max 
                      AT (to_date(''' || TO_CHAR(currDt + 1/24, 'yyyymmdd hh24') || ''', ''yyyymmdd hh24'')) ' ||
                    ' INTO (PARTITION ' || tbls.table_prefix || '_' || to_char(currDt, 'yyyymmdd_hh24') ||
                         ', PARTITION ' || tbls.table_prefix || '_max)';

            isOK := safe_exec_long_op ('partitioning.add_partitions', str, 100);
            
            IF(isOK < 0) THEN
              DBMS_OUTPUT.put_line('Exited by safe exec is NOT Ok');
              EXIT;
            END IF;

            IF (NVL(tbls.copy_stats, 'NO') = 'YES') THEN
              partsrc := tbls.table_prefix || '_' || TO_CHAR(currDt - (TRUNC(tbls.days_ahead/7)*7 + 7), 'yyyymmdd_hh24'); -- One week later
              DBMS_STATS.COPY_TABLE_STATS( tbls.table_owner, tbls.table_name, partsrc, tbls.table_prefix || '_' || to_char(currDt, 'yyyymmdd_hh24'), force => true );
            END IF;
            
            currDt := currDt + 1/24;
  
          EXCEPTION
            WHEN OTHERS THEN
              var_exc := sqlerrm;
              INSERT INTO tm_exceptions(module, errm, ts) VALUES('partitioning.add_partitions', var_exc, SYSDATE);
              COMMIT;
              EXIT;
          END;
        END LOOP;     
      END;
------------------>  SPLIT PARTITIONS BY MINUTES  <------------------  
    ELSIF(tbls.partition_period = 'MINUTE') THEN
      BEGIN
        currDt := to_date(tbls.next_date, 'yyyymmdd_hh24_mi');
        DBMS_OUTPUT.put_line('Current date: ' || currDt);
        WHILE(currDt <= trunc(sysdate, 'HH24') + tbls.days_ahead) LOOP  -- X days ahead from current date
          BEGIN
            isOK := 0;
            str  := 'ALTER TABLE ' || tbls.table_owner || '.' || tbls.table_name || ' SPLIT PARTITION ' || tbls.table_prefix || '_max 
                      AT (to_date(''' || to_char(currDt + tbls.part_gran/1440, 'yyyymmdd hh24:mi') || ''', ''yyyymmdd hh24:mi'')) ' ||
                    ' INTO (PARTITION ' || tbls.table_prefix || '_' || to_char(currDt, 'yyyymmdd_hh24_mi') ||
                         ', PARTITION ' || tbls.table_prefix || '_max)';

            isOK := safe_exec_long_op ('partitioning.add_partitions', str, 100);
            
            IF(isOK < 0) THEN
              DBMS_OUTPUT.put_line('Exited by safe exec is NOT Ok');
              EXIT;
            END IF;
            
            IF (tbls.copy_stats = 'YES') THEN
              partsrc := tbls.table_prefix || '_' || TO_CHAR(currDt - (TRUNC(tbls.days_ahead/7)*7 + 7), 'yyyymmdd_hh24_mi'); -- One week later
              DBMS_STATS.COPY_TABLE_STATS( tbls.table_owner, tbls.table_name, partsrc, tbls.table_prefix || '_' || to_char(currDt, 'yyyymmdd_hh24_mi'), force => true );
            END IF;
            
            currDt := currDt + tbls.part_gran/1440;

          EXCEPTION
            WHEN OTHERS THEN
              var_exc := sqlerrm;
              INSERT INTO tm_exceptions(module, errm, ts) VALUES('partitioning.add_partitions', var_exc, SYSDATE);
              COMMIT;
              EXIT;
          END;
        END LOOP;     
      END;
------------------> END BLOCK <------------------ 
    END IF;
    
    END LOOP;
  END add_partitions;
--============================================================================--

--------------------------------------------------------------------------------
--                               Drop partitions
--------------------------------------------------------------------------------
  PROCEDURE drop_partitions
  AS
    str VARCHAR2(512);
    var_exc VARCHAR2(2000);
    isOK NUMBER(1);
  BEGIN
    DBMS_OUTPUT.ENABLE (buffer_size => NULL);
    FOR i IN
    (
      SELECT table_owner, table_name, partition_name, keep_days, dt, sysdate-keep_days
        FROM
        (
          SELECT cfg.table_owner, parts.table_name, partition_name, cfg.part_gran,
                 CASE WHEN cfg.partition_period='MONTH'   THEN SUBSTR(partition_name, INSTR(partition_name, '_')+1)
                      WHEN cfg.partition_period='DAY'     THEN SUBSTR(partition_name, INSTR(partition_name, '_')+1)               
                      WHEN cfg.partition_period='HOUR'    THEN SUBSTR(partition_name, INSTR(partition_name, '_')+1)
                      WHEN cfg.partition_period='MINUTE'  THEN SUBSTR(partition_name, INSTR(partition_name, '_')+1)
                      ELSE SUBSTR(partition_name, INSTR(partition_name, '_')+1, 8)
                 END dt,
                 cfg.keep_days, cfg.partition_period
          FROM all_tab_partitions parts 
          INNER JOIN main.partition_config cfg 
            ON cfg.table_name=parts.table_name
          WHERE INSTR(partition_name, 'MAX')<1
        )
        WHERE (sysdate-keep_days) > to_date(dt, CASE WHEN partition_period='MONTH'  THEN 'yyyymm'
                                                     WHEN partition_period='DAY'    THEN 'yyyymmdd'
                                                     WHEN partition_period='HOUR'   THEN 'yyyymmdd_hh24'
                                                     WHEN partition_period='MINUTE' THEN 'yyyymmdd_hh24_mi'
                                                END)
    )
    LOOP
      str := 'ALTER TABLE ' || i.table_owner || '.' || i.table_name || ' DROP PARTITION ' || i.PARTITION_name;
      DBMS_OUTPUT.put_line(str);

      isOK := safe_exec_long_op ('partitioning.drop_partitions', str, 100);

      IF(isOK < 0) THEN
        DBMS_OUTPUT.put_line('Exited by safe exec is not Ok');
        EXIT;
      END IF;

    END LOOP;
  END drop_partitions;

--============================================================================--

--------------------------------------------------------------------------------
--                               Move partitions
--------------------------------------------------------------------------------
  PROCEDURE move_partitions
  AS
    str VARCHAR2(512);
    var_exc VARCHAR2(2000);
    var_cnt NUMBER;
    var_timefield VARCHAR2(50);
    var_partname VARCHAR2(50);
    isOK NUMBER(1);
  BEGIN
    DBMS_OUTPUT.ENABLE (buffer_size => NULL);
     
    FOR i IN
    (
      SELECT table_owner, table_name, partition_name, move_after_days, dt, sysdate-move_after_days, tablespace_name, tablespace_new, table_prefix
        FROM
        (
          SELECT cfg.table_owner, parts.table_name, partition_name, cfg.part_gran, cfg.move_after_days, cfg.partition_period, parts.tablespace_name, cfg.tablespace_new, cfg.table_prefix,
                 CASE WHEN cfg.partition_period='MONTH'   THEN SUBSTR(partition_name, INSTR(partition_name, '_')+1)
                      WHEN cfg.partition_period='DAY'     THEN SUBSTR(partition_name, INSTR(partition_name, '_')+1)               
                      WHEN cfg.partition_period='HOUR'    THEN SUBSTR(partition_name, INSTR(partition_name, '_')+1)
                      WHEN cfg.partition_period='MINUTE'  THEN SUBSTR(partition_name, INSTR(partition_name, '_')+1)
                      ELSE SUBSTR(partition_name, INSTR(partition_name, '_')+1, 8)
                 END dt
          FROM all_tab_partitions parts 
          INNER JOIN main.partition_config cfg 
            ON cfg.table_name=parts.table_name
          WHERE INSTR(partition_name, 'MAX')<1
          AND parts.tablespace_name <> cfg.tablespace_new
        )
        WHERE (sysdate-move_after_days) > to_date(dt, CASE  WHEN partition_period='MONTH'  THEN 'yyyymm'
                                                            WHEN partition_period='DAY'    THEN 'yyyymmdd'
                                                            WHEN partition_period='HOUR'   THEN 'yyyymmdd_hh24'
                                                            WHEN partition_period='MINUTE' THEN 'yyyymmdd_hh24_mi'
                                                END)
    )
    LOOP
  -- 0. Check is there old data in _move table (it could happen if last job has fallen)
      str := 'SELECT count(*) FROM ' || i.table_owner || '.' || i.table_name || '_move';
      EXECUTE IMMEDIATE str INTO var_cnt;
      IF (var_cnt > 0 AND substr(i.table_name, 1, 3) = 'CEM') THEN
          IF i.table_name    = 'CEM_DATA' THEN
            var_timefield := 'open_time';
          ELSIF i.table_name = 'CEM_VOICE' THEN
            var_timefield := 'event_date';
          ELSIF i.table_name = 'CEM_SMS' THEN
            var_timefield := 'start_time';
          ELSIF i.table_name = 'CEM_LOCATION' THEN
            var_timefield := 'date_occurrence';
          END IF;

          -- Determining target partition name
          str := 'SELECT ''' || i.table_prefix || ''' || ''_''|| TO_CHAR(' || var_timefield || ', ''yyyymmdd_hh24'') FROM ' || i.table_owner || '.' || i.table_name || '_move WHERE rownum = 1';
          EXECUTE IMMEDIATE str INTO var_partname;
          
          str := 'ALTER TABLE ' || i.table_owner || '.' || i.table_name || ' EXCHANGE PARTITION ' || var_partname || ' WITH TABLE ' || i.table_owner || '.' || i.table_name || '_move including INDEXES';
          isOK := safe_exec_long_op ('partitioning.move_partitions:step#0', str, 100);
          
          -- In case when partition and move table both contain data with similar time we must clear move table
          str := 'TRUNCATE TABLE ' || i.table_owner || '.' || i.table_name || '_move';
          EXECUTE IMMEDIATE str;
      ELSE
        str := 'TRUNCATE TABLE ' || i.table_owner || '.' || i.table_name || '_move';
        EXECUTE IMMEDIATE str;
      END IF;
     
  -- 1. Moving buffer table into fast tablespace and decompressing
      str := 'ALTER TABLE ' || i.table_owner || '.' || i.table_name || '_move MOVE TABLESPACE ' || i.tablespace_new || ' NOCOMPRESS';
      isOK := safe_exec_long_op ('partitioning.move_partitions:step#1', str, 100);
  -- 2. Exchanging partition from main table with buffer table (so both of them will be placed in fast tablespace) 
      str := 'ALTER TABLE ' || i.table_owner || '.' || i.table_name || ' EXCHANGE PARTITION ' || i.partition_name || ' WITH TABLE ' || i.table_owner || '.' || i.table_name || '_move including INDEXES';
      isOK := safe_exec_long_op ('partitioning.move_partitions:step#2', str, 100);
  -- 3. Moving buffer table to slow tablespace and compress
      str := 'ALTER TABLE ' || i.table_owner || '.' || i.table_name || '_move MOVE TABLESPACE ' || i.tablespace_new || ' COMPRESS PARALLEL 10';   -- UNUSABLE INDEX
      isOK := safe_exec_long_op ('partitioning.move_partitions:step#3', str, 100);
        -- 3.1. Rebuild indexes on buffer table
            FOR ind IN (
              SELECT * FROM all_indexes WHERE table_name = i.table_name || '_MOVE'
            )
            LOOP
               str := 'ALTER INDEX ' || ind.owner || '.' || ind.index_name || ' REBUILD parallel 10 online';
               execute immediate str;
            END LOOP;
  -- 4. Move empty partition of main table to slow tablespace and compress
      str := 'ALTER TABLE ' || i.table_owner || '.' || i.table_name || ' MOVE PARTITION ' || i.partition_name || ' TABLESPACE ' || i.tablespace_new || ' COMPRESS';
      isOK := safe_exec_long_op ('partitioning.move_partitions:step#4', str, 100);
  -- 5. Exchange partition with buffer table (now both of them in slow tablespace)
      str := 'ALTER TABLE ' || i.table_owner || '.' || i.table_name || ' EXCHANGE PARTITION ' || i.partition_name || ' WITH TABLE ' || i.table_owner || '.' || i.table_name || '_move including INDEXES without validation';
      isOK := safe_exec_long_op ('partitioning.move_partitions:step#5', str, 100);
  -- 6. Clear move table from records which could get into partition during moving (especially for cem_data table)
      str := 'TRUNCATE TABLE ' || i.table_owner || '.' || i.table_name || '_move';
      EXECUTE IMMEDIATE str;
      IF(isOK < 0) THEN
        DBMS_OUTPUT.put_line('Exited by safe exec is not Ok');
        EXIT;

      END IF;
    END LOOP;
    
  END move_partitions;

--============================================================================--

--------------------------------------------------------------------------------
--                      Procedure for auto tasking
--------------------------------------------------------------------------------
  PROCEDURE auto_task
  AS
    var_exc VARCHAR2(2000);
  BEGIN
    BEGIN
      add_partitions;
    EXCEPTION
      WHEN OTHERS THEN var_exc:=sqlerrm; INSERT INTO TM_EXCEPTIONS VALUES('partitioning.add_partitions', var_exc, sysdate); COMMIT;
    END;
    
    BEGIN
      drop_partitions;
    EXCEPTION
      WHEN OTHERS THEN var_exc:=sqlerrm; INSERT INTO TM_EXCEPTIONS VALUES('partitioning.drop_partitions', var_exc, sysdate); COMMIT;
    END;

    BEGIN
      move_partitions;
    EXCEPTION
      WHEN OTHERS THEN var_exc:=sqlerrm; INSERT INTO TM_EXCEPTIONS VALUES('partitioning.move_partitions', var_exc, sysdate); COMMIT;
    END;
    
    EVENTS_LOG_INSERT_P(TO_CHAR(TRUNC(sysdate, 'DDD'), 'dd.mm.yyyy hh24:mi'), 'PARTITIONING', 'DAY', 0);
    
  END auto_task;
    
END partitioning;