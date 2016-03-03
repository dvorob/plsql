create or replace FUNCTION STARS_f 
(
  MSISDN IN VARCHAR2 
, PAT IN VARCHAR2
) RETURN VARCHAR2 AS 
BEGIN
  CASE
    WHEN MSISDN=PAT THEN
      RETURN MSISDN;
    ELSE
      RETURN CONCAT(
        SUBSTR(MSISDN,0,LENGTH(MSISDN)-3),
        TRANSLATE(
          SUBSTR(MSISDN,LENGTH(MSISDN)-2,LENGTH(MSISDN)),
          '1'||'2'||'3'||'4'||'5'||'6'||'7'||'8'||'9'||'0',
          '*******************'));
  END CASE;
END STARS_f;