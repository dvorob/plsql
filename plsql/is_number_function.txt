create or replace FUNCTION IS_NUMBER_F( p_str IN VARCHAR2 )
  RETURN VARCHAR2
IS
  l_num NUMBER;
BEGIN
  l_num := to_number( p_str );
  RETURN 'Y';
EXCEPTION
  WHEN others THEN
    RETURN 'N';
END IS_NUMBER_F;