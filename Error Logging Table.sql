-- Error Logging Table
CREATE TABLE error_log 
( 
   log_id       NUMBER GENERATED ALWAYS AS IDENTITY, 
   created_on   TIMESTAMP WITH LOCAL TIME ZONE, 
   created_by   VARCHAR2 (100), 
   errorcode    INTEGER, 
   callstack    VARCHAR2 (4000), 
   errorstack   VARCHAR2 (4000), 
   backtrace    VARCHAR2 (4000), 
   error_info   VARCHAR2 (4000) 
);

PROCEDURE log_error (app_info_in IN VARCHAR2) 
   IS 
      PRAGMA AUTONOMOUS_TRANSACTION; 
      /* Cannot call this function directly in SQL */ 
      c_code   CONSTANT INTEGER := SQLCODE; 
   BEGIN 
      INSERT INTO error_log (created_on, 
                             created_by, 
                             errorcode, 
                             callstack, 
                             errorstack, 
                             backtrace, 
                             error_info) 
           VALUES (SYSTIMESTAMP, 
                   USER, 
                   c_code, 
                   DBMS_UTILITY.format_call_stack, 
                   DBMS_UTILITY.format_error_stack, 
                   DBMS_UTILITY.format_error_backtrace, 
                   app_info_in); 
 
      COMMIT; 
   END; 
END;
/


-- Try it Out
DECLARE 
   l_company_id   INTEGER; 
BEGIN 
   IF l_company_id IS NULL 
   THEN 
      RAISE VALUE_ERROR; 
   END IF; 
EXCEPTION  
   WHEN OTHERS 
   THEN 
      error_mgr.log_error ('Company ID is NULL - not allowed.'); 
END;
/

SELECT backtrace, errorstack, callstack FROM error_log;