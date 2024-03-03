CREATE OR REPLACE PROCEDURE XDMMGR.SAVE_EXCEPTION_IMPLEMENTED ( vc_partitionid VARCHAR2 )
is

	--To execute EXEC XDMMGR.TEST_IB_PRODUCT_ROLLOUT_HPESVC_DFLT('P1')
	
	--Declare
	--vc_partitionid VARCHAR2(10):= 'P1';
	V_JOB_ID NUMBER;
	V_SP_NAME   VARCHAR2 (50) := $$PLSQL_UNIT;
	

	--CREATE a Data Type
	TYPE DATA_TYPE IS RECORD (
		BASE_PRODUCT_ID       IB_PRODUCT_ROLLOUT_FULL.BASE_PRODUCT_ID%TYPE,
		ORIG_SERIAL_NUMBER_ID IB_PRODUCT_ROLLOUT_FULL.ORIG_SERIAL_NUMBER_ID%TYPE	);
	--Setting DATA_TYPE for collection --Index by means Associative Array --WITHOUT Index by its NESTED Table
	TYPE collection_name IS TABLE OF DATA_TYPE INDEX BY PLS_INTEGER;
	
	--collection Variable Declaration
	collection_variable collection_name;
	--Setting LIMIT Size
	BatchSize NUMBER := 100000;

	--Cursor to hold the records from source table
	V_source_sys_refcursor		SYS_REFCURSOR;
	V_query_string             VARCHAR2 (2000);

	--to track row count 
	v_totalrow_Count      NUMBER default 0;
	collection_Count      NUMBER default 0;

	--forall SAVE EXCEPTION
	e_bulk_dml_error EXCEPTION;							--Exception Declaration for BULK Exceptions
    PRAGMA EXCEPTION_INIT (e_bulk_dml_error, -24381);   --24381 FORALL DML Exception

BEGIN
    SELECT IB_LOAD_TIME_LOG_SEQ.NEXTVAL INTO V_JOB_ID FROM DUAL; -- to be placed in main procedure
	
	--TIME LOGGING STARTS
    XDMMGR.LOGGER_PROC(VC_PARTITIONID,'STARTS',0,V_JOB_ID,V_SP_NAME);

	--STEP_1
    --Build the dynamic SQL query
	--Partitions are used on the source table query to get the records based on Partitions.
	V_query_string := 'SELECT BASE_PRODUCT_ID, ORIG_SERIAL_NUMBER_ID 
			 FROM IB_PRODUCT_ROLLOUT_FULL PARTITION ('||vc_partitionid|| ') 
			 WHERE SPM_VALID_CONTRACT_HPESVC_FLAG IS NULL';

   OPEN V_source_sys_refcursor FOR V_query_string;

	--STEP_2
	--Fetch the REF CURSOR's data into the collection 
	--below says FETCH chunk of Cursor data BULK COLLECT INTO collection_variable in loop based LIMIT value;
	LOOP
		FETCH V_source_sys_refcursor BULK COLLECT INTO collection_variable LIMIT BatchSize;
		EXIT WHEN collection_variable.COUNT = 0; -- Exit the loop if no more rows fetched
	
	--STEP_3
	-- Use FORALL to update the data in the table
    -- SAVE EXCEPTIONS means don't stop if some DML fails
	FORALL i IN collection_variable.FIRST .. collection_variable.LAST SAVE EXCEPTIONS
	--ReWritten only the UPDATE first line to process only the within PARTITION instead of whole table (No Change in Logic)
		EXECUTE IMMEDIATE 
		'update IB_PRODUCT_ROLLOUT_FULL PARTITION ('||vc_partitionid||') ibp
		set ibp.SPM_VALID_CONTRACT_HPESVC_FLAG = ''N''
		where ibp.BASE_PRODUCT_ID = :A and
		ibp.ORIG_SERIAL_NUMBER_ID = :B
		' 
		USING collection_variable(i).BASE_PRODUCT_ID, 
				collection_variable(i).ORIG_SERIAL_NUMBER_ID;

	--to track row count 
	collection_count := collection_variable.count;
	v_totalrow_Count := v_totalrow_Count + collection_count;

    -- if any errors occurred during the FORALL SAVE EXCEPTIONS,
    -- a single exception is raised when the statement completes.
	
	COMMIT;
	END LOOP;

	CLOSE V_source_sys_refcursor;

	--TIME LOGGING ENDS
    XDMMGR.LOGGER_PROC(VC_PARTITIONID, 'ENDS', v_totalrow_Count, V_JOB_ID,V_SP_NAME);


EXCEPTION
	--forall SAVE EXCEPTION
	WHEN e_bulk_dml_error THEN
		BULK_DML_ERR_COUNT NUMBER := SQL%BULK_EXCEPTIONS.COUNT;
		SP_PROCESS_E_BULK_DML_ERROR(V_JOB_ID, V_SP_NAME, v_totalrow_Count, BULK_DML_ERR_COUNT );
	WHEN OTHERS
	THEN
		sp_log_error (V_SP_NAME); 
		sp_show_errors;
	RETURN;
END;
/

-- Error Logging Table
--CREATE TABLE error_log 
--CREATE TABLE BULK_DML_ERROR_LOG_SUMMARY 
--CREATE TABLE BULK_DML_ERROR_LOG_DETAIL

-- Error Logging procedures

--PROCEDURE sp_log_error (V_SP_NAME IN VARCHAR2) 
--create SP sp_show_errors
--create SP_PROCESS_E_BULK_DML_ERROR

-- Error Logging Table
CREATE TABLE error_log 
( 
   log_id       NUMBER GENERATED ALWAYS AS IDENTITY, 
   job_id		NUMBER,   --WE GET THIS FROM V_JOB_ID
   created_on   TIMESTAMP, 
   created_by   VARCHAR2 (100), 
   errorcode    INTEGER, 
   callstack    VARCHAR2 (4000), 
   errorstack   VARCHAR2 (4000), 
   backtrace    VARCHAR2 (4000), 
   error_object   VARCHAR2 (400) --WE GET THIS FROM V_SP_NAME
);

CREATE TABLE BULK_DML_ERROR_LOG_SUMMARY 
( 
   log_id       NUMBER GENERATED ALWAYS AS IDENTITY, 
   job_id		NUMBER,   --WE GET THIS FROM V_JOB_ID
   created_on   TIMESTAMP, 
   error_object   VARCHAR2 (100), --WE GET THIS FROM V_SP_NAME
   rows_done	NUMBER, --WE GET THIS FROM v_totalrow_Count
   rows_err		NUMBER	--WE GET THIS FROM BULK_DML_ERR_COUNT
);   

CREATE TABLE BULK_DML_ERROR_LOG_DETAIL
(
   log_id_i     NUMBER GENERATED ALWAYS AS IDENTITY, 
   job_id		NUMBER,   --WE GET THIS FROM V_JOB_ID
   created_on   TIMESTAMP, 
   error_object   VARCHAR2 (100), --WE GET THIS FROM V_SP_NAME
   ERR_CODE 	VARCHAR2 (40),  --SQL%BULK_EXCEPTIONS(i).ERROR_CODE;
   ERR_TEXT 	VARCHAR2 (1000), --v_error_text
   ERR_CAUSE_BY VARCHAR2 (1000) --v_error_caused_by
);


-- Error Logging procedures
CREATE OR REPLACE PROCEDURE sp_log_error (V_SP_NAME IN VARCHAR2) 
   IS 
      PRAGMA AUTONOMOUS_TRANSACTION; 
      c_code   CONSTANT INTEGER := SQLCODE; 
   BEGIN 
      INSERT INTO error_log (   job_id,
							 created_on, 
                             created_by, 
                             errorcode, 
                             callstack, 
                             errorstack, 
                             backtrace, 
                             error_object) 
           VALUES (V_SP_NAME,
				   SYSTIMESTAMP, 
                   USER, 
                   c_code, 
                   DBMS_UTILITY.format_call_stack, 
                   DBMS_UTILITY.format_error_stack, 
                   DBMS_UTILITY.format_error_backtrace, 
                   V_SP_NAME); 
 
      COMMIT; 
   END; 
END;
/

   CREATE OR REPLACE PROCEDURE PROCEDURE show_errors 
   IS 
   BEGIN 
      DBMS_OUTPUT.put_line ('-------SQLERRM-------------'); 
      DBMS_OUTPUT.put_line (LENGTH (SQLERRM)); 
      DBMS_OUTPUT.put_line (SQLERRM); 
      DBMS_OUTPUT.put_line ('-------FORMAT_ERROR_STACK--'); 
      DBMS_OUTPUT.put_line (LENGTH (DBMS_UTILITY.format_error_stack)); 
      DBMS_OUTPUT.put_line (DBMS_UTILITY.format_error_stack); 
   END; 

	CREATE OR REPLACE PROCEDURE SP_PROCESS_E_BULK_DML_ERROR (V_JOB_ID NUMBER, V_SP_NAME VARCHAR, v_totalrow_Count NUMBER, BULK_DML_ERR_COUNT NUMBER)
	IS 
    PRAGMA AUTONOMOUS_TRANSACTION; 
		

	  BEGIN 
	  
      INSERT INTO ERR_BULK_DML_ERROR_LOG_SUMMARY
	  (job_id, created_on, error_object, rows_done, rows_fail ) 
	  VALUES
	  (V_JOB_ID, SYSTIMESTAMP, V_SP_NAME, v_totalrow_Count, BULK_DML_ERR_COUNT );
      COMMIT; 
	
		FOR i IN 1 .. BULK_DML_ERR_COUNT 
			LOOP
			v_error_text := 'Error Index: ' || SQL%BULK_EXCEPTIONS (i).ERROR_INDEX || ' -- ERROR:' || sqlerrm(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE); 
			v_error_caused_by := collection_variable(SQL%BULK_EXCEPTIONS (i).ERROR_INDEX)
			INSERT INTO bulk_dml_error_log_detail
				(
				job_id, created_on, error_object, ERR_CODE, ERR_TEXT, ERR_CAUSE_BY)
				VALUES
				(
				V_JOB_ID, SYSTIMESTAMP, V_SP_NAME, SQL%BULK_EXCEPTIONS(i).ERROR_CODE, v_error_text, v_error_caused_by);
		END LOOP;
		COMMIT;

   END; 
END;
/



		



--+New Lines Starts
  WHEN e_forall
   THEN
    -- get the number of errors in the exception array
	BULK_DML_ERR_COUNT= SQL%BULK_EXCEPTIONS.COUNT
	DBMS_OUTPUT.PUT_LINE ('Number of Rows inserted: ' || v_totalrow_Count);
	DBMS_OUTPUT.PUT_LINE ('Number of Rows Failed to insert: ' || BULK_DML_ERR_COUNT);
    -- insert all exceptions into the load_errors table
	FOR i IN 1 .. SQL%BULK_EXCEPTIONS.COUNT 
	LOOP
	 v_code := SQL%BULK_EXCEPTIONS(i).ERROR_CODE;
	 v_error_text := 'Error Index in Array: ' || SQL%BULK_EXCEPTIONS (i).ERROR_INDEX || ' -- ERROR:' || sqlerrm(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE);
	 DBMS_OUTPUT.PUT_LINE ('Where: ' || V_SP_NAME);
	 DBMS_OUTPUT.PUT_LINE ('What Happened: ' || v_error_text);
	 DBMS_OUTPUT.PUT_LINE ('Whats the Value that caused this error: '|| collection_variable(SQL%BULK_EXCEPTIONS (i).ERROR_INDEX))
     DBMS_OUTPUT.PUT_LINE('Error #' || i || ' at '|| 'iteration#' || SQL%BULK_EXCEPTIONS(i).ERROR_INDEX);
     DBMS_OUTPUT.PUT_LINE('Error message is ' || SQLERRM(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE));

	END LOOP
--+New Lines Ends

    -- insert all exceptions into the load_errors table

    FOR j IN 1 .. SQL%BULK_EXCEPTIONS LOOP
      ecode := SQL%BULK_EXCEPTIONS(j).ERROR_CODE;
      collection_column_name1 := TRUNC(collection_variable(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).collection_column_name1);
      collection_column_name2 := collection_variable(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).collection_column_name2;
      collection_column_name3 := collection_variable(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).collection_column_name3;
      collection_column_name4 := collection_variable(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX).collection_column_name4;
	INSERT INTO load_errors
      (error_code, COLUMNS1, COLUMNS2, COLUMNS3, COLUMNS4)
      VALUES
      (ecode, collection_column_name1, collection_column_name2, collection_column_name3, collection_column_name4);
    END LOOP;


  WHEN OTHERS
   THEN
      v_code := SQLCODE;
      v_errm := SUBSTR (SQLERRM, 1, 64);  
      DBMS_OUTPUT.PUT_LINE (v_code || ' ' || v_errm);
      raise_application_error (-20101, 'Error in procedure rec', TRUE);
   RETURN;

END;
/


 





