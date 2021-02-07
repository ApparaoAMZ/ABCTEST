package com.amazon.gdpr.util;

/****************************************************************************************
 * This Class contains all the Queries that is required through out the project 
 ****************************************************************************************/
public class SqlQueriesConstant {
	
	public static int BATCH_ROW_COUNT = 10000;
	
	public static String RUNMGMT_LAST_RUN 	= "SELECT RUN_ID, RUN_STATUS FROM GDPR.RUN_MGMT WHERE RUN_ID > 0 ORDER BY RUN_ID DESC LIMIT 1";
	
	public static String RUNMGMT_INSERT_RUN = "INSERT INTO GDPR.RUN_MGMT(RUN_NAME, RUN_STATUS, RUN_START_DATE_TIME) VALUES( "
			+ "?, ?, CURRENT_TIMESTAMP)";
	
	public static String RUNMGMT_INSERT_QRY	= "INSERT INTO GDPR.RUN_MGMT(RUN_NAME, RUN_STATUS) VALUES(?, ?) "
			+ "ON DUPLICATE KEY UPDATE RUN_NAME=VALUES(RUN_NAME), RUN_ID=LAST_INSERT_ID(RUN_ID)";
	
	public static String RUNMGMT_UPDATE_STATUS = "UPDATE GDPR.RUN_MGMT SET RUN_STATUS = ?, RUN_END_DATE_TIME = CURRENT_TIMESTAMP, "
			+ " RUN_COMMENTS = CONCAT(RUN_COMMENTS, ?) WHERE RUN_ID = ?";
	
	public static String RUNMGMT_UPDATE_COMMENTS = "UPDATE GDPR.RUN_MGMT SET RUN_COMMENTS = CONCAT(RUN_COMMENTS, ?) WHERE RUN_ID = ?";
	
	public static String RUN_MODULE_MGMT_INSERT	= "INSERT INTO GDPR.RUN_MODULE_MGMT (RUN_ID, MODULE_NAME, SUBMODULE_NAME, MODULE_STATUS, "
			+ "MODULE_START_DATE_TIME, MODULE_END_DATE_TIME, MODULE_COMMENTS, ERROR_DETAIL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
	
	public static String RUN_MODULE_MGMT_JOB_STATUS_FETCH = "SELECT MODULE_STATUS FROM GDPR.RUN_MODULE_MGMT "
			+ "WHERE RUN_ID = ? AND MODULE_STATUS = 'SUCCESS'";
	
	public static String RUN_MODULE_STATUS_FETCH = "SELECT MODULE_STATUS FROM GDPR.RUN_MODULE_MGMT "
			+ " WHERE RUN_ID = ?";
	
	//public static String CATEGORY_FETCH		= "SELECT CATEGORY_ID, CATEGORY_NAME, GDPR_CATEGORY_STATUS_FIELD, PROCESSED_DATE_FIELD, STATUS FROM "
	public static String CATEGORY_FETCH		= "SELECT CATEGORY_ID, CATEGORY_NAME, GDPR_CATEGORY_STATUS_FIELD, STATUS FROM "
			+ "GDPR.CATEGORY WHERE STATUS IN ('ACTIVE')";
	
	public static String IMPACT_TABLE_FETCH	= "SELECT IMPACT_TABLE_ID, IMPACT_TABLE_NAME, PARENT_TABLE_NAME, "
			+ "IMPACT_TABLE_COLUMN, PARENT_TABLE_COLUMN FROM GDPR.IMPACT_TABLE ";
	
	public static String IMPACT_TABLE_FETCH_ALL = "SELECT IMPACT_TABLE_ID, IMPACT_SCHEMA, IMPACT_TABLE_NAME, IMPACT_COLUMNS, IMPACT_TABLE_TYPE, "
			+ "PARENT_SCHEMA, PARENT_TABLE_NAME, IMPACT_TABLE_COLUMN, PARENT_TABLE_COLUMN FROM GDPR.IMPACT_TABLE";
	
	public static String COUNTRY_FETCH = "SELECT REGION, COUNTRY_CODE FROM GDPR.COUNTRY WHERE STATUS = 'ACTIVE' AND "
			+ " COUNTRY_CODE IN (%s) ORDER BY REGION, COUNTRY_CODE ASC";
	
	public static String ALL_COUNTRY_FETCH = "SELECT REGION, COUNTRY_CODE FROM GDPR.COUNTRY WHERE STATUS = 'ACTIVE' "
			+ "ORDER BY REGION, COUNTRY_CODE ASC";
	
	public static String IMPACT_FIELD_FETCH = "SELECT IMPACT_FIELD_ID, IMPACT_TABLE_ID, IMPACT_FIELD_LABEL, "
			+ " IMPACT_FIELD_NAME, IMPACT_FIELD_TYPE FROM GDPR.IMPACT_FIELD";
	
	public static String ANONYMIZATION_DETAIL_FETCH = "SELECT IMPACT_FIELD_ID, "
			+ " CATEGORY_ID, REGION, COUNTRY_CODE, TRANSFORMATION_TYPE, STATUS FROM GDPR.ANONYMIZATION_DETAIL";
			
	public static String SUMMARY_DATA_FETCH = "SELECT DISTINCT AD.CATEGORY_ID CATEGORY_ID, AD.REGION REGION, RA.COUNTRY_CODE COUNTRY_CODE, "
			+" IT.IMPACT_TABLE_NAME IMPACT_TABLE_NAME, IT.IMPACT_SCHEMA IMPACT_SCHEMA, IF.IMPACT_FIELD_NAME IMPACT_FIELD_NAME, "
			+ "IF.IMPACT_FIELD_TYPE IMPACT_FIELD_TYPE, AD.TRANSFORMATION_TYPE TRANSFORMATION_TYPE, IF.IMPACT_TABLE_ID IMPACT_TABLE_ID FROM "
			+" GDPR.ANONYMIZATION_DETAIL AD, GDPR.IMPACT_FIELD IF, GDPR.IMPACT_TABLE IT, GDPR.CATEGORY CAT, GDPR.COUNTRY CTY, GDPR.RUN_ANONYMIZATION RA "
			+" WHERE AD.IMPACT_FIELD_ID = IF.IMPACT_FIELD_ID AND IF.IMPACT_TABLE_ID =IT.IMPACT_TABLE_ID "
			+" AND CAT.CATEGORY_ID = AD.CATEGORY_ID AND (AD.REGION = CTY.REGION OR AD.COUNTRY_CODE = CTY.COUNTRY_CODE) "
			+" AND AD.STATUS = 'ACTIVE' AND CAT.STATUS = 'ACTIVE' AND CTY.STATUS = 'ACTIVE' "
			+" AND AD.ANONYMIZATION_DETAIL_ID = RA.ANONYMIZATION_DETAIL_ID AND RA.RUN_ID = ? "			
			+" ORDER BY AD.CATEGORY_ID, AD.REGION, RA.COUNTRY_CODE, IF.IMPACT_TABLE_ID ";
		
	public static String IMPACT_FIELD_INSERT = "INSERT INTO GDPR.IMPACT_FIELD (IMPACT_TABLE_ID, "
			+ " IMPACT_FIELD_LABEL, IMPACT_FIELD_NAME, IMPACT_FIELD_TYPE) VALUES (?, ?, ?, ?)";
	
	public static String ANONYMIZATION_DETAIL_INSERT = "INSERT INTO GDPR.ANONYMIZATION_DETAIL "
			+ " (IMPACT_FIELD_ID, CATEGORY_ID, REGION, COUNTRY_CODE, TRANSFORMATION_TYPE, STATUS) "
			+ " VALUES (?, ?, ?, ?, ?, ?)";
	public static String ANONYMIZATION_DETAIL_UPDATE = "UPDATE GDPR.ANONYMIZATION_DETAIL SET CATEGORY_ID=?,REGION=?,COUNTRY_CODE=?,"
			+ "TRANSFORMATION_TYPE=?,STATUS=? WHERE IMPACT_FIELD_ID=?";

	public static String RUN_ANONYMIZATION_INSERT = "INSERT INTO GDPR.RUN_ANONYMIZATION (RUN_ID, COUNTRY_CODE, ANONYMIZATION_DETAIL_ID) "
			+ " SELECT ?, ?, ANONYMIZATION_DETAIL_ID FROM GDPR.ANONYMIZATION_DETAIL WHERE STATUS = 'ACTIVE' AND (REGION = ? OR "
			+ " COUNTRY_CODE = ?)";
	
	public static String RUN_ERROR_MGMT_INSERT = "INSERT INTO GDPR.RUN_ERROR_MGMT (RUN_ID, ERROR_SUMMARY, ERROR_DETAIL ) "
			+ "VALUES (?, ?, ?) ";
	
	public static String RUN_SUMMARY_MGMT_INSERT = "INSERT INTO GDPR.RUN_SUMMARY_MGMT (RUN_ID, CATEGORY_ID, REGION, COUNTRY_CODE, "
			+ " IMPACT_TABLE_ID, IMPACT_TABLE_NAME, BACKUP_QUERY, DEPERSONALIZATION_QUERY, TAGGED_QUERY) VALUES (?,?,?,?,?, ?,?,?,?) ";
	
	public static String RUN_SUMMARY_MGMT_UPDATE = "UPDATE GDPR.RUN_SUMMARY_MGMT SET "
			+ "DEPERSONALIZATION_ROW_COUNT = ? WHERE SUMMARY_ID = ?";
			//+ "DEPERSONALIZATION_ROW_COUNT = COALESCE(NULLIF(DEPERSONALIZATION_ROW_COUNT, NULL),'0') + ? WHERE SUMMARY_ID = ?";
	
	public static String RUN_SUMMARY_MGMT_LIST_FETCH = "SELECT SUMMARY_ID, RUN_ID, CATEGORY_ID, COUNTRY_CODE, IMPACT_TABLE_ID, IMPACT_TABLE_NAME, "
			+ " BACKUP_QUERY, DEPERSONALIZATION_QUERY, TAGGED_QUERY FROM GDPR.RUN_SUMMARY_MGMT WHERE RUN_ID = ? ORDER BY RUN_ID, SUMMARY_ID";
	
	public static String RUN_SUMMARY_MGMT_ROW_FETCH = "SELECT SUMMARY_ID12, RUN_ID, CATEGORY_ID, COUNTRY_CODE, IMPACT_TABLE_ID, IMPACT_TABLE_NAME, "
			+ " BACKUP_QUERY, DEPERSONALIZATION_QUERY, TAGGED_QUERY FROM GDPR.RUN_SUMMARY_MGMT WHERE RUN_ID = ? AND SUMMARY_ID = ? "
			+ " ORDER BY RUN_ID, SUMMARY_ID ";
	
	/*****Backup Table Processor Code Change Starts***/
	public static String BACKUPTABLE_COLUMNS_QRY	= "SELECT TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE "
			+ "LOWER(TABLE_SCHEMA) = LOWER('GDPR') AND TABLE_NAME LIKE LOWER('%BKP_%')";
	
	public static String IMPACTTABLE_DETAILS_QRY	= "SELECT T1.IMPACT_TABLE_NAME, T2.IMPACT_FIELD_NAME,  T2.IMPACT_FIELD_TYPE FROM "
			+ "GDPR.IMPACT_TABLE T1 JOIN  GDPR.IMPACT_FIELD T2 ON T1.IMPACT_TABLE_ID = T2.IMPACT_TABLE_ID;";	
	/*****Backup Table Processor Code Change Ends***/
		
	public static String GDPR_DEPERSONALIZATION_FETCH =  "SELECT GD.CANDIDATE__C, GD.BGC_APPLICATION__c, GD.CATEGORY__C, GD.COUNTRY_CODE__C, " 
		+ "GD.BGC_STATUS__C, GD.PH_AMAZON_ASSESSMENT_STATUS__C, GD.PH_CANDIDATE_PROVIDED_STATUS__C, GD.PH_MASTER_DATA_STATUS__C, "
		+ "GD.PH_WITH_CONSENT_STATUS__C FROM SF_ARCHIVE.GDPR_DEPERSONALIZATION__C GD, GDPR.DATA_LOAD DL "
		+ " WHERE (GD.BGC_STATUS__C = 'Cleared' OR GD.PH_AMAZON_ASSESSMENT_STATUS__C = 'Cleared' OR " 
		+ "GD.PH_CANDIDATE_PROVIDED_STATUS__C = 'Cleared' OR GD.PH_MASTER_DATA_STATUS__C = 'Cleared' OR "
		+ "GD.PH_WITH_CONSENT_STATUS__C = 'Cleared') "
		+ "AND GD.COUNTRY_CODE__C = DL.COUNTRY_CODE AND DL.TABLE_NAME = 'GDPR_DEPERSONALIZATION__C' " 
		+ "AND (GD.CREATEDDATE >= DL.LAST_DATA_LOADED_DATE OR GD.LASTMODIFIEDDATE >= DL.LAST_DATA_LOADED_DATE) "
		
		+ "AND GD.COUNTRY_CODE__C =  ";
	
	public static String GDPR_DEPERSONALIZATION_INPUT = "INSERT INTO GDPR.GDPR_DEPERSONALIZATION (RUN_ID, CANDIDATE__C, CATEGORY_ID, "
			+ "COUNTRY_CODE, HVH_STATUS, HEROKU_STATUS) VALUES (?, ?, ?, ?, ?, ?)";		
		
	public static String DATA_LOAD_UPDATE = "UPDATE GDPR.DATA_LOAD SET LAST_DATA_LOADED_DATE = "
			+ "(SELECT DATA_LOAD_DATE FROM GDPR.RUN_MGMT WHERE RUN_ID = ? ) WHERE DATA_LOAD_ID IN "
			+ "(SELECT DATA_LOAD_ID FROM GDPR.DATA_LOAD WHERE COUNTRY_CODE IN "
			+ "(SELECT DISTINCT COUNTRY_CODE FROM GDPR.RUN_SUMMARY_MGMT WHERE RUN_ID = ?))";
	
	public static String LAST_DATA_LOAD_FETCH = "SELECT TO_CHAR(MAX(LAST_DATA_LOADED_DATE), \'"
			+GlobalConstants.DATE_FORMAT+"\') STR_DATA_LOADED_DATE FROM GDPR.DATA_LOAD";
		
	public static String FETCH_CURRENT_TIMESTAMP = "SELECT TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.US')";
	
	public static String GDPR_DEP_MAX_DATE_FETCH = "SELECT TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS.US')"; 
	
	//BackupServiceQueris
	public static String GDPR_SUMMARYDATA_FETCH = "SELECT SUMMARY_ID,RUN_ID,CATEGORY_ID,REGION,COUNTRY_CODE,IMPACT_TABLE_ID,IMPACT_TABLE_NAME,BACKUP_QUERY,DEPERSONALIZATION_QUERY FROM  GDPR.RUN_SUMMARY_MGMT WHERE RUN_ID=";	
	public static String LAST_MODULE_DATA_FETCH = "SELECT * FROM GDPR.RUN_MODULE_MGMT WHERE RUN_ID = ? ";
	public static String UPDATE_BACKUPROW_COUNT = "UPDATE GDPR.RUN_SUMMARY_MGMT SET BACKUP_ROW_COUNT=? WHERE SUMMARY_ID=? AND RUN_ID=?";
	

}