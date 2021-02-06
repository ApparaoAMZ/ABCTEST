package com.amazon.gdpr.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.amazon.gdpr.model.gdpr.output.RunErrorMgmt;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * The DAOImpl file fetches the JDBCTemplate created in the DatabaseConfig and 
 * Connects with the schema to fetch the Output table rows 
 ****************************************************************************************/
@Transactional
@Repository
public class GdprOutputDaoImpl {
	
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_GDPROUTPUTDAOIMPL;
	
	@Autowired
	@Qualifier("gdprJdbcTemplate")
	private JdbcTemplate jdbcTemplate;
	
	/**
	 * This method inserts the RunAnonymizationMapping values 
	 * @param lstImpactField
	 * @return
	 */	
	public int batchInsertRunAnonymizeMapping(long runId, String countryCode, String region) {
		String CURRENT_METHOD = "fetchCategoryDetails";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		
		return jdbcTemplate.update(SqlQueriesConstant.RUN_ANONYMIZATION_INSERT, new Object[]{runId, countryCode, region, countryCode});		
	}
	
	/**
	 * This method is called whenever there is an error 
	 * The error details are loaded in this method
	 * @param runErrorMgmt 
	 */
	@Transactional
	public void loadErrorDetails(RunErrorMgmt runErrorMgmt) {
		String CURRENT_METHOD = "loadErrorDetails";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		jdbcTemplate.update(SqlQueriesConstant.RUN_ERROR_MGMT_INSERT, new Object[]{runErrorMgmt.getRunId(), runErrorMgmt.getErrorSummary(), 
				runErrorMgmt.getErrorDetail()});
	}
	
	public void loadDataProcess(long runId) {
		
	}
		
	/**
	 * This method updates the DataLoad values 
	 * @param dataLoad the column values required for the table
	 * @return
	 */	
	public int updateDataLoad(long runId) {
		String CURRENT_METHOD = "updateDataLoad";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		return jdbcTemplate.update(SqlQueriesConstant.DATA_LOAD_UPDATE, new Object[]{runId, runId});	
	}
			
	/**
	 * Fetches the current timestamp
	 * @return Date The date when the last data was loaded in the table
	 */
	public String fetchCurrentTimestamp() {
		String CURRENT_METHOD = "fetchCurrentTimestamp";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		return (String) jdbcTemplate.queryForObject(SqlQueriesConstant.FETCH_CURRENT_TIMESTAMP,  String.class);
	}

	/**
	 * Fetches the Last Data Load detail of a particular table
	 * @return Date The date when the last data was loaded in the table
	 */
	public String fetchLastDataLoad() {
		String CURRENT_METHOD = "fetchLastDataLoad";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		return (String)jdbcTemplate.queryForObject(SqlQueriesConstant.LAST_DATA_LOAD_FETCH, String.class);
	}	
	
	/**
	 * Fetches the Last Data Load detail of a particular table
	 * @return Date The date when the last data was loaded in the table
	 */
	public List<RunModuleMgmt> fetchLastModuleData(long runId) {
		String CURRENT_METHOD = "fetchLastModuleData";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		@SuppressWarnings("unchecked")
		List<RunModuleMgmt> lstRunModuleMgmt= jdbcTemplate.query(SqlQueriesConstant.LAST_MODULE_DATA_FETCH, new Object[]{runId}, new ModuleDataRowMapper());
		return lstRunModuleMgmt;
	}
	
	/****************************************************************************************
	 * This rowMapper converts the row data from RUN_MODULE_MGMT table to RunModuleMgmt Object 
	 ****************************************************************************************/
	@SuppressWarnings("rawtypes")
	class ModuleDataRowMapper implements RowMapper {
		@SuppressWarnings("unused")
		private String CURRENT_CLASS = GlobalConstants.CLS_DATALOADROWMAPPER;
		
		/* 
		 * @see org.springframework.jdbc.core.RowMapper#mapRow(java.sql.ResultSet, int)
		 */
		@Override
		public RunModuleMgmt mapRow(ResultSet rs, int rowNum) throws SQLException {
			@SuppressWarnings("unused")
			String CURRENT_METHOD = "mapRow";		
			return new RunModuleMgmt(rs.getLong("RUN_ID"),rs.getString("MODULE_NAME"),
					rs.getString("SUBMODULE_NAME"),rs.getString("MODULE_STATUS"),
					rs.getDate("MODULE_START_DATE_TIME"),rs.getDate("MODULE_END_DATE_TIME"),
					rs.getString("MODULE_COMMENTS"), rs.getString("ERROR_DETAIL"));
		}
	}

}