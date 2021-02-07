package com.amazon.gdpr.dao;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.amazon.gdpr.model.BackupServiceOutput;
import com.amazon.gdpr.model.archive.AnonymizeTable;
import com.amazon.gdpr.model.archive.ArchiveTable;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * The DAOImpl file fetches the JDBCTemplate created in the DatabaseConfig and
 * Connects with the schema to load the Hvh Output table rows
 ****************************************************************************************/
@Transactional
@Repository
public class BackupServiceDaoImpl {

	private static String CURRENT_CLASS = "BackupServiceDaoImpl";

	@Autowired
	@Qualifier("gdprJdbcTemplate")
	private JdbcTemplate jdbcTemplate;

	/**
	 * This method inserts the backup data
	 * @param backupdatainsertquery
	 * @return data insterted count
	 */

	@Transactional
	public int insertBackupTable(String backupDataInsertQuery) {
		String CURRENT_METHOD = "insertBackupTable";
		//System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + ":: Inside method");
		// int ainsertedCount=jdbcTemplate.update(backupDataInsertQuery);
		int ainsertedCount = jdbcTemplate.update(backupDataInsertQuery);
		return ainsertedCount;
	}
	
	@Transactional
	public void updateSummaryTable(List<? extends BackupServiceOutput> lstBackupServiceOutput) {
		
				
	    //String sql = "UPDATE GDPR.RUN_SUMMARY_MGMT SET BACKUP_ROW_COUNT=? WHERE SUMMARY_ID=? AND RUN_ID=?";
	    String CURRENT_METHOD = "batchInsertGdprDepersonalizationOutput";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		int[] updateCounts  = jdbcTemplate.batchUpdate(SqlQueriesConstant.UPDATE_BACKUPROW_COUNT, new BatchPreparedStatementSetter() { 						
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				String CURRENT_METHOD = "setValues";		
				//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
				BackupServiceOutput backupServiceOutput = (BackupServiceOutput) lstBackupServiceOutput.get(i);
				ps.setLong(1, backupServiceOutput.getBackupRowCount());
				ps.setLong(2, backupServiceOutput.getSummaryId());
				ps.setLong(3, backupServiceOutput.getRunId());
				
				
			}
			public int getBatchSize() {
				return lstBackupServiceOutput.size();
			}
		});

	}
	
	@Transactional
	public void tagArchivalTables(List<? extends ArchiveTable> lstArchiveTable, String sqlQuery) {
	    String CURRENT_METHOD = "tagArchivalTables";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		int[] updateCounts  = jdbcTemplate.batchUpdate(sqlQuery, new BatchPreparedStatementSetter() { 						
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				String CURRENT_METHOD = "setValues";		
				//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
				//RUN_ID, ID, CATEGORY_ID, COUNTRY_CODE, STATUS
				ArchiveTable archiveTable = (ArchiveTable) lstArchiveTable.get(i);				
				ps.setLong(1, archiveTable.getRunId());
				ps.setLong(2, archiveTable.getId());
				ps.setInt(3, archiveTable.getCategoryId());
				ps.setString(4, archiveTable.getCountryCode());
				ps.setString(5, archiveTable.getStatus());
			}
			public int getBatchSize() {
				return lstArchiveTable.size();
			}
		});
	}
	
	@Transactional
	public void anonymizeArchivalTables(List<? extends AnonymizeTable> lstAnonymizeTable, String sqlQuery) {
	    String CURRENT_METHOD = "anonymizeArchivalTables";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		int[] updateCounts  = jdbcTemplate.batchUpdate(sqlQuery, new BatchPreparedStatementSetter() { 						
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				String CURRENT_METHOD = "setValues";		
				//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
				AnonymizeTable anonymizeTable = (AnonymizeTable) lstAnonymizeTable.get(i);
				ps.setLong(1, anonymizeTable.getAnonymizeId());
			}
			public int getBatchSize() {
				return lstAnonymizeTable.size();
			}
		});
	}
	
	@Transactional
	public void updateTagTables(List<? extends AnonymizeTable> lstAnonymizeTable, String sqlQuery) {
	    String CURRENT_METHOD = "anonymizeArchivalTables";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		int[] updateCounts  = jdbcTemplate.batchUpdate(sqlQuery, new BatchPreparedStatementSetter() { 						
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				String CURRENT_METHOD = "setValues";		
				//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
				AnonymizeTable anonymizeTable = (AnonymizeTable) lstAnonymizeTable.get(i);
				ps.setLong(1, anonymizeTable.getAnonymizeId());
				ps.setLong(2, anonymizeTable.getRunId());
			}
			public int getBatchSize() {
				return lstAnonymizeTable.size();
			}
		});
	}
	
	@Transactional
	public void tagStatusUpdate(List<? extends AnonymizeTable> lstAnonymizeTable, String sqlQuery) {
	    String CURRENT_METHOD = "tagStatusUpdate";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		int[] updateCounts  = jdbcTemplate.batchUpdate(sqlQuery, new BatchPreparedStatementSetter() { 						
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				String CURRENT_METHOD = "setValues";		
				//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
				AnonymizeTable anonymizeTable = (AnonymizeTable) lstAnonymizeTable.get(i);
				ps.setLong(1, anonymizeTable.getAnonymizeId());
			}
			public int getBatchSize() {
				return lstAnonymizeTable.size();
			}
		});
	}
	
	@Transactional
	public int gdprDepStatusUpdate(long runId, String sqlQuery) {
	    String CURRENT_METHOD = "tagStatusUpdate";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
		
		int updatedCount = jdbcTemplate.update(sqlQuery, new Object[]{runId});
		return updatedCount;
	}
	
}