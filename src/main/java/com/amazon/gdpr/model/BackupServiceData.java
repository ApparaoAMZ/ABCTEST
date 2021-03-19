package com.amazon.gdpr.model;

public class BackupServiceData {

	private long runId;
	private long summaryId;
	private String tableName;
	private String countryCode;	
	private int categoryId;
	private int recId;
	private String backupSelectQuery;
	private String backupDataInsertQuery;
	
	public BackupServiceData(){
		
	}
	
	/**
	 * @param summaryId
	 * @param runId
	 * @param categoryId
	 * @param countryCode
	 * @param impactTableId
	 * @param backupQuery
	 * @param depersonalizationQuery
	 * @param backupRowCount
	 * @param taggedRowCount
	 * @param depersonalizedRowCount
	 */
	public BackupServiceData(long summaryId,long runId,String backupSelectQuery, String backupDataInsertQuery,int recId, String tableName,int categoryId, String countryCode) {
		super();
		this.summaryId = summaryId;
		this.runId = runId;
		this.backupSelectQuery = backupSelectQuery;
		this.backupDataInsertQuery = backupDataInsertQuery;
		this.recId = recId;
		this.tableName = tableName;
		this.categoryId = categoryId;
		this.countryCode = countryCode;
	}

	public int getRecId() {
		return recId;
	}

	public void setRecId(int recId) {
		this.recId = recId;
	}

	public long getRunId() {
		return runId;
	}

	public void setRunId(long runId) {
		this.runId = runId;
	}

	public long getSummaryId() {
		return summaryId;
	}

	public void setSummaryId(long summaryId) {
		this.summaryId = summaryId;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}

	public int getCategoryId() {
		return categoryId;
	}

	public void setCategoryId(int categoryId) {
		this.categoryId = categoryId;
	}

	public String getBackupSelectQuery() {
		return backupSelectQuery;
	}

	public void setBackupSelectQuery(String backupSelectQuery) {
		this.backupSelectQuery = backupSelectQuery;
	}

	public String getBackupDataInsertQuery() {
		return backupDataInsertQuery;
	}

	public void setBackupDataInsertQuery(String backupDataInsertQuery) {
		this.backupDataInsertQuery = backupDataInsertQuery;
	}
	
	
}