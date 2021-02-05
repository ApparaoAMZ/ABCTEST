package com.amazon.gdpr.processor;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazon.gdpr.dao.RunSummaryDaoImpl;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.model.gdpr.output.RunSummaryMgmt;
import com.amazon.gdpr.model.gdpr.output.SummaryData;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

@Component
public class SummaryDataProcessor {
	
	public static String CURRENT_CLASS = GlobalConstants.CLS_SUMMARYDATAPROCESSOR;
	public String summaryDataProcessStatus = "";
	Map<String, RunSummaryMgmt> runSummaryMgmtMap = null;
		
	@Autowired
	RunSummaryDaoImpl runSummaryDaoImpl;
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	@Autowired
	TagQueryProcessor tagQueryProcessor;
	
	public String processSummaryData(long runId) throws GdprException {
		
		String CURRENT_METHOD = "processSummaryData";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method");
		  
		Boolean exceptionOccured = false;
		Date moduleStartDateTime = new Date();
		Date moduleEndDateTime = null;
		List<SummaryData> lstSummaryData = null;
		String errorDetails = "";
		
		try{
			lstSummaryData = this.extractSummaryDetails(runId);
			if(lstSummaryData != null && lstSummaryData.size() > 0) {
				List<RunSummaryMgmt> lstRunSummaryMgmt = this.transformSummaryDetails(runId, lstSummaryData);
				lstRunSummaryMgmt = tagQueryProcessor.updateSummaryQuery(runId, lstRunSummaryMgmt);
				this.loadRunSummaryMgmt(runId, lstRunSummaryMgmt);
				summaryDataProcessStatus = GlobalConstants.MSG_SUMMARY_ROWS+lstRunSummaryMgmt.size();
			}			
		} catch(GdprException exception) {
			exceptionOccured = true;
			summaryDataProcessStatus = GlobalConstants.ERR_RUN_SUMMARY_DATA_TRANSFORM;
			errorDetails = exception.getMessage();
		}
		try {			
			String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
			moduleEndDateTime = new Date();
			RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, GlobalConstants.SUB_MODULE_SUMMARY_DATA_INITIALIZE,
					moduleStatus, moduleStartDateTime, moduleEndDateTime, summaryDataProcessStatus, errorDetails);
			moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);			
		} catch(GdprException exception) {
			exceptionOccured = true;
			errorDetails = errorDetails + exception.getMessage();
			summaryDataProcessStatus = summaryDataProcessStatus + GlobalConstants.SEMICOLON_STRING + exception.getExceptionMessage();			
		}
		if(exceptionOccured)
			throw new GdprException(GlobalConstants.ERR_RUN_SUMMARY_DATA_TRANSFORM + GlobalConstants.ERR_MODULE_MGMT_INSERT, errorDetails);
		
		return summaryDataProcessStatus;
	}
		
	public List<SummaryData> extractSummaryDetails(long runId) throws GdprException {
		String CURRENT_METHOD = "extractSummaryDetails";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method");
		
		List<SummaryData> lstSummaryData = null;
		String errorDetails = "";
		
		try{
			lstSummaryData = runSummaryDaoImpl.fetchSummaryDetails(runId);
		} catch (Exception exception) {	
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+GlobalConstants.ERR_RUN_SUMMARY_DATA_FETCH);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
			throw new GdprException(GlobalConstants.ERR_RUN_SUMMARY_DATA_FETCH, errorDetails);
		}
		return lstSummaryData;
	}
	
	public List<RunSummaryMgmt> transformSummaryDetails(long runId, List<SummaryData> lstSummaryData) throws GdprException {
		String CURRENT_METHOD = "transformSummaryDetails";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method");
		
		int prevCategoryId = 0;
		String prevRegion = "";
		String prevCountryCode = "";
		int prevImpactTableId = 0; 
		String prevImpactSchema = "";
		String prevImpactTableName = "";
		String backupQuery = "SELECT ";
		String depersonalizationQuery = "UPDATE ";
		RunSummaryMgmt runSummaryMgmt = null;		
		List<RunSummaryMgmt> lstRunSummaryMgmt = new ArrayList<RunSummaryMgmt>();
		String errorDetails = "";
		
		try{
			for(SummaryData summaryData : lstSummaryData) {
				String currentRegion = summaryData.getRegion();
				String currentCountryCode = summaryData.getCountryCode();
				int currentCategoryId = summaryData.getCategoryId();
				int currentImpactTableId = summaryData.getImpactTableId();
				String currentImpactTableName = summaryData.getImpactTableName();
				String currentImpactFieldName = summaryData.getImpactFieldName();
				String currentImpactFieldType = summaryData.getImpactFieldType();
								
				if(prevCategoryId == 0){
					backupQuery = backupQuery + currentImpactFieldName;
					depersonalizationQuery = depersonalizationQuery +summaryData.getImpactSchema()+"."+currentImpactTableName + " SET " + 
							fetchUpdateField(currentImpactFieldName, currentImpactFieldType, summaryData.getTransformationType());
							
							//+ currentImpactFieldName +" = "+summaryData.getTransformationType();
				}else{
					if(currentCategoryId == prevCategoryId && currentRegion.equalsIgnoreCase(prevRegion) && 
							currentCountryCode.equalsIgnoreCase(prevCountryCode) && prevImpactTableId == currentImpactTableId ) {
						backupQuery = backupQuery + GlobalConstants.COMMA_STRING + currentImpactFieldName;
					    depersonalizationQuery = depersonalizationQuery + GlobalConstants.COMMA_STRING 
							+ fetchUpdateField(currentImpactFieldName, currentImpactFieldType, summaryData.getTransformationType());
							//currentImpactFieldName +" = "+summaryData.getTransformationType();
					} else {
						backupQuery = backupQuery + " FROM " + prevImpactTableName; 
						depersonalizationQuery = depersonalizationQuery + " WHERE ID = ?";
						//depersonalizationQuery = depersonalizationQuery + " WHERE ID = :anonymizeId";						
						runSummaryMgmt = new RunSummaryMgmt(runId, prevCategoryId, prevRegion, prevCountryCode, prevImpactTableId, 
								prevImpactTableName, backupQuery, depersonalizationQuery);
						//System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: runSummaryMgmt "+runSummaryMgmt.toString());
						lstRunSummaryMgmt.add(runSummaryMgmt);
						backupQuery = "SELECT "+currentImpactFieldName;
						depersonalizationQuery = "UPDATE " + summaryData.getImpactSchema()+"."+currentImpactTableName + " SET " 
								+ fetchUpdateField(currentImpactFieldName, currentImpactFieldType, summaryData.getTransformationType());
								//+ currentImpactFieldName +" = "+summaryData.getTransformationType();
					}
				}
			
				prevCategoryId = currentCategoryId;
				prevRegion = currentRegion;
				prevCountryCode = currentCountryCode;
				prevImpactTableId = currentImpactTableId;
				prevImpactSchema = summaryData.getImpactSchema();
				prevImpactTableName = currentImpactTableName;
			}
			backupQuery = backupQuery + " FROM " + prevImpactTableName;
			depersonalizationQuery = depersonalizationQuery + " WHERE ID = ?";
			//depersonalizationQuery = depersonalizationQuery + " WHERE ID = :anonymizeId";
			
			runSummaryMgmt = new RunSummaryMgmt(runId, prevCategoryId, prevRegion, prevCountryCode, prevImpactTableId, prevImpactTableName,
					backupQuery, depersonalizationQuery);
			//System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: runSummaryMgmt "+runSummaryMgmt.toString());
			lstRunSummaryMgmt.add(runSummaryMgmt);	
		} catch (Exception exception) {	
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+GlobalConstants.ERR_RUN_SUMMARY_DATA_TRANSFORM);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
			throw new GdprException(GlobalConstants.ERR_RUN_SUMMARY_DATA_TRANSFORM, errorDetails);
		}
		return lstRunSummaryMgmt;
	}
	
	public String fetchUpdateField(String fieldName, String fieldType, String conversionType) {
		
		String CURRENT_METHOD = "fetchUpdateField";
		//System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method");		
		String subQuery =  fieldName+" = (CASE WHEN ("+fieldName+" IS NULL OR TRIM("+fieldName+") = \'\') THEN "+fieldName+" ELSE ";
		if(fieldType.startsWith(GlobalConstants.DATE_DATATYPE)){			
			subQuery =  subQuery+" TO_DATE(TO_CHAR("+fieldName+", \'"+conversionType+"\'), \'DD-MM-YYYY\') END)";
		}else if (fieldType.startsWith(GlobalConstants.TEXT_DATATYPE)){			
			switch (conversionType) {
				case "PRIVACY DELETED" : 					
					subQuery = subQuery + "\'Privacy Deleted\' END)";
					break;
				case "NULL" :					
					subQuery =  subQuery + " null END)";
					break;
				case "EMPTY" :
					subQuery =  subQuery + " \'\'  END)";
					break;
				case "ALL ZEROS" :
					subQuery =  subQuery +" TRANSLATE("+fieldName+", \'123456789\', \'000000000\') END)";
					break;
				default : 					
					subQuery =  subQuery +" \'"+conversionType+"\' END)";
					break;
			}
		} else {
			subQuery =  subQuery+"\'"+conversionType+"\' END)";
		}
		return subQuery;
	}
	
	public void loadRunSummaryMgmt(long runId, List<RunSummaryMgmt> lstRunSummaryMgmt) throws GdprException {
		String CURRENT_METHOD = "loadRunSummaryMgmt";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method");
		String errorDetails = "";
		
		try{
			runSummaryDaoImpl.batchInsertRunSummaryMgmt(lstRunSummaryMgmt, SqlQueriesConstant.BATCH_ROW_COUNT);			
		} catch (Exception exception) {	
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+GlobalConstants.ERR_RUN_SUMMARY_DATA_LOAD);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
			throw new GdprException(GlobalConstants.ERR_RUN_SUMMARY_DATA_LOAD, errorDetails);
		}
	}
}