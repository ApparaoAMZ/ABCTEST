package com.amazon.gdpr.processor;

import java.util.Date;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.dao.RunSummaryDaoImpl;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.model.gdpr.output.RunSummaryMgmt;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * This Service will initiate the TaggingBatchConfig Job  
 * This will be invoked after the completion of Backup Service 
 ****************************************************************************************/
@Component
public class TagDataProcessor {
	public static String CURRENT_CLASS	= GlobalConstants.CLS_TAGDATAPROCESSOR;
	
	@Autowired
	JobLauncher jobLauncher;
	
	@Autowired
    Job processTaggingJob;
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	@Autowired
	AnonymizeProcessor depersonalizationProcessor;
	
	@Autowired
	RunSummaryDaoImpl runSummaryDaoImpl;
	//GdprOutputDaoImpl gdprOutputDaoImpl;
	
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
	
	Date moduleStartDateTime = null;
	
	public void taggingInitialize(long runId) throws GdprException {
		String CURRENT_METHOD = "taggingInitialize";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
				
		TaggedJobThread jobThread = new TaggedJobThread(runId);
		jobThread.start();
	}
	
	class TaggedJobThread extends Thread {
		
		long runId;
		TaggedJobThread(long runId){
			this.runId = runId;
		}
		
		@Override
		public void run() {
			String CURRENT_METHOD = "run";
			String taggingDataStatus = "";
			Boolean exceptionOccured = false;			
			String errorDetails = "";
			String moduleStatus = "";
			String prevJobModuleStatus = "";

			try {
				moduleStartDateTime = new Date();
				String query = SqlQueriesConstant.RUN_SUMMARY_MGMT_LIST_FETCH;
				List<RunSummaryMgmt> lstRunSummaryMgmt = runSummaryDaoImpl.fetchRunSummaryDetail(runId, query);
				if(lstRunSummaryMgmt != null) {
					for(RunSummaryMgmt runSummaryMgmt : lstRunSummaryMgmt) { 
						JobParametersBuilder jobParameterBuilder= new JobParametersBuilder();
						jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_RUN_ID, runId);						
						jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_JOB_ID, new Date().getTime());
						jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_RUN_SUMMARY_ID, runSummaryMgmt.getSummaryId());
						jobParameterBuilder.addString(GlobalConstants.JOB_INPUT_TABLE_NAME, "TAG."+runSummaryMgmt.getImpactTableName());
						jobParameterBuilder.addDate(GlobalConstants.JOB_INPUT_START_DATE, new Date());
						/*jobParameterBuilder.addString(GlobalConstants.JOB_INPUT_COUNTRY_CODE, runSummaryMgmt.getCountryCode());
						jobParameterBuilder.addString(GlobalConstants.JOB_INPUT_CATEGORY_ID, String.valueOf(runSummaryMgmt.getCategoryId()));
						jobParameterBuilder.addString(GlobalConstants.JOB_INPUT_TAG_QRY, runSummaryMgmt.getTaggedQueryLoad());*/
						
						System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: JobParameters set ");
						JobParameters jobParameters = jobParameterBuilder.toJobParameters();

						jobLauncher.run(processTaggingJob, jobParameters);
					}
				}				
				taggingDataStatus = GlobalConstants.MSG_TAGGING_JOB;
			} catch (JobExecutionAlreadyRunningException | JobRestartException
					| JobInstanceAlreadyCompleteException | JobParametersInvalidException exception) {
				exceptionOccured = true;
				taggingDataStatus = GlobalConstants.ERR_TAGGED_JOB_RUN;
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+taggingDataStatus);
				exception.printStackTrace();
				errorDetails = exception.getMessage();
			} 
	    	try {
	    		prevJobModuleStatus = moduleMgmtProcessor.prevJobModuleStatus(runId);
				moduleStatus = (exceptionOccured || prevJobModuleStatus.equalsIgnoreCase(GlobalConstants.STATUS_FAILURE)) ? 
						GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
						GlobalConstants.SUB_MODULE_TAG_JOB_INITIALIZE, moduleStatus, moduleStartDateTime, 
						new Date(), taggingDataStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
				
				if((! exceptionOccured) && GlobalConstants.STATUS_SUCCESS.equalsIgnoreCase(prevJobModuleStatus)){
					depersonalizationProcessor.depersonalizationInitialize(runId);
				}
			} catch(GdprException exception) {
				exceptionOccured = true;
				taggingDataStatus = taggingDataStatus + exception.getExceptionMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+taggingDataStatus);
			}
	    	try {
				if(exceptionOccured || 
						GlobalConstants.STATUS_FAILURE.equalsIgnoreCase(prevJobModuleStatus)){
					runMgmtDaoImpl.updateRunStatus(runId, GlobalConstants.STATUS_FAILURE, taggingDataStatus);
				}else {
					runMgmtDaoImpl.updateRunComments(runId, taggingDataStatus);
				}
			} catch(Exception exception) {
				exceptionOccured = true;
				taggingDataStatus = taggingDataStatus + exception.getMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+taggingDataStatus);
			}	    	
		}
	}
}