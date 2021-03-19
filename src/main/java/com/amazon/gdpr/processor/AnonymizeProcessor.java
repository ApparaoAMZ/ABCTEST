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

import com.amazon.gdpr.dao.BackupServiceDaoImpl;
import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.dao.RunSummaryDaoImpl;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.model.gdpr.output.RunSummaryMgmt;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * This Service will initiate the DepersonalizationBatchConfig Job  
 * This will be invoked after the completion of Tagging 
 ****************************************************************************************/
@Component
public class AnonymizeProcessor {
	public static String CURRENT_CLASS	= GlobalConstants.CLS_ANONYMIZEPROCESSOR;
	
	@Autowired
	JobLauncher jobLauncher;
	
	@Autowired
    Job processAnonymizeJob;
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	@Autowired
	RunSummaryDaoImpl runSummaryDaoImpl;
	//GdprOutputDaoImpl gdprOutputDaoImpl;
	
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
	
	@Autowired
	BackupServiceDaoImpl backupServiceDaoImpl;
	
	@Autowired
	DataLoadProcessor dataLoadProcessor;
	
	Date moduleStartDateTime = null;
	
	public void depersonalizationInitialize(long runId) throws GdprException {
		String CURRENT_METHOD = "depersonalizationInitialize";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
				
		DepersonalizationJobThread jobThread = new DepersonalizationJobThread(runId);
		jobThread.start();
	}
	
	class DepersonalizationJobThread extends Thread {
		
		long runId;
		
		DepersonalizationJobThread(long runId){
			this.runId = runId;
		}
		
		@Override
		public void run() {
			String CURRENT_METHOD = "run";
			String depersonalizationDataStatus = "";
			Boolean exceptionOccured = false;			
			String errorDetails = "";
			String moduleStatus = "";
			String prevModuleStatus = ""; 

			try {
				moduleStartDateTime = new Date();
				String query = SqlQueriesConstant.RUN_SUMMARY_MGMT_DEPER_FETCH;
				List<RunSummaryMgmt> lstRunSummaryMgmt = runSummaryDaoImpl.fetchRunSummaryDetail(runId, query);
				if(lstRunSummaryMgmt != null) {
					for(RunSummaryMgmt runSummaryMgmt : lstRunSummaryMgmt) { 
						JobParametersBuilder jobParameterBuilder= new JobParametersBuilder();
						jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_RUN_ID, runId);						
						jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_JOB_ID, new Date().getTime());
						jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_RUN_SUMMARY_ID, runSummaryMgmt.getSummaryId());
						jobParameterBuilder.addDate(GlobalConstants.JOB_INPUT_START_DATE, new Date());
						
						System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: JobParameters set ");
						JobParameters jobParameters = jobParameterBuilder.toJobParameters();
						jobLauncher.run(processAnonymizeJob, jobParameters);						
					}
				}
				depersonalizationDataStatus = GlobalConstants.MSG_DEPERSONALIZATION_JOB;
			} catch (JobExecutionAlreadyRunningException | JobRestartException
					| JobInstanceAlreadyCompleteException | JobParametersInvalidException exception) {
				exceptionOccured = true;
				depersonalizationDataStatus = GlobalConstants.ERR_DEPERSONALIZE_JOB_RUN;
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+depersonalizationDataStatus);
				exception.printStackTrace();
				errorDetails = exception.getStackTrace().toString();
			}
	    	try {
    			prevModuleStatus = moduleMgmtProcessor.prevJobModuleStatus(runId);
    			if (GlobalConstants.STATUS_SUCCESS.equalsIgnoreCase(prevModuleStatus)) {
	    			String tagQueryUpdate = "UPDATE GDPR.GDPR_DEPERSONALIZATION SET HEROKU_STATUS = \'CLEARED\'"
							+" WHERE RUN_ID = ?";
					int gdprDepCount = backupServiceDaoImpl.gdprDepStatusUpdate(runId, tagQueryUpdate);
					depersonalizationDataStatus = depersonalizationDataStatus + " Heroku status updated in GDPR_Depersonalization table count : "+gdprDepCount+". ";
	    		}	    		
			} catch(Exception exception) {
				exceptionOccured = true;
				depersonalizationDataStatus = depersonalizationDataStatus + "Facing issues while updating GDPR_Depersonalization Heroku status. ";
				errorDetails = errorDetails + exception.getMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+depersonalizationDataStatus);
			}
	    	try {
	    		if (GlobalConstants.STATUS_SUCCESS.equalsIgnoreCase(prevModuleStatus)) {
					dataLoadProcessor.updateDataLoad(runId);				
					depersonalizationDataStatus = depersonalizationDataStatus + GlobalConstants.MSG_DATALOAD_DTLS;
	    		}
			} catch(GdprException exception) {
				exceptionOccured = true;
				depersonalizationDataStatus = depersonalizationDataStatus + GlobalConstants.ERR_DATALOAD_DTLS;
				errorDetails = errorDetails + exception.getExceptionDetail();
			}
	    	try {
				moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
						GlobalConstants.SUB_MODULE_ANONYMIZE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime, 
						new Date(), depersonalizationDataStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			} catch(GdprException exception) {
				exceptionOccured = true;
				depersonalizationDataStatus = depersonalizationDataStatus + GlobalConstants.ERR_MODULE_MGMT_INSERT;
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+depersonalizationDataStatus);
			}
	    	try {
				if(exceptionOccured || GlobalConstants.STATUS_FAILURE.equalsIgnoreCase(prevModuleStatus)){					
					runMgmtDaoImpl.updateRunStatus(runId, GlobalConstants.STATUS_FAILURE, depersonalizationDataStatus);
				}else {					
					runMgmtDaoImpl.updateRunStatus(runId, GlobalConstants.STATUS_SUCCESS, depersonalizationDataStatus);
				}
			} catch(Exception exception) {
				exceptionOccured = true;
				depersonalizationDataStatus = depersonalizationDataStatus + GlobalConstants.ERR_RUN_MGMT_UPDATE;
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+depersonalizationDataStatus);
			}
		}
	}
}