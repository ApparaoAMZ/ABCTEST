package com.amazon.gdpr.batch;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.dao.RunSummaryDaoImpl;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

public class TaggingJobCompletionListener extends JobExecutionListenerSupport {
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
	
	@Autowired
	RunSummaryDaoImpl runSummaryDaoImpl;
	
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_TAGGINGCOMPLETIONLISTENER;
	String jobRelatedName = "";
	Date moduleStartDateTime = null;	
	
	public TaggingJobCompletionListener(String jobRelatedName) {
		this.jobRelatedName = jobRelatedName;
	}
		
	@Override
	public void afterJob(JobExecution jobExecution) {		
		String CURRENT_METHOD = "afterJob";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		
		JobParameters jobParameters = jobExecution.getJobParameters();
		
		long runId = jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_ID);
		long jobId = jobParameters.getLong(GlobalConstants.JOB_INPUT_JOB_ID);
		long runSummaryId = jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_SUMMARY_ID);
		String tableName = jobParameters.getString(GlobalConstants.JOB_INPUT_TABLE_NAME);
		Date moduleStartDateTime = jobParameters.getDate(GlobalConstants.JOB_INPUT_START_DATE);
		
		String taggingData = "";
		String errorMessage = "";		
		Boolean exceptionOccurred = false;
		moduleStartDateTime = jobExecution.getStartTime();
		String jobExitStatus = jobExecution.getExitStatus().getExitCode();
		
		List<Throwable> lstThrowable = jobExecution.getAllFailureExceptions();
		if(lstThrowable != null && lstThrowable.size() > 0) {
			for(Throwable throwable : lstThrowable) {
				taggingData = taggingData + " Error Message : "+throwable.getMessage() +" Localized Message "+throwable.getLocalizedMessage() +
						" Error Cause : "+throwable.getCause()+" Class "+throwable.getClass();				
				errorMessage = errorMessage + Arrays.toString(throwable.getStackTrace());					
			}	
		} else{
			taggingData = GlobalConstants.MSG_TAGGING_DATA + " for runId - "+runId+" for runSummaryId - "+ runSummaryId+". ";
		}
		try {
			String sqlQuery = "UPDATE GDPR.RUN_SUMMARY_MGMT SET TAGGED_ROW_COUNT = (SELECT COUNT(TAG.ID) COUNT FROM "+tableName+
					" TAG, GDPR.RUN_SUMMARY_MGMT RSM WHERE TAG.RUN_ID = RSM.RUN_ID AND TAG.CATEGORY_ID = RSM.CATEGORY_ID "+
					" AND TAG.COUNTRY_CODE = RSM.COUNTRY_CODE AND RSM.SUMMARY_ID = "+runSummaryId+" ) WHERE SUMMARY_ID = "+runSummaryId;
			runSummaryDaoImpl.tagDataCountUpdate(sqlQuery);
			taggingData = taggingData + GlobalConstants.MSG_TAGDATA_COUNT_UPDATE;
		} catch(Exception exception) {
			exceptionOccurred = true;
			taggingData = taggingData +GlobalConstants.ERR_TAGDATA_COUNT_UPDATE;
			errorMessage = errorMessage + Arrays.toString(exception.getStackTrace());
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Exception : "+exception.getMessage());
		}
		
		String batchJobStatus = "BATCH JOB COMPLETED SUCCESSFULLY for runId - "+runId+" for runSummaryId - "+ runSummaryId
				+" with status -"+jobExitStatus;
		
		try {
			String moduleStatus = (jobExecution.getStatus() == BatchStatus.FAILED || exceptionOccurred) ? GlobalConstants.STATUS_FAILURE : 
				GlobalConstants.STATUS_SUCCESS;
			RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, GlobalConstants.SUB_MODULE_TAGGED_DATA,
					moduleStatus, moduleStartDateTime, new Date(), taggingData, errorMessage);
			moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			//runMgmtDaoImpl.updateRunComments(runId, taggingData);
		} catch(GdprException exception) {
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Exception : "+exception.getExceptionMessage());
		}
	}
}