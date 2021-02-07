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
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.processor.TagDataProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

public class BackupJobCompletionListener extends JobExecutionListenerSupport {
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	@Autowired
	TagDataProcessor tagDataProcessor;
	
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
	
	private static String CURRENT_CLASS		 		= "BackupJobCompletionListener";
	String jobRelatedName = "";
	Date moduleStartDateTime = null;
	
	public BackupJobCompletionListener(String jobRelatedName) {
		this.jobRelatedName = jobRelatedName;
	}
		
	@Override
	public void afterJob(JobExecution jobExecution) {		
		String CURRENT_METHOD = "afterJob";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+jobRelatedName+ " BATCH JOB COMPLETED SUCCESSFULLY");
		JobParameters jobParameters = jobExecution.getJobParameters();
		
		long runId = jobParameters.getLong(GlobalConstants.JOB_REORGANIZE_INPUT_RUNID);
		moduleStartDateTime = jobExecution.getStartTime();
		
		String backUpData = "";
		String errorMessage = "";
		String jobExitStatus = jobExecution.getExitStatus().getExitCode();
				
		List<Throwable> lstThrowable = jobExecution.getAllFailureExceptions();
		if(lstThrowable != null && lstThrowable.size() > 0) {
			for(Throwable throwable : lstThrowable) {
				backUpData = backUpData + " Error Message : "+throwable.getMessage() +" Localized Message "+throwable.getLocalizedMessage() +
						" Error Cause : "+throwable.getCause()+" Class "+throwable.getClass();				
				errorMessage = errorMessage + Arrays.toString(throwable.getStackTrace());				
			}	
		} else
			backUpData = GlobalConstants.MSG_BACKUPSERVICE_INPUT + " for runId - "+runId+". ";
				
		try {
			String moduleStatus = (jobExecution.getStatus() == BatchStatus.FAILED) ? GlobalConstants.STATUS_FAILURE : 
				GlobalConstants.STATUS_SUCCESS;
			RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_BACKUPSERVICE, GlobalConstants.SUB_MODULE_BACKUPSERVICE_DATA,
					moduleStatus, moduleStartDateTime, new Date(), backUpData, errorMessage);
			moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			//runMgmtDaoImpl.updateRunComments(runId, backUpData);
			if(GlobalConstants.STATUS_SUCCESS.equalsIgnoreCase(moduleStatus)) {
				tagDataProcessor.taggingInitialize(runId);
			}
		} catch(GdprException exception) {
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Exception : "+exception.getExceptionMessage());
		}
	}
}