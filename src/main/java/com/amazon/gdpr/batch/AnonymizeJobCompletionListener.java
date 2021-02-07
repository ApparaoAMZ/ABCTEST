package com.amazon.gdpr.batch;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

public class AnonymizeJobCompletionListener extends JobExecutionListenerSupport {
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
	
	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;
	
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_ANONYMIZECOMPLETIONLISTENER;
	String jobRelatedName = "";
	Date moduleStartDateTime = null;
		
	public AnonymizeJobCompletionListener(String jobRelatedName) {
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
		
		moduleStartDateTime = jobExecution.getStartTime();
		String anonymizeData = "";
		String errorMessage = "";
		String jobExitStatus = jobExecution.getExitStatus().getExitCode();
		
		List<Throwable> lstThrowable = jobExecution.getAllFailureExceptions();
		if(lstThrowable != null && lstThrowable.size() > 0) {
			for(Throwable throwable : lstThrowable) {
				
				anonymizeData = anonymizeData + " Error Message : "+throwable.getMessage() +" Localized Message "+throwable.getLocalizedMessage() +
						" Error Cause : "+throwable.getCause()+" Class "+throwable.getClass();				
				errorMessage = errorMessage + Arrays.toString(throwable.getStackTrace());					
			}	
		} else
			anonymizeData = GlobalConstants.MSG_ANONYMIZE_DATA + "for runId - "+runId+"; runSummaryId - "+ runSummaryId+". ";
						
		String batchJobStatus = "BATCH JOB COMPLETED SUCCESSFULLY for runId - "+runId+" for runSummaryId - "+ runSummaryId
				+" with status -"+jobExitStatus;
		
		try {
			String moduleStatus = (jobExecution.getStatus() == BatchStatus.FAILED) ? GlobalConstants.STATUS_FAILURE : 
				GlobalConstants.STATUS_SUCCESS;
			RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, GlobalConstants.SUB_MODULE_ANONYMIZE_DATA,
				moduleStatus, moduleStartDateTime, new Date(), anonymizeData, errorMessage);
			moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			//runMgmtDaoImpl.updateRunComments(runId, anonymizeData );
			
		} catch(GdprException exception) {
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Exception : "+exception.getExceptionMessage());
		}		
	}
}