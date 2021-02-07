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
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

public class ReorganizeInputCompletionListener extends JobExecutionListenerSupport {
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
		
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
	
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_REORGANIZECOMPLETIONLISTENER;
	String jobRelatedName = "";
	Date moduleStartDateTime = null;
	String failureStatus = "";
	
	public ReorganizeInputCompletionListener(String jobRelatedName) {
		this.jobRelatedName = jobRelatedName;
	}
		
	@Override
	public void afterJob(JobExecution jobExecution) {		
		String CURRENT_METHOD = "afterJob";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		
		String reOrganizeData = CURRENT_CLASS +":::"+CURRENT_METHOD+"::";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+jobRelatedName+ " BATCH JOB COMPLETED SUCCESSFULLY");
		JobParameters jobParameters = jobExecution.getJobParameters();
		
		long runId = jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_ID);
		long jobId = jobParameters.getLong(GlobalConstants.JOB_INPUT_JOB_ID);
		String countryCode = jobParameters.getString(GlobalConstants.JOB_INPUT_COUNTRY_CODE);
		moduleStartDateTime = jobExecution.getStartTime();
		List<Throwable> lstThrowable = jobExecution.getAllFailureExceptions();
		if(lstThrowable != null && lstThrowable.size() > 0) {
			for(Throwable throwable : lstThrowable) {
				reOrganizeData = reOrganizeData + " Error Message : "+throwable.getMessage() +" Localized Message "+throwable.getLocalizedMessage() +
						" Error Cause : "+throwable.getCause()+" Class "+throwable.getClass();				
				failureStatus = failureStatus + Arrays.toString(throwable.getStackTrace());				
			}				
		} else
			reOrganizeData = reOrganizeData + GlobalConstants.MSG_REORGANIZEINPUT + countryCode+". ";
			
		try {
			String moduleStatus = (jobExecution.getStatus() == BatchStatus.FAILED) ? GlobalConstants.STATUS_FAILURE : 
				GlobalConstants.STATUS_SUCCESS;
			RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, GlobalConstants.SUB_MODULE_REORGANIZE_DATA,
				moduleStatus, moduleStartDateTime, new Date(), reOrganizeData, failureStatus);
			moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			//runMgmtDaoImpl.updateRunComments(runId, reOrganizeDataStatus);
			
		} catch(GdprException exception) {
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Exception : "+exception.getExceptionMessage());
		}
	}
}