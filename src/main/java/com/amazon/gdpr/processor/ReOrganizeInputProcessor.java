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

import com.amazon.gdpr.dao.GdprInputDaoImpl;
import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.service.BackupService;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This Service will reorganize the GDPR_Depersonalization__c table  
 * This will be invoked by the GDPRController
 ****************************************************************************************/
@Component
public class ReOrganizeInputProcessor {

	public static String CURRENT_CLASS	= GlobalConstants.CLS_REORGANIZE_INPUT_PROCESSOR;
	
	@Autowired
	JobLauncher jobLauncher;
	
	@Autowired
    Job processreorganizeInputJob;
	
	@Autowired
	RunMgmtProcessor runMgmtProcessor;

	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
			
	@Autowired
	GdprInputDaoImpl gdprInputDaoImpl;
	
	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;
	
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
	
	@Autowired
	BackupService backupService;
			
	public String reOrganizeData(long runId, List<String> selectedCountries) throws GdprException {
		String CURRENT_METHOD = "reOrganizeData";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+":: Inside method");
			
		JobThread jobThread = new JobThread(runId, selectedCountries);
		jobThread.start();
		return GlobalConstants.MSG_REORGANIZEINPUT_JOB;
	}
	
	class JobThread extends Thread {
		long runId;
		List<String> selectedCountries;
		
		JobThread(long runId, List<String> selectedCountries){
			this.runId = runId;
			this.selectedCountries = selectedCountries;
		}
		
		@Override
		public void run() {
			String CURRENT_METHOD = "run";
			String reOrganizeDataStatus = "";
			Boolean exceptionOccured = false;
			Date moduleStartDateTime = null;
			String errorDetails = "";
			String moduleStatus = "";
			String prevJobModuleStatus = "";
						
			try {
	    		moduleStartDateTime = new Date();	    		
	    		if(selectedCountries != null && selectedCountries.size() > 0) {
					for(String currentCountry : selectedCountries) { 
	    				    		
						JobParametersBuilder jobParameterBuilder= new JobParametersBuilder();
						jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_RUN_ID, runId);
						jobParameterBuilder.addLong(GlobalConstants.JOB_INPUT_JOB_ID, new Date().getTime());
						jobParameterBuilder.addDate(GlobalConstants.JOB_INPUT_START_DATE, new Date());
						jobParameterBuilder.addString(GlobalConstants.JOB_INPUT_COUNTRY_CODE, currentCountry);
						
						System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: JobParameters set ");
						JobParameters jobParameters = jobParameterBuilder.toJobParameters();
		
						jobLauncher.run(processreorganizeInputJob, jobParameters);
					}
					reOrganizeDataStatus = GlobalConstants.MSG_REORGANIZEINPUT_JOB;
	    		} else {
	    			reOrganizeDataStatus = "No countries selected to reorganize. ";
	    		}
	    						
			} catch (JobExecutionAlreadyRunningException | JobRestartException
					| JobInstanceAlreadyCompleteException | JobParametersInvalidException exception) {
				exceptionOccured = true;
				reOrganizeDataStatus = GlobalConstants.ERR_REORGANIZE_JOB_RUN;
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeDataStatus);
				exception.printStackTrace();
				errorDetails = exception.getStackTrace().toString();
			} 
	    	try {
				prevJobModuleStatus = moduleMgmtProcessor.prevJobModuleStatus(runId);				
				moduleStatus = (exceptionOccured || prevJobModuleStatus.equalsIgnoreCase(GlobalConstants.STATUS_FAILURE)) ? 
						GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, 
						GlobalConstants.SUB_MODULE_REORGANIZE_JOB_INITIALIZE, moduleStatus, moduleStartDateTime, 
						new Date(), reOrganizeDataStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);	
				if(GlobalConstants.STATUS_FAILURE.equalsIgnoreCase(moduleStatus)){
					runMgmtDaoImpl.updateRunStatus(runId, GlobalConstants.STATUS_FAILURE, reOrganizeDataStatus);
				}else{
					runMgmtDaoImpl.updateRunComments(runId, reOrganizeDataStatus);
				}
			} catch(GdprException exception) {
				exceptionOccured = true;
				reOrganizeDataStatus = reOrganizeDataStatus + exception.getExceptionMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeDataStatus);
			}
	    	try {
	    		if((! exceptionOccured) && GlobalConstants.STATUS_SUCCESS.equalsIgnoreCase(prevJobModuleStatus)){
					backupService.backupServiceInitiate(runId);
				}
			} catch(Exception exception) {
				exceptionOccured = true;
				reOrganizeDataStatus = reOrganizeDataStatus + exception.getMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeDataStatus);
			}
		}
	}	
}