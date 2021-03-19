package com.amazon.gdpr.processor;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazon.gdpr.dao.GdprInputFetchDaoImpl;
import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.model.gdpr.input.Country;
import com.amazon.gdpr.model.gdpr.output.RunMgmt;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.view.GdprInput;

/****************************************************************************************
 * This processor verifies the previous failure run / initiates a current run 
 * Any processing or updates related to the RunMgmt tables are performed here
 ****************************************************************************************/
@Component
public class RunMgmtProcessor {
		
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_RUNMGMTPROCESSOR;
	public String initializeRunStatus = "";
	public Boolean oldRun = false;
	
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
			
	@Autowired
	GdprInputFetchDaoImpl gdprInputFetchDaoImpl;
	
	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	@Autowired
	TagDataProcessor tagDataProcessor;

	@Autowired
	AnonymizeProcessor depersonalizationProcessor;
	
	/**
	 * This method handled the initialization of the current run
	 * Takes a call whether the old failure run has to be proceeded on or a new run should be instantiated	 
	 * @param runName The description of the current run is being maintained
	 * @return The RunId is returned back to controller and this is passed on to all methods 
	 */
	public long initializeRun(String runName) throws GdprException {
		String CURRENT_METHOD = "initializeRun";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		long runId = 0;
		Boolean exceptionOccured = false;
		Date moduleStartDateTime = null;
		String errorDetails = "";
		String subModule = "";
		
		try{
			moduleStartDateTime = new Date();	
			//Fetch lastRunId if failed
			
			RunMgmt runMgmt = runMgmtDaoImpl.fetchLastRunDetail();
			if(runMgmt != null){
				String runStatus = runMgmt.getRunStatus().trim();
				if(GlobalConstants.STATUS_FAILURE.equalsIgnoreCase(runStatus) ||
						GlobalConstants.STATUS_INPROGRESS.equalsIgnoreCase(runStatus)) {
					initializeRunStatus = GlobalConstants.MSG_OLD_RUN_NOT_SUCCESSFUL;
					oldRun = true;
				} else if(GlobalConstants.STATUS_RERUN.equalsIgnoreCase(runStatus)) {
					oldRun = true;
					runId = runMgmt.getRunId();
					subModule = GlobalConstants.SUB_MODULE_RERUN_INITIALIZE;				
					String pastRunStatus = reinitiatePendingActivity(runId);
					initializeRunStatus = GlobalConstants.MSG_OLD_RUN_FETCHED + runId+". "+pastRunStatus;
				}
			} 
			if(! oldRun) {
				runId = initiateNewRun(runName);
				subModule = GlobalConstants.SUB_MODULE_RUN_INITIALIZE;
				initializeRunStatus = GlobalConstants.MSG_NEW_RUN_INITIATED + runId;
			}
		} catch(GdprException exception) {
			initializeRunStatus = initializeRunStatus + "Facing issues in initializing the Run. ";
			errorDetails = exception.getStackTrace().toString();
		}
		try {
			if(runId > 0) {
				String moduleStatus = exceptionOccured ? GlobalConstants.STATUS_FAILURE : GlobalConstants.STATUS_SUCCESS;
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, subModule,
						moduleStatus, moduleStartDateTime, new Date(), initializeRunStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
			}
		} catch(GdprException exception) {
			exceptionOccured = true;
			initializeRunStatus = initializeRunStatus + exception.getExceptionMessage();
		}		
		return runId;
	}		
			
	/**
	 * A new run is initiated in this method. An entry is made in the RunMgmt table for this run
	 * @param runName The description of the current run is being maintained
	 * @return The RunId is returned back to controller and this is passed on to all methods
	 */
	public long initiateNewRun(String runName) throws GdprException {
		String CURRENT_METHOD = "initiateNewRun";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: New run initiation in progress.");
		long runId = 0;		
		String errorDetails = "";
		
		try {
			runMgmtDaoImpl.initiateNewRun(runName);
			runId = runMgmtDaoImpl.fetchLastRunDetail().getRunId(); 
		} catch(Exception exception) {	
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+GlobalConstants.ERR_NEW_RUN_INITIATION);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
			throw new GdprException(GlobalConstants.ERR_NEW_RUN_INITIATION, errorDetails);
		}
		return runId;
	}
	
	public GdprInput loadCountryDetail() throws GdprException {
		String CURRENT_METHOD = "loadCountryDetail";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Loading the GdprInput Object. ");
		
		GdprInput gdprInput = new GdprInput();
		Map<String, List<String>> mapRegionCountry = new HashMap<String, List<String>>();
		List<String> lstRegion = new ArrayList<String>();
		List<String> lstCountryCode = new ArrayList<String>();
		
		String loadCountryDtlStatus = "";
		String errorDetails = "";
				
		try {
			List<Country> lstCountry = gdprInputFetchDaoImpl.fetchAllCountries(); 
			
			String prevRegion = null;
			List<String> lstRegionsCountry = new ArrayList<String>();
			Set<String> setRegion = new HashSet<String>();
			
			for(Country country : lstCountry) {
				String currentRegion = country.getRegion();
				String currentCountry = country.getCountryCode();
				
				if(prevRegion != null && !(prevRegion.equalsIgnoreCase(currentRegion))){
					mapRegionCountry.put(prevRegion, lstRegionsCountry);
					lstRegionsCountry = new ArrayList<String>();				
				}
				lstRegionsCountry.add(currentCountry);
				lstCountryCode.add(currentCountry);
				setRegion.add(currentRegion);
				prevRegion = currentRegion;
			}
			mapRegionCountry.put(prevRegion, lstRegionsCountry);
			lstRegion.addAll(setRegion);
			
			gdprInput.setLstRegion(lstRegion);
			gdprInput.setLstCountry(lstCountryCode);
			gdprInput.setMapRegionCountry(mapRegionCountry);
			loadCountryDtlStatus = GlobalConstants.MSG_LOAD_FORM;
		} catch (Exception exception){			
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+GlobalConstants.ERR_LOAD_FORM);
			exception.printStackTrace();
			loadCountryDtlStatus = GlobalConstants.ERR_LOAD_FORM;
			errorDetails = exception.getMessage();
			throw new GdprException(loadCountryDtlStatus, errorDetails);
		}
		return gdprInput;
	}
	
	public String reinitiatePendingActivity(long runId) throws GdprException {
		String CURRENT_METHOD = "reinitiatePendingActivity";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method");
		String pastRunStatus = "";
		Boolean anonymizeRerun = false;		
		
		try{
			List<RunModuleMgmt> lstRunModuleMgmt = runMgmtDaoImpl.fetchLastModuleMgmtDetail(runId);			
			if(lstRunModuleMgmt != null && lstRunModuleMgmt.size() > 0) {
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: lstRunModuleMgmt size : "+lstRunModuleMgmt.size());
			
				for(RunModuleMgmt runModuleMgmt : lstRunModuleMgmt) {
					String subModuleName = runModuleMgmt.getSubModuleName();
					String moduleStatus = runModuleMgmt.getModuleStatus();
					
					System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: subModuleName : "+subModuleName);
					System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: moduleStatus : "+moduleStatus);
					System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: pastRunStatus : "+pastRunStatus);
					
					if(GlobalConstants.SUB_MODULE_REORGANIZE_JOB_INITIALIZE.equalsIgnoreCase(subModuleName)) {
						if(GlobalConstants.STATUS_FAILURE.equalsIgnoreCase(moduleStatus)) {
							pastRunStatus = "The reorganizing of input data has failed. Please refresh the DB and rerun from start. ";
							break;
						} else {
							pastRunStatus = "The reorganizing of input data was successful. ";
							continue;
						}
					}
					if(GlobalConstants.SUB_MODULE_BACKUPSERVICE_JOB_INITIALIZE.equalsIgnoreCase(subModuleName)) {
						if(GlobalConstants.STATUS_FAILURE.equalsIgnoreCase(moduleStatus)) {
							pastRunStatus = pastRunStatus + "The backup of the run has failed. Please refresh the DataLoad, "
									+ "GDPR_Depersonalization table and rerun from start. ";
							break;
						} else {
							pastRunStatus = pastRunStatus + "The backup of the run was successful. ";
							continue;
						}
					}
					if(GlobalConstants.SUB_MODULE_TAG_JOB_INITIALIZE.equalsIgnoreCase(subModuleName)) { 
						if (GlobalConstants.STATUS_FAILURE.equalsIgnoreCase(moduleStatus)) {
							pastRunStatus = pastRunStatus + "The tagging of the data has failed. Please refresh the failed tables data "
									+ "and update the status of the module to reRun and restart the run. ";						
							break;
						} else if(GlobalConstants.STATUS_RERUN.equalsIgnoreCase(moduleStatus)) {
							pastRunStatus = pastRunStatus + "The tagging of the data is restarted. ";
							tagDataProcessor.taggingInitialize(runId);
							break;
						} else {
							pastRunStatus = pastRunStatus + "The tagging of the data was successful. ";
							anonymizeRerun = true;
							continue;
						}
					}						
					if(GlobalConstants.SUB_MODULE_ANONYMIZE_JOB_INITIALIZE.equalsIgnoreCase(subModuleName)) { 
						if(GlobalConstants.STATUS_FAILURE.equalsIgnoreCase(moduleStatus)) {
							pastRunStatus = pastRunStatus + "The archival of the data has failed. Please refresh the failed tables data "
									+ "and update the status of the module to reRun and restart the run. ";						
						} else if(GlobalConstants.STATUS_RERUN.equalsIgnoreCase(moduleStatus)) {							
							anonymizeRerun = true;
						} else if(GlobalConstants.STATUS_SUCCESS.equalsIgnoreCase(moduleStatus)) {
							anonymizeRerun = false;
							pastRunStatus = pastRunStatus + "The archival of the data was successful. No re-run is required.";
						}
						break;
					}					
				}		
				if(anonymizeRerun) {
					pastRunStatus = pastRunStatus + "The archival of the data is restarted. ";
					depersonalizationProcessor.depersonalizationInitialize(runId);
				}
			}
		} catch (Exception exception){			
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+GlobalConstants.ERR_LOAD_FORM);
			throw new GdprException("Facing issues while verifying pending activity", exception.getMessage());
		}
		return pastRunStatus;
	}
}