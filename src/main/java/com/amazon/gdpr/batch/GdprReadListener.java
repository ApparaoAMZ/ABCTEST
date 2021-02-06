package com.amazon.gdpr.batch;

import java.util.Date;

import org.springframework.batch.core.ItemReadListener;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

@SuppressWarnings("rawtypes")
public class GdprReadListener implements ItemReadListener {
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_GDPRREADLISTENER;
	String impactedClass = "";
	Date moduleStartDateTime = null;
	long runId;
	
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
		
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
	
	public GdprReadListener(String impactedClass, Date moduleStartDateTime, long runId){
		this.impactedClass = impactedClass;
		this.moduleStartDateTime = moduleStartDateTime;
		this.runId = runId;
	}

	@Override
	public void beforeRead() {
		// TODO Auto-generated method stub		
	}

	@Override
	public void afterRead(Object item) {
		// TODO Auto-generated method stub		
	}

	@Override
	public void onReadError(Exception exception) {
		String CURRENT_METHOD = "reader";
		Boolean exceptionOccured = true;
		String reOrganizeDataStatus  = "Facing issues in reading GDPR_Depersonalization table. " ;
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + reOrganizeDataStatus);
		exception.printStackTrace();
		String errorDetails = exception.getMessage();
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: getMessage" + exception.getMessage());
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: getLocalizedMessage" + exception.getLocalizedMessage());
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: getStackTrace" + exception.getStackTrace());
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: toString" + exception.toString());
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: getClass" + exception.getClass());
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: getCause" + exception.getCause());
		
		try {
			if(exceptionOccured){
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_INITIALIZATION, 
						GlobalConstants.SUB_MODULE_REORGANIZE_JOB_INITIALIZE, GlobalConstants.STATUS_FAILURE, moduleStartDateTime, 
						new Date(), reOrganizeDataStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
				runMgmtDaoImpl.updateRunComments(runId, reOrganizeDataStatus);				
			}
		} catch(GdprException gdprException) {
			reOrganizeDataStatus = reOrganizeDataStatus + gdprException.getExceptionMessage();
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+reOrganizeDataStatus);
			errorDetails = errorDetails + exception.getMessage();
		}
		try {
			throw new GdprException(reOrganizeDataStatus, errorDetails);
		} catch(Exception exc){
			
		}
		
	}
	

}
