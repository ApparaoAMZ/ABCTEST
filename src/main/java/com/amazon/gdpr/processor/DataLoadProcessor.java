package com.amazon.gdpr.processor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;

/****************************************************************************************
 * This processor Fetches and loads the data from the Data_Load Table
 ****************************************************************************************/
@Component
public class DataLoadProcessor {
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_DATALOAD_PROCESSOR;
		
	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;
	
	/**
	 * After the load of each table the Data_Load Table is loaded
	 * @param dataLoad The details of the DataLoad are passed on as input
	 */
	public void updateDataLoad(long runId) throws GdprException {
		String CURRENT_METHOD = "updateDataLoad";		
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Data Load update in progress.");
		
		try {
			gdprOutputDaoImpl.updateDataLoad(runId);
		} catch(Exception exception) {
			String dataLoadStatus = GlobalConstants.ERR_DATA_LOAD_UPDATE;
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Exception occured");
			exception.printStackTrace();
			String errorDetails = exception.getMessage();
			throw new GdprException(dataLoadStatus, errorDetails); 
		}		
	}
}