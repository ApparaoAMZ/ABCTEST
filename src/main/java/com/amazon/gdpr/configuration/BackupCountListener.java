package com.amazon.gdpr.configuration;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.beans.factory.annotation.Autowired;

import com.amazon.gdpr.dao.RunSummaryDaoImpl;
import com.amazon.gdpr.util.GlobalConstants;

public class BackupCountListener implements ChunkListener { 
	private static String CURRENT_CLASS		 		= "BackupCountListener";	
	
	@Autowired
	RunSummaryDaoImpl runSummaryDaoImpl;
	
	@Override
    public void beforeChunk(ChunkContext context) {
		String CURRENT_METHOD = "beforeChunk";
    	
		JobParameters jobParameters = context.getStepContext().getStepExecution().getJobParameters();
		long runId	= jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_ID);
		long currentRun 	= jobParameters.getLong(GlobalConstants.JOB_INPUT_JOB_ID);
    	long runSummaryId = jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_SUMMARY_ID);
    	
    	System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Before Chunk : runId : "+runId+" currentRun : "
    			+currentRun+" runSummaryId : "+runSummaryId);
    }
 
    @Override
    public void afterChunk(ChunkContext context) { 
    	String CURRENT_METHOD = "afterChunk";
    	
    	JobParameters jobParameters = context.getStepContext().getStepExecution().getJobParameters();
		long runId	= jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_ID);
		long currentRun 	= jobParameters.getLong(GlobalConstants.JOB_INPUT_JOB_ID);
    	long runSummaryId = jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_SUMMARY_ID);
    	    	
    	int backupCount = context.getStepContext().getStepExecution().getWriteCount(); 
    	runSummaryDaoImpl.backupCountUpdate(backupCount, runSummaryId);
    	System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: After Chunk : runId : "+runId+" currentRun : "
    			+currentRun+" runSummaryId : "+runSummaryId+" Write Count : "+backupCount);        
    }
     
    @Override
    public void afterChunkError(ChunkContext context) {
    }
}