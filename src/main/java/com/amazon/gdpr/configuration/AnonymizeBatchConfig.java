package com.amazon.gdpr.configuration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.amazon.gdpr.batch.AnonymizeJobCompletionListener;
import com.amazon.gdpr.dao.BackupServiceDaoImpl;
import com.amazon.gdpr.dao.GdprInputDaoImpl;
import com.amazon.gdpr.dao.RunMgmtDaoImpl;
import com.amazon.gdpr.dao.RunSummaryDaoImpl;
import com.amazon.gdpr.model.archive.AnonymizeTable;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
import com.amazon.gdpr.model.gdpr.output.RunSummaryMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * This Configuration handles the anonymization of SF_ARCHIVE.<Tables>
 ****************************************************************************************/
@SuppressWarnings("unused")
@EnableScheduling
@EnableBatchProcessing
@EnableAutoConfiguration(exclude = {DataSourceAutoConfiguration.class})
@Configuration
public class AnonymizeBatchConfig {
	private static String CURRENT_CLASS		 		= GlobalConstants.CLS_ANONYMIZE_BATCH_CONFIG;
	
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	public DataSource dataSource;
	
	@Autowired
	RunSummaryDaoImpl runSummaryDaoImpl;

	@Autowired
	GdprInputDaoImpl gdprInputDaoImpl;

	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
		
	@Autowired
	public BackupServiceDaoImpl backupServiceDaoImpl;
	
	@Autowired
	RunMgmtDaoImpl runMgmtDaoImpl;
	
	@Autowired
	@Qualifier("gdprJdbcTemplate")
	JdbcTemplate jdbcTemplate;
	
	public long runId;
	public long runSummaryId;	
	public Date moduleStartDateTime;
	
	@Bean
	@StepScope
	public JdbcCursorItemReader<AnonymizeTable> anonymizeTableReader(@Value("#{jobParameters[RunId]}") long runId,
			@Value("#{jobParameters[RunSummaryId]}") long runSummaryId, 
			@Value("#{jobParameters[StartDate]}") Date moduleStartDateTime) {
		String CURRENT_METHOD = "reader";
		//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		
		JdbcCursorItemReader<AnonymizeTable> reader = null;
		String anonymizeDataStatus = "";
		String errorDetails = "";
		Boolean exceptionOccured = false;
		AnonymizeTable anonymizeTable = null;
		RunSummaryMgmt runSummaryMgmt = null;
		
		runSummaryMgmt = runSummaryDaoImpl.fetchRunSummaryDetail(runId, runSummaryId);
		if (runSummaryMgmt != null) {
			String taggedQueryFetch = "SELECT DISTINCT ID FROM TAG." +runSummaryMgmt.getImpactTableName()+
					" WHERE CATEGORY_ID = "+runSummaryMgmt.getCategoryId() +" AND COUNTRY_CODE = \'"+runSummaryMgmt.getCountryCode()+
					"\' AND STATUS = \'SCHEDULED\'";
			reader = new JdbcCursorItemReader<AnonymizeTable>();
			reader.setDataSource(dataSource);
			reader.setSql(taggedQueryFetch);
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: taggedQueryFetch "+taggedQueryFetch);
			reader.setRowMapper(new AnonymizeTableRowMapper(runId, runSummaryMgmt.getDepersonalizationQuery(), 
					runSummaryMgmt.getImpactTableName(), runSummaryMgmt.getCategoryId(), runSummaryMgmt.getCountryCode()));
		}
		return reader;
		/*try {
			runSummaryMgmt = runSummaryDaoImpl.fetchRunSummaryDetail(runId, runSummaryId);
		} catch (Exception exception) {
			exceptionOccured = true;
			anonymizeDataStatus  = "Facing issues in reading runSummaryDetail for run - "+runId+" for run summary id -"
						+runSummaryId;
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + anonymizeDataStatus);
			exception.printStackTrace();
			errorDetails = exception.getMessage();
		}
		try {
			if(exceptionOccured){
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
						GlobalConstants.SUB_MODULE_ANONYMIZE_DATA, GlobalConstants.STATUS_FAILURE, moduleStartDateTime, 
						new Date(), anonymizeDataStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
				runMgmtDaoImpl.updateRunComments(runId, anonymizeDataStatus);
				throw new GdprException(anonymizeDataStatus, errorDetails);
			}
		} catch(GdprException exception) {
			anonymizeDataStatus = anonymizeDataStatus + exception.getExceptionMessage();
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+anonymizeDataStatus);
			errorDetails = errorDetails+exception.getMessage();
			throw new GdprException(anonymizeDataStatus, errorDetails); 
		}
		try {
			if (runSummaryMgmt != null) {
				String taggedQueryFetch = "SELECT DISTINCT ID FROM TAG." +runSummaryMgmt.getImpactTableName()+
						" WHERE CATEGORY_ID = "+runSummaryMgmt.getCategoryId() +" AND COUNTRY_CODE = \'"+runSummaryMgmt.getCountryCode()+
						"\' AND STATUS = \'SCHEDULED\'";
				reader = new JdbcCursorItemReader<AnonymizeTable>();
				reader.setDataSource(dataSource);
				reader.setSql(taggedQueryFetch);
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: taggedQueryFetch "+taggedQueryFetch);
				reader.setRowMapper(new AnonymizeTableRowMapper(runId, runSummaryMgmt.getDepersonalizationQuery(), 
						runSummaryMgmt.getImpactTableName(), runSummaryMgmt.getCategoryId(), runSummaryMgmt.getCountryCode()));
				//anonymizeDataStatus = "Reading tagged tables for Run Summary Id "+runSummaryMgmt.getSummaryId();
			}
		} catch (Exception exception) {
			exceptionOccured = true;
			anonymizeDataStatus  = "Facing issues in reading tagged tables. " ;
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + anonymizeDataStatus);
			errorDetails = exception.getMessage(); 
			exception.printStackTrace();
		}
		try {
			if(exceptionOccured) {
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
						GlobalConstants.SUB_MODULE_ANONYMIZE_DATA, GlobalConstants.STATUS_FAILURE, moduleStartDateTime, 
						new Date(), anonymizeDataStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
				runMgmtDaoImpl.updateRunComments(runId, anonymizeDataStatus);
				throw new GdprException(anonymizeDataStatus, errorDetails);
			}
		} catch(GdprException exception) {
			anonymizeDataStatus = anonymizeDataStatus + exception.getExceptionMessage();
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+anonymizeDataStatus);
			errorDetails = errorDetails + exception.getMessage();
			throw new GdprException(anonymizeDataStatus, errorDetails); 
		}
		return reader;*/
	}
	
	//To set values into Tag Tables Object
	public class AnonymizeTableRowMapper implements RowMapper<AnonymizeTable> {
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_ANONYMIZETABLEROWMAPPER;
		String tableName;
		String depersonalizationQuery;
		//AnonymizeTable updatedAnonymizeTable;
		long runId;
		int categoryId;
		String countryCode;
		
		public AnonymizeTableRowMapper(long runId, String depersonalizationQuery, String tableName, int categoryId, String countryCode) {			
			this.runId = runId;
			this.depersonalizationQuery = depersonalizationQuery;
			this.tableName = tableName;
			this.categoryId = categoryId;
			this.countryCode = countryCode;
		}
		
		@Override
		public AnonymizeTable mapRow(ResultSet rs, int rowNum) throws SQLException {
			String CURRENT_METHOD = "mapRow";			
			return new AnonymizeTable(runId, depersonalizationQuery, rs.getInt("ID"), tableName, categoryId, countryCode);					
		}
	}
	
	public class AnonymizeProcessor implements ItemProcessor<AnonymizeTable, AnonymizeTable> {
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_JOB_ANONYMIZEPROCESSOR;
		//private Map<String, String> mapTableIdToName = null;
		
		@BeforeStep
		public void beforeStep(final StepExecution stepExecution) {
			String CURRENT_METHOD = "beforeStep";
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Job Before Step : "+LocalTime.now());
			
			long currentRun;
			
			JobParameters jobParameters = stepExecution.getJobParameters();
			runId	= jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_ID);
			currentRun 	= jobParameters.getLong(GlobalConstants.JOB_INPUT_JOB_ID);
			runSummaryId = jobParameters.getLong(GlobalConstants.JOB_INPUT_RUN_SUMMARY_ID);
			moduleStartDateTime = jobParameters.getDate(GlobalConstants.JOB_INPUT_START_DATE);
			
			//mapTableIdToName = gdprInputDaoImpl.fetchImpactTableMap(GlobalConstants.IMPACTTABLE_MAP_IDTONAME);			
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);			
		}
		
		@Override
		public AnonymizeTable process(AnonymizeTable arg0) throws Exception {
			String CURRENT_METHOD = "process";		
			//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");		
			//arg0.setTableName(mapTableIdToName.get(arg0.getTableId()));
			return arg0;
		}
	}
	
	public class AnonymizeInputWriter<T> implements ItemWriter<AnonymizeTable> { 
		private String CURRENT_CLASS		 		= GlobalConstants.CLS_ANONYMIZEINPUTWRITER;		
		private Date moduleStartDateTime;
		private long runSummaryId;		
		
		public AnonymizeInputWriter(Date moduleStartDateTime, long runSummaryId) {
			this.moduleStartDateTime = moduleStartDateTime;
			this.runSummaryId = runSummaryId;			
		}

		@Override
		public void write(List<? extends AnonymizeTable> lstAnonymizeTable) {
			String CURRENT_METHOD = "write";
			Boolean exceptionOccured = false;
			String anonymizeDataStatus = "";
			String errorDetails = "";
			long runId = 0;
			
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
			//try {
				if(lstAnonymizeTable != null && lstAnonymizeTable.size() > 0){					
					AnonymizeTable firstRowAnonymizeTable = (AnonymizeTable) lstAnonymizeTable.get(0);	
					runId = firstRowAnonymizeTable.getRunId();
					String depersonalizeQuery = firstRowAnonymizeTable.getDepersonalizationQuery();
					System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: depersonalizeQuery "+depersonalizeQuery);
					System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: lstAnonymizeTable size "+lstAnonymizeTable.size());
										
					backupServiceDaoImpl.anonymizeArchivalTables(lstAnonymizeTable, depersonalizeQuery);
					//anonymizeDataStatus = "Anonymization completed for runSummaryId - "+ runSummaryId;
				}
			/*} catch (Exception exception) {
				exceptionOccured = true;
				anonymizeDataStatus  = "Facing issues while anonymizing data in archival table. ";
				System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + anonymizeDataStatus);
				exception.printStackTrace();
				errorDetails = exception.getStackTrace().toString();
			}
			try {
				if(exceptionOccured){
					RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
							GlobalConstants.SUB_MODULE_ANONYMIZE_DATA, GlobalConstants.STATUS_FAILURE, moduleStartDateTime, 
							new Date(), anonymizeDataStatus, errorDetails);
					moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
					runMgmtDaoImpl.updateRunComments(runId, anonymizeDataStatus);
					throw new GdprException(anonymizeDataStatus, errorDetails);
				}
			} catch(GdprException exception) {
				exceptionOccured = true;
				anonymizeDataStatus = anonymizeDataStatus + exception.getExceptionMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+anonymizeDataStatus);
				errorDetails = errorDetails + exception.getStackTrace().toString();
				throw new GdprException(anonymizeDataStatus, errorDetails); 
			}
			try {
				if(! exceptionOccured) {*/
					if(lstAnonymizeTable != null && lstAnonymizeTable.size() > 0) {					
						AnonymizeTable firstRowAnonymizeTable = (AnonymizeTable) lstAnonymizeTable.get(0);										
						System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: lstAnonymizeTable size "+lstAnonymizeTable.size());							
						String tagQueryUpdate = "UPDATE TAG."+firstRowAnonymizeTable.getTableName()+" SET STATUS = \'CLEARED\'"
								+" WHERE CATEGORY_ID = "+firstRowAnonymizeTable.getCategoryId()+" AND COUNTRY_CODE = \'"
								+firstRowAnonymizeTable.getCountryCode()+"\' AND ID = ? AND RUN_ID = ?";
						backupServiceDaoImpl.updateTagTables(lstAnonymizeTable, tagQueryUpdate);
						//anonymizeDataStatus = anonymizeDataStatus+" Tag tables status updated for runSummaryId - "+ runSummaryId;
					}
				/*} 
			} catch (Exception exception) {
				exceptionOccured = true;
				anonymizeDataStatus  = anonymizeDataStatus + "Facing issues while updating data in tagging table. ";
				System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + anonymizeDataStatus);
				exception.printStackTrace();
				errorDetails = errorDetails + exception.getStackTrace().toString();
			}
			try {
				if(exceptionOccured){
					RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
							GlobalConstants.SUB_MODULE_ANONYMIZE_DATA, GlobalConstants.STATUS_FAILURE, moduleStartDateTime, 
							new Date(), anonymizeDataStatus, errorDetails);
					moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
					runMgmtDaoImpl.updateRunComments(runId, anonymizeDataStatus);
					throw new GdprException(anonymizeDataStatus, errorDetails);
				}
			} catch(GdprException exception) {
				anonymizeDataStatus = anonymizeDataStatus + exception.getExceptionMessage();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+anonymizeDataStatus);
				errorDetails = errorDetails + exception.getStackTrace().toString();
				throw new GdprException(anonymizeDataStatus, errorDetails); 
			}*/
		}
	}
		
	@Bean
	public Step anonymizeStep() {
		String CURRENT_METHOD = "anonymizeStep";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		
		Step step = null;
		Boolean exceptionOccured = false;				
		String anonymizeDataStatus = "";
		String errorDetails = "";
	
		//try {			
			step = stepBuilderFactory.get(CURRENT_METHOD)
				.<AnonymizeTable, AnonymizeTable> chunk(SqlQueriesConstant.BATCH_ROW_COUNT)
				.reader(anonymizeTableReader(0,0, new Date()))
				.processor(new AnonymizeProcessor())
				.writer(new AnonymizeInputWriter<Object>(new Date(), 0))
				.listener(listener())
				.build();
		/*} catch (Exception exception) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: " + GlobalConstants.ERR_TAGGING_LOAD);
			exceptionOccured = true;
			exception.printStackTrace();
			anonymizeDataStatus  = GlobalConstants.ERR_TAGGING_LOAD ;
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Exception : "+anonymizeDataStatus);
			errorDetails = exception.getStackTrace().toString();
		}
		try {
			if(exceptionOccured) {
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
						GlobalConstants.SUB_MODULE_ANONYMIZE_DATA, GlobalConstants.STATUS_FAILURE, moduleStartDateTime, 
						new Date(), anonymizeDataStatus, errorDetails);				
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
				runMgmtDaoImpl.updateRunComments(runId, anonymizeDataStatus);
				throw new GdprException(anonymizeDataStatus, errorDetails);
			}
		} catch(GdprException exception) {
			anonymizeDataStatus = anonymizeDataStatus + exception.getExceptionMessage();
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+anonymizeDataStatus);
			errorDetails = errorDetails + exception.getStackTrace().toString();
			throw new GdprException(anonymizeDataStatus, errorDetails); 
		}*/
		return step;
	}
			
	@Bean
	public Job processAnonymizeJob() {
		String CURRENT_METHOD = "processAnonymizeJob";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Before Batch Process : "+LocalTime.now());
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		
		Job job = null;
		Boolean exceptionOccured = false;
		String anonymizeDataStatus = "";
		String errorDetails = "";
		
		//try {
			job = jobBuilderFactory.get(CURRENT_METHOD)
					.incrementer(new RunIdIncrementer()).listener(anonymizeListener(GlobalConstants.JOB_ANONYMIZE))										
					.flow(anonymizeStep())
					.end()
					.build();
		/*} catch(Exception exception) {
			exceptionOccured = true;
			anonymizeDataStatus = GlobalConstants.ERR_ANONYMIZE_JOB_RPOCESS;
			exception.printStackTrace();
			errorDetails = exception.getStackTrace().toString();
		}
		try {
			if(exceptionOccured) {
				RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_DEPERSONALIZATION, 
						GlobalConstants.SUB_MODULE_ANONYMIZE_DATA, GlobalConstants.STATUS_FAILURE, moduleStartDateTime, 
						new Date(), anonymizeDataStatus, errorDetails);
				moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
				runMgmtDaoImpl.updateRunComments(runId, anonymizeDataStatus);
				throw new GdprException(anonymizeDataStatus, errorDetails);
			}
		} catch(GdprException exception) {
			anonymizeDataStatus = anonymizeDataStatus + exception.getExceptionMessage();
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+anonymizeDataStatus);
			errorDetails = errorDetails + exception.getStackTrace().toString();
			throw new GdprException(anonymizeDataStatus, errorDetails); 
		}*/
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: After Batch Process : "+LocalTime.now());
		return job;
	}

	@Bean
	public ItemCountListener listener() {
	    return new ItemCountListener();
	}
	
	@Bean
	public JobExecutionListener anonymizeListener(String jobRelatedName) {
		String CURRENT_METHOD = "listener";
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Inside method. ");
		//System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Job Completion listener : "+LocalTime.now());
		System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: runId "+runId);
		return new AnonymizeJobCompletionListener(jobRelatedName);
	}
}