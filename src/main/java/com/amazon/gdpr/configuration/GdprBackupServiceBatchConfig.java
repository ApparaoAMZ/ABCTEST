package com.amazon.gdpr.configuration;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

import com.amazon.gdpr.batch.BackupJobCompletionListener;
import com.amazon.gdpr.dao.BackupServiceDaoImpl;
import com.amazon.gdpr.dao.BackupTableProcessorDaoImpl;
import com.amazon.gdpr.dao.GdprInputDaoImpl;
import com.amazon.gdpr.dao.GdprInputFetchDaoImpl;
import com.amazon.gdpr.dao.GdprOutputDaoImpl;
import com.amazon.gdpr.dao.RunSummaryDaoImpl;

import com.amazon.gdpr.model.BackupServiceData;
import com.amazon.gdpr.model.gdpr.input.ImpactTable;
import com.amazon.gdpr.model.gdpr.output.BackupTableDetails;
import com.amazon.gdpr.model.gdpr.output.RunErrorMgmt;
import com.amazon.gdpr.model.gdpr.output.RunSummaryMgmt;
import com.amazon.gdpr.processor.ModuleMgmtProcessor;
import com.amazon.gdpr.processor.TagQueryProcessor;
import com.amazon.gdpr.util.GdprException;
import com.amazon.gdpr.util.GlobalConstants;
import com.amazon.gdpr.util.SqlQueriesConstant;

/****************************************************************************************
 * This Configuration handles the Reading of GDPR.RUN_SUMMARY_MGMT table and
 * Writing into GDPR.BKP Tables
 ****************************************************************************************/
@EnableScheduling
@EnableBatchProcessing
@EnableAutoConfiguration(exclude = { DataSourceAutoConfiguration.class })
@Configuration
public class GdprBackupServiceBatchConfig {
	private static String CURRENT_CLASS = "GdprBackupServiceBatchConfig";

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Autowired
	GdprOutputDaoImpl gdprOutputDaoImpl;

	@Autowired
	GdprInputDaoImpl gdprInputDaoImpl;
	
	@Autowired
	RunSummaryDaoImpl runSummaryDaoImpl;
	@Autowired
	GdprInputFetchDaoImpl gdprInputFetchDaoImpl;

	@Autowired
	public BackupServiceDaoImpl backupServiceDaoImpl;
	@Autowired
	ModuleMgmtProcessor moduleMgmtProcessor;
	
	@Autowired
	TagQueryProcessor tagQueryProcessor;
	
	@Autowired
	private BackupTableProcessorDaoImpl backupTableProcessorDaoImpl;

	@Autowired
	@Qualifier("gdprJdbcTemplate")
	private JdbcTemplate jdbcTemplate;

	public long runId;
	public Date moduleStartDateTime;
	
	@Bean
	@StepScope
	public JdbcCursorItemReader<BackupServiceData> backupServiceReader(@Value("#{jobParameters[RunId]}") long runId,
			@Value("#{jobParameters[RunSummaryId]}") long runSummaryId, 
			@Value("#{jobParameters[StartDate]}") Date moduleStartDateTime) {
	String CURRENT_METHOD = "BackupreaderClass";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method. " + runId);
		JdbcCursorItemReader<BackupServiceData> reader = null;
		List<BackupTableDetails> lstBackupTableDetails = null;
		Map<String, List<String>> mapbackuptable = null;
		Map<String, String> impactFieldAnonymizationMap = null;
		
		Map<String, String> mapSummaryInputs = new HashMap<String, String>();
		
        RunSummaryMgmt runSummaryMgmt = null;
        List<String> lstcls=null;
		
		runSummaryMgmt = runSummaryDaoImpl.fetchRunSummaryDetail(runId, runSummaryId);
		
		impactFieldAnonymizationMap=gdprInputDaoImpl.fetchImpactFieldAnonyMizationMap();
			
		lstBackupTableDetails = backupTableProcessorDaoImpl.fetchBackupTableDetails();
		mapbackuptable = lstBackupTableDetails.stream().collect(Collectors.toMap(BackupTableDetails::getBackupTableName, bkp -> {
			List list = new ArrayList<String>();
			list.add(bkp.getBackupTablecolumn());
			return list;
		}, (s, a) -> {
			s.add(a.get(0));
			return s;
		}));
		if (runSummaryMgmt != null) {

			String backpQuery = runSummaryMgmt.getBackupQuery();
			String selectColumns = backpQuery.substring("SELECT ".length(), backpQuery.indexOf(" FROM "));
			
			List<String> stringList = Arrays.asList(selectColumns.split(","));
			List<String> trimmedStrings = new ArrayList<String>();
			for (String s : stringList) {
				trimmedStrings.add(s.trim());
			}
			
			if(impactFieldAnonymizationMap.containsValue("DELETE ROW")) {
				lstcls=mapbackuptable.get(runSummaryMgmt.getImpactTableName().toLowerCase());		
					String tranType=impactFieldAnonymizationMap.get(runSummaryMgmt.getImpactTableName().toUpperCase()+"-"+trimmedStrings.get(0));
				    if(tranType!=null&&tranType.toUpperCase().contains("DELETE ROW")) {
					System.out.println("tranType::"+tranType);
					trimmedStrings=lstcls;
					trimmedStrings.remove("id");
					}
				 }
			Set<String> hSet = new HashSet<String>(trimmedStrings);

			selectColumns = hSet.stream().map(String::valueOf).collect(Collectors.joining(","));
			String splittedValues = hSet.stream().map(s -> s + "=excluded." + s).collect(Collectors.joining(","));

			//String completeQuery = fetchCompleteBackupDataQuery(impactTableName, mapImpacttable, backupServiceInput,
					//selectColumns, runId);
			mapSummaryInputs.put(GlobalConstants.KEY_CATEGORY_ID, String.valueOf(runSummaryMgmt.getCategoryId()));
			mapSummaryInputs.put(GlobalConstants.KEY_COUNTRY_CODE,runSummaryMgmt.getCountryCode() );
			String bkpselectQuery = tagQueryProcessor.fetchBackupDataSelQuery(runSummaryMgmt.getImpactTableName(),  mapSummaryInputs,
			    runId);
			String backupDataInsertQuery = "INSERT INTO BKP." + runSummaryMgmt.getImpactTableName() + " (ID," + selectColumns + ") "
					+"SELECT ID,"+selectColumns+" FROM  SF_ARCHIVE."+runSummaryMgmt.getImpactTableName()+" WHERE ID=? ON CONFLICT (id) DO UPDATE " + "  SET " + splittedValues + ";";
			bkpselectQuery = bkpselectQuery.replaceAll("TAG.", "SF_ARCHIVE.");
			backupDataInsertQuery = backupDataInsertQuery.replaceAll("TAG.", "SF_ARCHIVE.");
			
			reader = new JdbcCursorItemReader<BackupServiceData>();
			reader.setDataSource(dataSource);
			reader.setSql(bkpselectQuery);
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: bkpselectQuery "+bkpselectQuery);
			reader.setRowMapper(new BackupServiceInputRowMapper(runSummaryMgmt.getSummaryId(),runId, bkpselectQuery, 
					backupDataInsertQuery,runSummaryMgmt.getImpactTableName(), runSummaryMgmt.getCategoryId(), runSummaryMgmt.getCountryCode()));
		}
		return reader;
	}

	
	public class BackupServiceInputRowMapper implements RowMapper<BackupServiceData> {
		private String CURRENT_CLASS		 		= "BackupServiceInputRowMapper";
		String tableName;
		String bkpselectQuery;
		String backupDataInsertQuery;
		//AnonymizeTable updatedAnonymizeTable;
		long runId;
		long runSummaryId;
		int categoryId;
		String countryCode;
		
		public BackupServiceInputRowMapper(long runId, long runSummaryId,String bkpselectQuery,  String backupDataInsertQuery,String tableName, int categoryId, String countryCode) {			
			this.runId = runId;
			this.runSummaryId = runSummaryId;
			this.bkpselectQuery = bkpselectQuery;
			this.backupDataInsertQuery = backupDataInsertQuery;
			this.tableName = tableName;
			this.categoryId = categoryId;
			this.countryCode = countryCode;
		}
		
		@Override
		public BackupServiceData mapRow(ResultSet rs, int rowNum) throws SQLException {
			String CURRENT_METHOD = "mapRow";			
			return new BackupServiceData(runId, runSummaryId,bkpselectQuery,backupDataInsertQuery, rs.getInt("ID"), tableName, categoryId, countryCode);					
		}
	}

	// @Scope(value = "step")
	public class GdprBackupServiceProcessor implements ItemProcessor<BackupServiceData, BackupServiceData> {
		private String CURRENT_CLASS = "GdprBackupServiceProcessor";
		private Map<String, String> impactFieldAnonymizationMap = null;
		private List<ImpactTable> impactTableDtls = null;
		Map<String, ImpactTable> mapImpacttable = null;
		Map<Integer, ImpactTable> mapWithIDKeyImpacttable = null;
		
		RunErrorMgmt runErrorMgmt = null;
		String strLastFetchDate = null;
		List<BackupTableDetails> lstBackupTableDetails = null;
		Map<String, List<String>> mapbackuptable = null;
				
		@BeforeStep
		public void beforeStep(final StepExecution stepExecution) throws GdprException {
			String CURRENT_METHOD = "BackupbeforeStep";
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method23w23. ");

			Boolean exceptionOccured = false;
			String backupData = CURRENT_CLASS +":::"+CURRENT_METHOD+"::";
			String errorDetails = "";
		
			System.out.println(
					CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: runId " + runId);
			
		}

		@Override
		public BackupServiceData process(BackupServiceData arg0) {
			String CURRENT_METHOD = "Backupprocess";
			// System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside
			// method. ");

			
			return arg0;
		}
	}

	public class BackupServiceOutputWriter<T> implements ItemWriter<BackupServiceData> {
		private String CURRENT_CLASS = "BackupServiceOutputWriter";
		private Date moduleStartDateTime;
		private long runSummaryId;		
		

		public BackupServiceOutputWriter(Date moduleStartDateTime, long runSummaryId) {
			this.moduleStartDateTime = moduleStartDateTime;
			this.runSummaryId = runSummaryId;			
		}

		@Override
		public void write(List<? extends BackupServiceData> lstBackupServiceOutput) {
			String CURRENT_METHOD = "write";
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method. ");
			Boolean exceptionOccured = false;
			String backupDataStatus = "";
			String errorDetails = "";
			
			
			if(lstBackupServiceOutput != null && lstBackupServiceOutput.size() > 0){					
				BackupServiceData firstRowBackupService = (BackupServiceData) lstBackupServiceOutput.get(0);	
				runId = firstRowBackupService.getRunId();
				String dataInsertQuery = firstRowBackupService.getBackupDataInsertQuery();
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: depersonalizeQuery "+dataInsertQuery);
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: lstAnonymizeTable size "+lstBackupServiceOutput.size());
									
				backupServiceDaoImpl.backupInsertTables(lstBackupServiceOutput, dataInsertQuery);
				//anonymizeDataStatus = "Anonymization completed for runSummaryId - "+ runSummaryId;
			}
				
			
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Bean
	public Step gdprBackupServiceStep() {
		String CURRENT_METHOD = "gdprBackupServiceStep";
		System.out.println(CURRENT_CLASS + " :::  " + CURRENT_METHOD + " :: Inside method. ");
		Step step = null;
		Boolean exceptionOccured = false;
		String backupDataStatus = "";
		String errorDetails = "";
		
			step = stepBuilderFactory.get("gdprBackupServiceStep")
					.<BackupServiceData, BackupServiceData>chunk(1000)
					.reader(backupServiceReader(0,0, new Date())).processor(new GdprBackupServiceProcessor())
					.writer(new BackupServiceOutputWriter<Object>(new Date(), 0))
					.listener(bkplistener()).build();
		return step;
	}

	@Bean
	public Job processGdprBackupServiceJob() {
		String CURRENT_METHOD = "processGdprBackupServiceJob";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method. ");
		Boolean exceptionOccured = false;
		String backupDataStatus = "";
		Job job = null;
		String errorDetails = "";

			job = jobBuilderFactory.get("processGdprBackupServiceJob").incrementer(new RunIdIncrementer())
					.listener(backupListener(GlobalConstants.JOB_BACKUP_SERVICE_LISTENER)).flow(gdprBackupServiceStep())
					.end().build();
				return job;
	}

	@Bean
	public BackupCountListener bkplistener() {
	    return new BackupCountListener();
	}
	@Bean
	public JobExecutionListener backupListener(String jobRelatedName) {
		String CURRENT_METHOD = "Backuplistener";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method. ");

		return new BackupJobCompletionListener(jobRelatedName);
	}
	
}
