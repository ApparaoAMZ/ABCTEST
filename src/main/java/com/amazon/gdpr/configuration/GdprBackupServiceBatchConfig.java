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
import com.amazon.gdpr.model.BackupServiceInput;
import com.amazon.gdpr.model.BackupServiceOutput;
import com.amazon.gdpr.model.gdpr.input.ImpactTable;
import com.amazon.gdpr.model.gdpr.output.BackupTableDetails;
import com.amazon.gdpr.model.gdpr.output.RunErrorMgmt;
import com.amazon.gdpr.model.gdpr.output.RunModuleMgmt;
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
	public JdbcCursorItemReader<BackupServiceInput> backupServiceReader(@Value("#{jobParameters[RunId]}") long runId) {
		String gdprSummaryDataFetch = SqlQueriesConstant.GDPR_SUMMARYDATA_FETCH;
		String CURRENT_METHOD = "BackupreaderClass";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method. " + runId);
		JdbcCursorItemReader<BackupServiceInput> reader = null;
		Boolean exceptionOccured = false;
		String backupDataStatus = "";
		String errorDetails = "";

			reader = new JdbcCursorItemReader<BackupServiceInput>();
			reader.setDataSource(dataSource);
			reader.setSql(gdprSummaryDataFetch + runId);
			reader.setRowMapper(new BackupServiceInputRowMapper());

		return reader;
	}

	// To set values into BackupServiceInput Object
	public class BackupServiceInputRowMapper implements RowMapper<BackupServiceInput> {
		private String CURRENT_CLASS = "BackupServiceInputRowMapper";

		@Override
		public BackupServiceInput mapRow(ResultSet rs, int rowNum) throws SQLException {
			// TODO Auto-generated method stub
			String CURRENT_METHOD = "mapRow";

			String runId = rs.getString("RUN_ID");
			return new BackupServiceInput(rs.getLong("SUMMARY_ID"), rs.getLong("RUN_ID"), rs.getInt("CATEGORY_ID"),
					rs.getString("REGION"), rs.getString("COUNTRY_CODE"), rs.getInt("IMPACT_TABLE_ID"),
					rs.getString("BACKUP_QUERY"), rs.getString("DEPERSONALIZATION_QUERY"));
		}
	}

	// @Scope(value = "step")
	public class GdprBackupServiceProcessor implements ItemProcessor<BackupServiceInput, BackupServiceOutput> {
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
			
			try {
				JobParameters jobParameters = stepExecution.getJobParameters();
				runId = jobParameters.getLong(GlobalConstants.JOB_REORGANIZE_INPUT_RUNID);
				long currentRun 	= jobParameters.getLong(GlobalConstants.JOB_INPUT_JOB_ID);
				moduleStartDateTime = jobParameters.getDate(GlobalConstants.JOB_INPUT_START_DATE);
				impactFieldAnonymizationMap=gdprInputDaoImpl.fetchImpactFieldAnonyMizationMap();
				impactTableDtls = gdprInputFetchDaoImpl.fetchImpactTable();
				mapImpacttable = impactTableDtls.stream()
						.collect(Collectors.toMap(ImpactTable::getImpactTableName, i -> i));
				mapWithIDKeyImpacttable = impactTableDtls.stream()
						.collect(Collectors.toMap(ImpactTable::getImpactTableId, i -> i));			
				lstBackupTableDetails = backupTableProcessorDaoImpl.fetchBackupTableDetails();
				mapbackuptable = lstBackupTableDetails.stream().collect(Collectors.toMap(BackupTableDetails::getBackupTableName, bkp -> {
					List list = new ArrayList<String>();
					list.add(bkp.getBackupTablecolumn());
					return list;
				}, (s, a) -> {
					s.add(a.get(0));
					return s;
				}));
				
			} catch (Exception exception) {
				exceptionOccured = true;
				backupData  = backupData + exception.getMessage();				
				errorDetails = Arrays.toString(exception.getStackTrace());
			}
			try {
				if(exceptionOccured){
					RunModuleMgmt runModuleMgmt = new RunModuleMgmt(runId, GlobalConstants.MODULE_BACKUPSERVICE, 
							GlobalConstants.SUB_MODULE_BACKUPSERVICE_DATA, GlobalConstants.STATUS_FAILURE, moduleStartDateTime,  
							new Date(), backupData, errorDetails);
					moduleMgmtProcessor.initiateModuleMgmt(runModuleMgmt);
					throw new GdprException(backupData, errorDetails);
				}
			} catch(GdprException exception) {
				backupData = backupData + GlobalConstants.ERR_MODULE_MGMT_INSERT;
				System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+backupData);
				errorDetails = errorDetails + exception.getExceptionDetail();
				throw new GdprException(backupData, errorDetails); 
			}
			
			System.out.println(
					CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: runId " + runId);
			
		}

		@Override
		public BackupServiceOutput process(BackupServiceInput backupServiceInput) {
			String CURRENT_METHOD = "Backupprocess";
			// System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside
			// method. ");

			Boolean exceptionOccured = false;
			String backupDataStatus = "";

			String backpQuery = backupServiceInput.getBackupQuery();
			int catid = backupServiceInput.getCategoryId();
			String countrycode = backupServiceInput.getCountryCode();
			long sumId = backupServiceInput.getSummaryId();
			int impactTableId = backupServiceInput.getImpactTableId();

			long insertcount = 0;
			BackupServiceOutput backupServiceOutput = null;
			String backupTableName = "";
			//backupTableName = "BKP_" + impactTableName;
			List<String> lstcls=null;
			Map<String, String> mapSummaryInputs = new HashMap<String, String>();
			
				String impactTableName = mapWithIDKeyImpacttable.get(impactTableId).getImpactTableName();
				backupTableName = impactTableName;
				String selectColumns = backpQuery.substring("SELECT ".length(), backpQuery.indexOf(" FROM "));
			
				List<String> stringList = Arrays.asList(selectColumns.split(","));
				List<String> trimmedStrings = new ArrayList<String>();
				for (String s : stringList) {
					trimmedStrings.add(s.trim());
				}
				if(impactFieldAnonymizationMap.containsValue("DELETE ROW")) {
				lstcls=mapbackuptable.get(impactTableName.toLowerCase());		
					String tranType=impactFieldAnonymizationMap.get(impactTableName.toUpperCase()+"-"+trimmedStrings.get(0));
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
				mapSummaryInputs.put(GlobalConstants.KEY_CATEGORY_ID, String.valueOf(catid));
				mapSummaryInputs.put(GlobalConstants.KEY_COUNTRY_CODE, countrycode);
				String completeQuery = tagQueryProcessor.fetchCompleteBackupDataQuery(impactTableName,  mapSummaryInputs,
				   selectColumns, runId);
				completeQuery = completeQuery.replaceAll("TAG.", "SF_ARCHIVE.");
				@SuppressWarnings("unchecked")
				String backupDataInsertQuery = "INSERT INTO BKP." + backupTableName + " (ID," + selectColumns + ") "
						+ completeQuery + " ON CONFLICT (id) DO UPDATE " + "  SET " + splittedValues + ";";
				insertcount = backupServiceDaoImpl.insertBackupTable(backupDataInsertQuery);

				System.out.println("Inserted::"+insertcount+"backupDataInsertQuery::::::#$ " +
				 backupDataInsertQuery);

				backupServiceOutput = new BackupServiceOutput(sumId, runId, insertcount);
			
			return backupServiceOutput;
		}
	}

	public class BackupServiceOutputWriter<T> implements ItemWriter<BackupServiceOutput> {
		private String CURRENT_CLASS = "BackupServiceOutputWriter";

		private final BackupServiceDaoImpl bkpServiceDaoImpl;

		public BackupServiceOutputWriter(BackupServiceDaoImpl bkpServiceDaoImpl) {
			this.bkpServiceDaoImpl = bkpServiceDaoImpl;
		}

		@Override
		public void write(List<? extends BackupServiceOutput> lstBackupServiceOutput) {
			String CURRENT_METHOD = "write";
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method. ");
			Boolean exceptionOccured = false;
			String backupDataStatus = "";
			String errorDetails = "";
			
				bkpServiceDaoImpl.updateSummaryTable(lstBackupServiceOutput);
			
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Bean
	public Step gdprBackupServiceStep() {
		String CURRENT_METHOD = "gdprBackupServiceStep";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method. ");
		Step step = null;
		Boolean exceptionOccured = false;
		String backupDataStatus = "";
		String errorDetails = "";
		
			step = stepBuilderFactory.get("gdprBackupServiceStep")
					.<BackupServiceInput, BackupServiceOutput>chunk(1)
					.reader(backupServiceReader(0)).processor(new GdprBackupServiceProcessor())
					.writer(new BackupServiceOutputWriter(backupServiceDaoImpl)).build();
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
	public JobExecutionListener backupListener(String jobRelatedName) {
		String CURRENT_METHOD = "Backuplistener";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method. ");

		return new BackupJobCompletionListener(jobRelatedName);
	}
	
}
