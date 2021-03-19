package com.amazon.gdpr.processor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazon.gdpr.dao.GdprInputFetchDaoImpl;
import com.amazon.gdpr.model.BackupServiceInput;
import com.amazon.gdpr.model.gdpr.input.ImpactTable;
import com.amazon.gdpr.model.gdpr.output.RunSummaryMgmt;
import com.amazon.gdpr.util.GlobalConstants;

@Component
public class TagQueryProcessor {
	
	public static String CURRENT_CLASS = GlobalConstants.CLS_TAGQUERYPROCESSOR;
	
	@Autowired
	GdprInputFetchDaoImpl gdprInputFetchDaoImpl;
	
	public String tagQueryProcessStatus = "";
	List<String> lstTableHierarchy = new ArrayList<String>();
	
	public List<RunSummaryMgmt> updateSummaryQuery (long runId, List<RunSummaryMgmt> lstRunSummaryMgmt) {
		String CURRENT_METHOD = "updateSummaryQuery";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method");
		List<RunSummaryMgmt> tagUpdatedRunSummaryMgmt = new ArrayList<RunSummaryMgmt>();
				
		Map<String, String> mapSummaryInputs = new HashMap<String, String>();
		for(RunSummaryMgmt runSummaryMgmt : lstRunSummaryMgmt) {
			String tableName = runSummaryMgmt.getImpactTableName();
			mapSummaryInputs = new HashMap<String, String>();
			mapSummaryInputs.put(GlobalConstants.KEY_CATEGORY_ID, String.valueOf(runSummaryMgmt.getCategoryId()));
			mapSummaryInputs.put(GlobalConstants.KEY_COUNTRY_CODE, runSummaryMgmt.getCountryCode());
			
			lstTableHierarchy = new ArrayList<String>();
			runSummaryMgmt.setTaggedQueryLoad(fetchCompleteTaggedQuery(tableName, mapSummaryInputs, runId));
			tagUpdatedRunSummaryMgmt.add(runSummaryMgmt);
		}
		return tagUpdatedRunSummaryMgmt;
	}
			
	public String fetchCompleteTaggedQuery(String tableName, Map<String, String> mapSummaryInputs, long runId) {
		String CURRENT_METHOD = "fetchCompleteTaggedQuery";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method");
		
		Map<String, ImpactTable> mapImpactTable = fetchImpactTableMap(runId);
		String taggedCompleteQuery = "";
		ImpactTable impactTable = mapImpactTable.get(tableName);
		parentHierarchy(tableName, tableName, mapImpactTable);
				
		for(String hierarchy : lstTableHierarchy) {
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: lstTableHierarchy.size :"+lstTableHierarchy.size());
			System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: hierarchy :"+hierarchy);
			String[] tableList = hierarchy.split(":");
			String query[] = new String[3];
			String[] parentAry = null;
			String[] parentCol = null;
			String parentTableCol = "";
			for(int tableListIterator=0 ; tableListIterator< tableList.length; tableListIterator++) {
				System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: tableList[i] :"+tableList[tableListIterator]);
				ImpactTable currentImpactTable = mapImpactTable.get(tableList[tableListIterator]);
								
				if(tableListIterator == 0) {				
					query[0] = "SELECT DISTINCT "+tableName+".ID ID, \'"+tableName+"\' IMPACT_TABLE_NAME ";
					query[1] = " FROM " +currentImpactTable.getImpactSchema()+"."+tableName +" "+tableName; 
							//parentSchema +"."+parentTableNm+" "+parentTableNm;
					query[2] = " WHERE "+tableName+"."+impactTable.getImpactTableColumn()+" = ";
							//parentTableNm+"."+parentTableCol;
				} else {
					String currentTableType = currentImpactTable.getImpactTableType();	
					String currentTableName = currentImpactTable.getImpactTableName();
					String currentSchema = currentImpactTable.getImpactSchema();
					
					for(int parentIterator=0; parentIterator<parentAry.length; parentIterator++) {
						if(currentTableName.equalsIgnoreCase(parentAry[parentIterator])) {
							parentTableCol = parentCol[parentIterator];
							break;
						}
					}
					
					if(GlobalConstants.TYPE_TABLE_CHILD.equalsIgnoreCase(currentTableType)) {
						query[1] = query[1] + ", "+ currentSchema+"."+currentTableName+" "+currentTableName;
						query[2] = query[2] + currentTableName +"."+ parentTableCol +
								" AND "+currentTableName+"."+currentImpactTable.getImpactTableColumn()+" = ";
					} else {
						query[1] = query[1] + ", "+ currentSchema+"."+currentTableName+" "+currentTableName;
						query[2] = query[2] + currentTableName +"."+ parentTableCol;
						String colNames = currentImpactTable.getImpactColumns();
						String columnNames[];
						if(colNames.contains(GlobalConstants.COMMA_ONLY_STRING))
							columnNames = colNames.split(GlobalConstants.COMMA_ONLY_STRING);
						else {
							columnNames = new String[1];
							columnNames[0] = colNames;
						}
						int colLength=columnNames.length;
						for(int columnNameIterator = 0; columnNameIterator < colLength; columnNameIterator++) {  
							mapSummaryInputs.get(columnNames[columnNameIterator]);
							query[0] = query[0] +", "+currentTableName +"."+columnNames[columnNameIterator]+" "+columnNames[columnNameIterator];
							query[2] = query[2] +" AND "+currentTableName +"."+columnNames[columnNameIterator]+" = "+
									((columnNames[columnNameIterator].equalsIgnoreCase(GlobalConstants.KEY_CATEGORY_ID)) ? 
											Integer.parseInt(mapSummaryInputs.get(columnNames[columnNameIterator])) : 
												"\'"+mapSummaryInputs.get(columnNames[columnNameIterator])+"\'");
						}						
					}
				}
				String currentParent = currentImpactTable.getParentTable();
				String currentParentTableCol = currentImpactTable.getParentTableColumn();				
				parentAry = currentParent.contains(":") ? currentParent.split(":") : new String[]{currentParent};
				parentCol = currentParentTableCol.contains(":") ? currentParentTableCol.split(":") : new String[]{currentParentTableCol};
			}
			String taggedQuery = query[0] + query[1] + query[2]+" AND RUN_ID = "+runId;
			if(taggedCompleteQuery.equalsIgnoreCase(""))
				taggedCompleteQuery = taggedQuery;
			else
				taggedCompleteQuery = taggedCompleteQuery + " UNION "+ taggedQuery;
		}		
		return taggedCompleteQuery;
	}
	
	public String fetchCompleteBackupDataQuery(String tableName,Map<String, String> mapSummaryInputs, String selectCls, long runId) {
		String CURRENT_METHOD = "fetchCompleteBackupDataQuery";
	//	System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method");
		lstTableHierarchy = new ArrayList<String>();
		Map<String, ImpactTable> mapImpactTable = fetchImpactTableMap(runId);
		String backupCompleteQuery = "";
		ImpactTable impactTable = mapImpactTable.get(tableName);
		parentHierarchy(tableName, tableName, mapImpactTable);
		selectCls = tableName + "." + selectCls.replaceAll(",", "," + tableName + ".");
		for(String hierarchy : lstTableHierarchy) {
			//System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: lstTableHierarchy.size :"+lstTableHierarchy.size());
			//System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: hierarchy :"+hierarchy);
			String[] tableList = hierarchy.split(":");
			String query[] = new String[3];
			String[] parentAry = null;
			String[] parentCol = null;
			String parentTableCol = "";
			for(int tableListIterator=0 ; tableListIterator< tableList.length; tableListIterator++) {
				//System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: tableList[i] :"+tableList[tableListIterator]);
				ImpactTable currentImpactTable = mapImpactTable.get(tableList[tableListIterator]);
								
				if(tableListIterator == 0) {				
					query[0] = "SELECT DISTINCT "+tableName+".ID ID, "+ selectCls;
					query[1] = " FROM " +currentImpactTable.getImpactSchema()+"."+tableName +" "+tableName+", GDPR.DATA_LOAD DL"; 
							//parentSchema +"."+parentTableNm+" "+parentTableNm;
					query[2] = " WHERE "+tableName+"."+impactTable.getImpactTableColumn()+" = ";
							//parentTableNm+"."+parentTableCol;
				} else {
					String currentTableType = currentImpactTable.getImpactTableType();	
					String currentTableName = currentImpactTable.getImpactTableName();
					String currentSchema = currentImpactTable.getImpactSchema();
					
					for(int parentIterator=0; parentIterator<parentAry.length; parentIterator++) {
						if(currentTableName.equalsIgnoreCase(parentAry[parentIterator])) {
							parentTableCol = parentCol[parentIterator];
							break;
						}
					}
					
					if(GlobalConstants.TYPE_TABLE_CHILD.equalsIgnoreCase(currentTableType)) {
						query[1] = query[1] + ", "+ currentSchema+"."+currentTableName+" "+currentTableName;
						query[2] = query[2] + currentTableName +"."+ parentTableCol +
								" AND "+currentTableName+"."+currentImpactTable.getImpactTableColumn()+" = ";
					} else {
						query[1] = query[1] + ", "+ currentSchema+"."+currentTableName+" "+currentTableName;
						query[2] = query[2] + currentTableName +"."+ parentTableCol;
						String colNames = currentImpactTable.getImpactColumns();
						String columnNames[];
						if(colNames.contains(GlobalConstants.COMMA_ONLY_STRING))
							columnNames = colNames.split(GlobalConstants.COMMA_ONLY_STRING);
						else {
							columnNames = new String[1];
							columnNames[0] = colNames;
						}
						int colLength=columnNames.length;
						for(int columnNameIterator = 0; columnNameIterator < colLength; columnNameIterator++) {  
							mapSummaryInputs.get(columnNames[columnNameIterator]);
							//query[0] = query[0] +", "+currentTableName +"."+columnNames[columnNameIterator]+" "+columnNames[columnNameIterator];
							query[2] = query[2] +" AND "+currentTableName +"."+columnNames[columnNameIterator]+" = "+
									((columnNames[columnNameIterator].equalsIgnoreCase(GlobalConstants.KEY_CATEGORY_ID)) ? 
											Integer.parseInt(mapSummaryInputs.get(columnNames[columnNameIterator])) : 
												"\'"+mapSummaryInputs.get(columnNames[columnNameIterator])+"\'");
						}						
					}
				}
				String currentParent = currentImpactTable.getParentTable();
				String currentParentTableCol = currentImpactTable.getParentTableColumn();				
				parentAry = currentParent.contains(":") ? currentParent.split(":") : new String[]{currentParent};
				parentCol = currentParentTableCol.contains(":") ? currentParentTableCol.split(":") : new String[]{currentParentTableCol};
			}
			String backupQuery = query[0] + query[1] + query[2]+" AND GDPR_DEPERSONALIZATION.COUNTRY_CODE = DL.COUNTRY_CODE AND "
					+ "GDPR_DEPERSONALIZATION.CREATED_DATE_TIME >DL.LAST_DATA_LOADED_DATE AND GDPR_DEPERSONALIZATION.HEROKU_STATUS='SCHEDULED' "
					+ "AND RUN_ID = "+runId;
			if(backupCompleteQuery.equalsIgnoreCase(""))
				backupCompleteQuery = backupQuery;
			else
				backupCompleteQuery = backupCompleteQuery + " UNION "+ backupQuery;
		}		
		return backupCompleteQuery;
	}
	
	public Map<String, ImpactTable> fetchImpactTableMap(long runId) {
		
		String CURRENT_METHOD = "fetchImpactTableMap";
		System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method");
		Map<String, ImpactTable> mapImpactTable = new HashMap<String, ImpactTable>();
		try {
			List<ImpactTable> lstImpactTable = gdprInputFetchDaoImpl.fetchImpactTable(); 
			for (ImpactTable impactTable : lstImpactTable) {
				mapImpactTable.put(impactTable.getImpactTableName(), impactTable);
			}
		} catch (Exception exception) {	
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: "+GlobalConstants.ERR_FETCH_TABLE_DETAIL);
			exception.printStackTrace();
		}
		return mapImpactTable;
	}
	
	public void parentHierarchy(String tableName, String parentHierarchy, Map<String, ImpactTable> mapImpactTable) {
		String CURRENT_METHOD = "parentHierarchy";
		//System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method");
				
		List<String> currentParentList = fetchParentList(tableName, parentHierarchy, mapImpactTable);
		for(String parent: currentParentList) {
			String[] aryParent = parent.split(":");
			String currentParent = aryParent[(aryParent.length - 1)];
			ImpactTable parentImpactTable = mapImpactTable.get(currentParent);
			if(GlobalConstants.TYPE_TABLE_PARENT.equalsIgnoreCase(parentImpactTable.getImpactTableType())) {
				System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: parent "+parent);
				lstTableHierarchy.add(parent);
			} else {
				parentHierarchy(currentParent, parent, mapImpactTable);				
			}
		}		
	}
	
	public List<String> fetchParentList(String tableName, String parentHierarchy, Map<String, ImpactTable> mapImpactTable) { 
		String CURRENT_METHOD = "fetchParentList";
		//System.out.println(CURRENT_CLASS + " ::: " + CURRENT_METHOD + " :: Inside method");
		List<String> parentList = new ArrayList<String>();
	
		try {				
			ImpactTable impactTable = mapImpactTable.get(tableName);
			String parentTable = impactTable.getParentTable();
			String[] aryParent= null;			
			
			if(parentTable.contains(":")) {
				aryParent = parentTable.split(":");
			}else { 
				aryParent = new String[1];
				aryParent[0] = impactTable.getParentTable();
			}
			for(int i=0 ; i< aryParent.length; i++) {
				parentList.add(parentHierarchy+":"+aryParent[i]);					
			}
		} catch (Exception exception) {	
			System.out.println(CURRENT_CLASS+" ::: "+CURRENT_METHOD+" :: Error in fetching parent");
			exception.printStackTrace();
		}
		return parentList;
	}
}