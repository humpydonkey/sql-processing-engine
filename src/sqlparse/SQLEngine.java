package sqlparse;

import java.io.File;
import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import logicplan.JoinManager;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.update.Update;
import ra.Aggregator;
import ra.EvaluatorArithmeticExpres;
import ra.EvaluatorConditionExpres;
import ra.ExternalMergeSort;
import ra.Operator;
import ra.OperatorCache;
import ra.OperatorGroupBy;
import ra.OperatorIndexScan;
import ra.OperatorOrderBy;
import ra.OperatorProjection;
import ra.OperatorScan;
import ra.OperatorSelection;
import common.Tools;
import dao.Datum;
import dao.Schema;
import dao.Tuple;
import dao.Tuple.Row;

public class SQLEngine {
	/*********************	static	filed	*********************/
	private static final Map<String, CreateTable> globalCreateTables = new HashMap<String, CreateTable>();
	private static final Map<String, Schema> globalSchemas = new HashMap<String, Schema>();
	private static Tuple globalTuple;
	private static IndexManager indxMngr;
	
	public static Map<String, CreateTable> getGlobalCreateTables(){	return globalCreateTables; }
	public static Map<String, Schema> getGlobalSchemas(){ return globalSchemas; }
	public static Tuple getGlobalTuple(){ return globalTuple; }
	public static void setGlobalTuple(Tuple tup){ globalTuple = tup; }
	
	
	/**
	 * Parse Create Table statement
	 * @param stmt
	 */
	public static void create(Statement stmt)
	{
		if(stmt instanceof CreateTable){
			CreateTable ct = (CreateTable)stmt;
			String tabName = ct.getTable().getName();
			globalCreateTables.put(tabName,ct);
			
			Schema sch = Schema.schemaFactory(null, ct, ct.getTable());
			globalSchemas.put(tabName, sch);
		}
	}

	/**
	 * Build the index of given table
	 * @param indexDir: index directory
	 * @param dataDir: data file directory
	 */
	public static void buildIndex(String indexDir, File dataDir, Schema schema){
		IndexManager indxMngr = new IndexManager(indexDir);
		File dataFile = new File(dataDir.getPath()+File.separator+schema.getTableName()+".dat");
		Operator data = new OperatorScan(dataFile, schema);
		try {
			indxMngr.buildPrmyStore(schema.getTableName(), data, schema.getPrmyKey(), schema.getAllScdrKeys());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		indxMngr.close();
	}
	
	/*********************	non-static	filed	*********************/	
	private Map<String, Table> localFromItemTable;
	private File dataPath;
	
	public SQLEngine(File dataPathIn, String indexDir){
		localFromItemTable = new HashMap<String, Table>();

		if(dataPathIn!=null)
			dataPath = dataPathIn;
		if(indexDir!=null && indxMngr==null)
			indxMngr = new IndexManager(indexDir);
	}
	
	public IndexManager getIndexManager(){ return indxMngr; }
	public Map<String, Table> getLocalTables(){ return localFromItemTable; }
	
	/**
	 * Insert statement
	 * @param insert
	 * @return
	 */
	public boolean insert(Insert insert){
		boolean res = false; 
		Table tab = insert.getTable();
		String tabName = tab.getName();
		Schema schema = globalSchemas.get(tabName);
	
		ItemsList items = insert.getItemsList();
		InsertValueEvaluator eval = new InsertValueEvaluator();
		
		//One tuple
		if(items instanceof ExpressionList){
			ExpressionList exprs = (ExpressionList)items;
			@SuppressWarnings("unchecked")
			List<Expression> expList =  exprs.getExpressions();
			Datum[] row = new Datum[expList.size()];
			int i=0;
			for(Object expr : expList){
				Expression valueExpr = (Expression)expr;
				valueExpr.accept(eval);
				Datum cell = eval.getCell();
				row[i++] = cell;
			}
			try {
				indxMngr.insertInStoreMap(new Row(row), schema);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return true;
		}else{
			//subselect...
		}
			
		return res;
	}
	
	
	@SuppressWarnings("unchecked")
	public boolean update(Update update){
		final String tabName = update.getTable().getName();
		//convert Expressions to values
		List<Expression> vals = update.getExpressions();
		List<Datum> setValues = new ArrayList<Datum>(vals.size());
		EvaluatorArithmeticExpres eval = new EvaluatorArithmeticExpres();
		for(Expression exp : vals)
			setValues.add(eval.parse(exp));
		
		
		Schema schema = globalSchemas.get(tabName);
		Expression where = update.getWhere();
		//retrieve tuples
		@SuppressWarnings("serial")
		SelectionParser selParser = new SelectionParser(new ArrayList<String>(){{add(tabName);}});
		selParser.parse(where);
		List<Expression> exps = selParser.getSeparateExprs(tabName);
		
		FromItemConvertor tableConvertor = new FromItemConvertor(dataPath,selParser, globalCreateTables, null, this);
		OperatorIndexScan data = tableConvertor.createIndexScan(exps, schema);
		data.init();
		EvaluatorConditionExpres selecltionEval = new EvaluatorConditionExpres(null, this);
		//set values
		List<Column> cols = update.getColumns();
		int n = cols.size();
		for(Tuple tup : data.getData()){
			selecltionEval.updateTuple(tup);
			where.accept(selecltionEval);
			if(selecltionEval.getResult()){
				for(int i=0; i<n; i++){
					Column col = cols.get(i);
					Datum value = setValues.get(i);
					tup.setDataByName(value, col);
				}	
			}
		}
		
		//put back
		indxMngr.commit();
		return true;	
	}
	
	public boolean delete(Delete delete){
		final String tabName = delete.getTable().getName();
		Schema schema = globalSchemas.get(tabName);
		
		Expression where = delete.getWhere();
		
		@SuppressWarnings("serial")
		SelectionParser selParser = new SelectionParser(new ArrayList<String>(){{add(tabName);}});
		selParser.parse(where);
		List<Expression> exps = selParser.getSeparateExprs(tabName);
		
		FromItemConvertor tableConvertor = new FromItemConvertor(dataPath,selParser, globalCreateTables, null, this);
		OperatorIndexScan data = tableConvertor.createIndexScan(exps, schema);
		
		try {
			//TODO
			List<Long> keys = data.findDataKeys();
			String name = indxMngr.getName();
			indxMngr.reopen(name);
			indxMngr.deleteFromStoreMap(keys, schema);
		} catch (UnexpectedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}
	
	
	/**
	 * Select statement
	 * @param select
	 * @return
	 */
	public List<Tuple> select(SelectBody select){

		Map<String, Operator> operMap = new HashMap<String, Operator>();
		Operator oper = null;
		
		if(select instanceof PlainSelect){
			PlainSelect pselect = (PlainSelect) select;
			
			/******** Extract local table out, fill into localFromItemTable, ignore subselect, subjoin ************/
			TableParser tabParser = new TableParser(pselect);
			localFromItemTable = tabParser.getFromItemTable();
			
			/******** Parse select item, find out which column will be use ************/
			ColumnUsedParser cf = new ColumnUsedParser(pselect);
			
			/******** Parse where condition, decompose and distribute it to different table, and generate Join List ************/
			Expression where = pselect.getWhere();
			//decompose where condition, distribute to each one
			SelectionParser selParser = new SelectionParser(localFromItemTable.keySet());
			selParser.parse(where);
			//reset the original where to a filtered where
			pselect.setWhere(selParser.getMergedExprs(SelectionParser.MultiTabName));
			
			/*********************    Scan&Selection    ********************/
			//parse scan
			FromItemConvertor fromItemScan = new FromItemConvertor(dataPath,selParser, globalCreateTables, cf.getColumnMapper(), this);
			operMap = parseScan(pselect, fromItemScan);
			
			/*********************   Push down Selection   ****************/
			//resolve and redistribute the big where condition
			oper = pushDownSelection(selParser, operMap);
			
			/*********************    Equal Join    ********************/
			if(pselect.getJoins()!=null){
				JoinManager jm = new JoinManager(selParser.getJoinList(), operMap, Config.getSwapDir());
				oper = jm.pipeline();
				//oper = jm.executeJoin();
			}
			
			/*********************    Add filtered where condition  ****************/
			if(pselect.getWhere()!=null)
				oper = new OperatorSelection(oper, pselect.getWhere(), this);
			
			/*********************    Parsing selected items    ********************/
			SelectItemParser selectItemScan = new SelectItemParser(pselect);
			Schema newSchema = selectItemScan.getSelectedColumns();
			Aggregator[] aggrs = selectItemScan.getAggregators();
			
			/*********************    Group By + Order By   ********************/
			@SuppressWarnings("rawtypes")
			List groupbyCols = pselect.getGroupByColumnReferences();
			if(aggrs.length>0 || groupbyCols!=null){
				//if aggregate function exist or group by column exist
				OperatorGroupBy groupby = new OperatorGroupBy(oper, Config.getSwapDir(), groupbyCols, aggrs);
				OperatorOrderBy ob = null;
				if(pselect.getOrderByElements()!=null){
					@SuppressWarnings("unchecked")
					List<OrderByElement> eles = pselect.getOrderByElements();
					ob = new OperatorOrderBy(eles);
				}
				
				if(groupby.isSwap()){
					List<File> groupFiles = null;
					if(ob==null)
						groupFiles = groupby.dumpToDisk(null);
					else
						groupFiles = groupby.dumpToDisk(ob.getTupleComparator());
					
					ExternalMergeSort emsort = new ExternalMergeSort(
							groupFiles,
							groupby.getSchema(),
							ob.getCompAttrs());
					
					File mergedF = emsort.sort();
					oper = new OperatorScan(mergedF, oper.getSchema());			
				}else{
					List<Tuple> tuples = groupby.dump();
					oper = new OperatorCache(tuples);
					/*********************    Projection    ********************/
					if(!selectItemScan.getIfSelectAll()){
						oper = new OperatorProjection(oper, newSchema);
					}
					tuples = dump(oper);
					if(ob!=null)
						Collections.sort(tuples, ob.getTupleComparator());
					oper = new OperatorCache(tuples);
				}
			}else{
				/*********************  Only Projection No Group By    ********************/
				if(!selectItemScan.getIfSelectAll()){
					oper = new OperatorProjection(oper, newSchema);
				}
			}

			
			if(pselect.getLimit()==null)
				return SQLEngine.dump(oper);
			else{
				int n = (int) pselect.getLimit().getRowCount();
				List<Tuple> limitedRes = new ArrayList<Tuple>(n);

				Tuple tup;
				int limitCount = 0;
				while((tup=oper.readOneTuple())!=null){
					limitedRes.add(tup);
					limitCount++;
					if(limitCount==n)
						break;
				}

				return limitedRes;
			}	
		}
		
		return null;		
	}
	
	
	public Map<String, Operator> parseScanSelection(final PlainSelect pselect, SelectionParser selParser, FromItemConvertor scanGenerator){		
		/*********************  Create Scan    ********************/
		Map<String, Operator> operMap = new HashMap<String, Operator>(); 
		
		FromItem fromitem = pselect.getFromItem();
		fromitem.accept(scanGenerator);
		//the first table, fromItem, always have
		Operator oper = scanGenerator.getSourceOperator();	
		operMap.put(scanGenerator.getTableName(), oper);
			
		return null;
	}
	
	public Map<String, Operator> parseScan(final PlainSelect pselect, FromItemConvertor scanGenerator){
		/*********************  Create Scan    ********************/
		Map<String, Operator> operMap = new HashMap<String, Operator>(); 
		
		FromItem fromitem = pselect.getFromItem();
		fromitem.accept(scanGenerator);
		//the first table, fromItem, always have
		Operator oper = scanGenerator.getSourceOperator();	
		operMap.put(scanGenerator.getTableName(), oper);
		
		//the rest joinItems
		@SuppressWarnings("unchecked")
		List<Join> joinList = pselect.getJoins();
		if(joinList!=null){
			for(Join t : joinList){
				Table table = (Table)t.getRightItem();
				table.accept(scanGenerator);	//generate scan operator
				Operator scan = scanGenerator.getSourceOperator();
				operMap.put(scanGenerator.getTableName(), scan);
			}
		}
		
		return operMap;
	}
	

	/*********************  Push down Selection    ********************/
	private Operator pushDownSelection(SelectionParser selParser, Map<String, Operator> operMap){
		if(operMap.size()==0)
			throw new IllegalArgumentException("OperMap.size is 0!");
		Operator oper = null;
		//for every scan operator, pipeline select
		for(Entry<String, Operator> scan : operMap.entrySet()){
			//subwhere
			String tName = scan.getKey();
			Expression subwhere = selParser.getMergedExprs(tName);
			if(subwhere!=null){
				Operator scan_select = scan.getValue();
				scan_select = new OperatorSelection(scan_select, subwhere, this);
				operMap.put(tName, scan_select);
				Tools.debug("Push down selection : "+ tName +" "+subwhere.toString());
				oper = scan_select;
			}else
				oper = scan.getValue();
		}
		return oper;
	}

	
	public static List<Tuple> dump(Operator oper){
		List<Tuple> results = new LinkedList<Tuple>();
		Tuple tup;
		while((tup=oper.readOneTuple())!=null){
			results.add(tup);
		}
		
		return results;
	}
}
