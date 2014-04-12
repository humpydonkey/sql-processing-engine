package sql2ra;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import ra.Aggregator;
import ra.ConditionReorganizer;
import ra.Operator;
import ra.OperatorCache;
import ra.OperatorGroupBy;
import ra.OperatorOrderBy;
import ra.OperatorProjection;
import ra.OperatorSelection;
import dao.Schema;
import dao.Tuple;

public class SQLEngine {
	private boolean ifswap;
	private Map<String, Table> localFromItemTable;
	
	private static File swapDir;
	private static File dataPath;
	
	public static Map<String, CreateTable> globalCreateTables;
	public static Tuple globalTuple;
	
	public SQLEngine(File dataPathIn, File swapDirIn){
		localFromItemTable = new HashMap<String, Table>();
		
		if(swapDirIn==null)
			ifswap = false;
		else{
			ifswap = true;
			swapDir = swapDirIn;
		}
		
		if(dataPathIn!=null)
			dataPath = dataPathIn;
		
		if(globalCreateTables==null)
			globalCreateTables = new HashMap<String, CreateTable>();
		
	}
	
	public void setSwapDir(File dirIn){
		swapDir = dirIn;
	}
	
	public File getSwapDir(){
		return swapDir;
	}
	
	public Map<String, Table> getLocalTables(){
		return localFromItemTable;
	}
	
	public void create(Statement stmt)
	{
		if(stmt instanceof CreateTable){
			CreateTable ct = (CreateTable)stmt;
			//System.out.println("TABLE: " + ct.getTable().getName());
			
			globalCreateTables.put(
				ct.getTable().getName(),
				ct
			);
		}
	}
	
	public List<Tuple> select(SelectBody select){

		Map<String, Operator> operMap = new HashMap<String, Operator>();
		Operator oper = null;
		
		if(select instanceof PlainSelect){
			PlainSelect pselect = (PlainSelect) select;
			
			//extract local table into a map
			extractLocalTable(pselect,localFromItemTable);
			
			/*********************    Scan&Selection    ********************/		
			//parse scan
			SourceOperatorScanner fromItemScan = new SourceOperatorScanner(dataPath, swapDir, globalCreateTables);
			operMap = parseScan(pselect, fromItemScan);
			//push down selection
			if(operMap.size()==1){
				if(pselect.getWhere()!=null){
					//get the latest operator
					oper = operMap.get(fromItemScan.getTableName());
					oper = new OperatorSelection(oper, pselect.getWhere());
					operMap.put(fromItemScan.getTableName(), oper);	
				}else
					oper = operMap.get(fromItemScan.getTableName());
			}else{
				ConditionReorganizer organizer = pushDownSelection(pselect, operMap);
				
				/*********************    EqualJoin    ********************/
				if(pselect.getJoins()!=null){
					JoinManager jm = new JoinManager(organizer.getJoinList(), operMap, swapDir);
					oper = jm.executeJoin();
				}
			}

			/*********************    Parsing selected items    ********************/
			SelectItemScanner selectItemScan = new SelectItemScanner(pselect);
			Schema newSchema = selectItemScan.getSelectedColumns();
			Aggregator[] aggrs = selectItemScan.getAggregators();


			/*********************    Group By + Aggregate   ********************/
			@SuppressWarnings("rawtypes")
			List groupbyCols = pselect.getGroupByColumnReferences();
			if(aggrs.length>0 || groupbyCols!=null){
				//if aggregate function exist or group by column exist
				OperatorGroupBy groupby = new OperatorGroupBy(oper, groupbyCols, aggrs);
				List<Tuple> tuples = groupby.dump();
			//	System.out.println("Scanned "+OperatorScan.count+"\nGroup by "+groupby.count+" tuples");
				oper = new OperatorCache(tuples);	
			}

			
			/*********************    Projection    ********************/
			if(!selectItemScan.getIfSelectAll()){
				oper = new OperatorProjection(oper, newSchema);
			}

			
			/*********************    Order By    ********************/
			if(pselect.getOrderByElements()!=null){
				@SuppressWarnings("unchecked")
				List<OrderByElement> elets = pselect.getOrderByElements();

				try {
					OperatorOrderBy orderby = new OperatorOrderBy(dump(oper),elets);
					return orderby.getResults();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		return SQLEngine.dump(oper);
	}
	
	/**
	 * Extract local table out, fill into localFromItemTable
	 * ignore subselect, subjoin
	 * @param pselect
	 */
	public void extractLocalTable(final PlainSelect pselect, Map<String, Table> tableMap){
		//Parse From, getLocalItemTable
		FromItemParser fiParser = new FromItemParser(tableMap);
		pselect.getFromItem().accept(fiParser);
		@SuppressWarnings("unchecked")
		List<Join> joinList = pselect.getJoins();
		if(joinList!=null){
			for(Join t : joinList){
				Table table = (Table)t.getRightItem();
				table.accept(fiParser);	//generate scan operator
			}
		}
	}
	
	public Map<String, Operator> parseScan(final PlainSelect pselect, SourceOperatorScanner scanGenerator){
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
	
	
	public ConditionReorganizer pushDownSelection(final PlainSelect pselect, Map<String, Operator> operMap){
		/*********************  Push down Selection    ********************/
		Expression where = pselect.getWhere();
		
		//reorganize where condition, distribute to each one
		ConditionReorganizer reorganizer = new ConditionReorganizer(operMap.keySet());
		where.accept(reorganizer);
		
		if(where!=null){//the original where
			//for every scan operator, pipeline select
			for(Entry<String, Operator> scan : operMap.entrySet()){
				//subwhere
				String tName = scan.getKey();
				Expression subwhere = reorganizer.getExprByTName(tName);
				if(subwhere!=null){
					Operator scan_select = scan.getValue();
					scan_select = new OperatorSelection(scan_select, subwhere);
					operMap.put(tName, scan_select);
				}			
			}
			
			//reset the original where to a filtered where
			pselect.setWhere(reorganizer.getExprByTName(ConditionReorganizer.MultiTable));
		}
		return reorganizer;
	}
	
	
//	public List<EqualJoin> parseJoin(final PlainSelect pselect){
//		//evaluate where condition to find join pairs
//		Expression where = pselect.getWhere();
//		EvaluatorEqualJoin eval = new EvaluatorEqualJoin();
//		where.accept(eval);
//		List<EqualJoin> joinPairs = eval.getJoins();
//		return joinPairs;
//	}
	
	
	
	public void analysisQuery(final PlainSelect pselect){
		//get scan tables
		//Expression where = pselect.getWhere();
	}
	
	public static List<Tuple> dump(Operator oper){
		List<Tuple> results = new LinkedList<Tuple>();
		Tuple tup;
		while((tup=oper.readOneTuple())!=null){
			results.add(tup);
		}
		
		oper.reset();
		return results;
	}
}
