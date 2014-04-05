package sql2ra;

import java.io.File;
import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
import ra.EqualJoin;
import ra.EvaluatorEqualJoin;
import ra.Operator;
import ra.OperatorCache;
import ra.OperatorGroupBy;
import ra.OperatorHashJoin_File;
import ra.OperatorOrderBy;
import ra.OperatorProjection;
import ra.OperatorSelection;
import dao.Schema;
import dao.Tuple;

public class SQLEngine {
	private boolean ifswap;
	private static File swapDir;
	private static File dataPath;
	private static Map<String, CreateTable> globalCreateTables;
	
	public SQLEngine(File dataPathIn, File swapDirIn){
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
			/*********************    Scan&Selection    ********************/
			//parse scan and push down selection
			FromItemEvaluator fromItemScan = new FromItemEvaluator(dataPath, swapDir, globalCreateTables);
			operMap = parseScan(pselect, fromItemScan);
			
			//get the latest operator
			oper = operMap.get(fromItemScan.getTableName());
			
			/*********************    EqualJoin    ********************/
			if(pselect.getJoins()!=null){	
				oper = executeJoin(pselect, operMap, swapDir);
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
	
	public Map<String, Operator> parseScan(final PlainSelect pselect, FromItemEvaluator fromItemScan){
		/*********************  Create Scan    ********************/
		
		FromItem fromitem = pselect.getFromItem();
		fromitem.accept(fromItemScan);
		Operator oper = fromItemScan.getSource();	//scan operator
		Map<String, Operator> operMap = new HashMap<String, Operator>(); 
		operMap.put(fromItemScan.getTableName(), oper);
		
		@SuppressWarnings("unchecked")
		List<Join> joinList = pselect.getJoins();
		if(joinList!=null){
			for(Join t : joinList){
				Table table = new Table(null, t.toString());
				table.accept(fromItemScan);	//generate scan operator
				Operator scan = fromItemScan.getSource();
				operMap.put(fromItemScan.getTableName(), scan);
			}
		}
		
		/*********************  Push down Selection    ********************/
		Expression where = pselect.getWhere();
		if(where!=null){
			//for every scan operator, pipeline select
			for(Entry<String, Operator> scan : operMap.entrySet()){
				Operator scan_select = scan.getValue();
				scan_select = new OperatorSelection(scan_select,where);
				operMap.put(scan.getKey(), scan_select);
				oper = scan_select;
			}
		}
		
		return operMap;
	}
	
	public List<EqualJoin> parseJoin(final PlainSelect pselect){
		//evaluate where condition to find join pairs
		Expression where = pselect.getWhere();
		EvaluatorEqualJoin eval = new EvaluatorEqualJoin();
		where.accept(eval);
		List<EqualJoin> joinPairs = eval.getJoins();
		return joinPairs;
	}
	
	public Operator executeJoin(final PlainSelect pselect, Map<String, Operator> operMap, File swapDir){
		/*********************    EqualJoin    ********************/
		List<EqualJoin> joinPairs = parseJoin(pselect);	
		Set<String> joinedHistory = new HashSet<String>();

		Operator combineJoin = null;
		try {
			String key="";
			for(EqualJoin ej : joinPairs){
				//construct join operator
				String key1 = ej.getLeftTableName();
				String key2 = ej.getRightTableName();		
				if(combineJoin==null){
					combineJoin = new OperatorHashJoin_File(
							ej.getColName(), 
							operMap.get(key1), 
							operMap.get(key2),
							new File(swapDir.getPath()+key1));
				}else{
					Operator needToJoin = null;
					//pipeline the current equalJoin into 
					//the combineJoin
					if(joinedHistory.contains(key1)){
						//combine key2
						key = key1;
						needToJoin = operMap.get(key2);							
					}else if(joinedHistory.contains(key2)){
						//combine key1
						key = key2;
						needToJoin = operMap.get(key1);
					}else{
						try {
							throw new UnexpectedException("can not pipeline the equalJoin, key1:" + key1 + " key2:"+key2);
						} catch (UnexpectedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					
					combineJoin = new OperatorHashJoin_File(
							ej.getColName(),
							needToJoin, 
							combineJoin,
							new File(swapDir.getPath()+key));
				}
				
				joinedHistory.add(key1);
				joinedHistory.add(key2);
			}//end for
		}catch(Exception e){
			e.printStackTrace();
		}
			
		return combineJoin;
	}
	
	public void analysisQuery(final PlainSelect pselect){
		//get scan tables
		Expression where = pselect.getWhere();
	}
	
	public static List<Tuple> dump(Operator oper){
		List<Tuple> results = new LinkedList<Tuple>();
//		Tuple tuple = oper.readOneTuple();
//		while(tuple!=null){
//			results.add(tuple);
//			tuple = oper.readOneTuple();
//		}
		
		List<Tuple> tuples = oper.readOneBlock();
		while(tuples.size()!=0){
			results.addAll(tuples);
			tuples = oper.readOneBlock();
		}
		oper.reset();
		return results;
	}
}
