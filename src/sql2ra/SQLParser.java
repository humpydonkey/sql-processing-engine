package sql2ra;

import java.io.File;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import ra.Aggregator;
import ra.Operator;
import ra.OperatorCache;
import ra.OperatorEqualJoin;
import ra.OperatorGroupBy;
import ra.OperatorOrderBy;
import ra.OperatorProjection;
import ra.OperatorSelection;
import dao.Schema;
import dao.Tuple;

public class SQLParser {
	private File basePath;
	private static Map<String, CreateTable> globalCreateTables;
	
	public SQLParser(File basePathIn){
		basePath = basePathIn;
		if(globalCreateTables==null)
			globalCreateTables = new HashMap<String, CreateTable>();
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
	
			FromItemEvaluator fromscan = new FromItemEvaluator(basePath, globalCreateTables);
			FromItem fromitem = pselect.getFromItem();
			fromitem.accept(fromscan);
			oper = fromscan.getSource();
			
			@SuppressWarnings("unchecked")
			List<Join> joinList = pselect.getJoins();
			Expression where = pselect.getWhere();
			if(where!=null&&joinList!=null){	
				/*********************    EqualJoin    ********************/
				List<Table> joinTables = new LinkedList<Table>();
				for(Join join : joinList){
					joinTables.add(new Table(null, join.toString()));
				}
				
				
				oper = new OperatorSelection(oper,where);
				operMap.put(fromscan.getTableName(), oper);
				
				//create Scan Operators for join input
				for(Table t : joinTables){
					FromItemEvaluator joinSource = new FromItemEvaluator(basePath, globalCreateTables);
					t.accept(joinSource);
					Operator scan = joinSource.getSource();
					scan = new OperatorSelection(scan,where);	//wrap a selection
					operMap.put(joinSource.getTableName(), scan);
				}
			
				
				//evaluate where condition to find join pairs
				EqualJoinScanner eval = new EqualJoinScanner();
				where.accept(eval);
				List<Column> joins = eval.getJoins();
				if(joins.size()<2){
					try {
						throw new UnexpectedException("the number of joins is wrong : " + joins.size());
					} catch (UnexpectedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				//construct join operator
				Column equalColumn = joins.get(0);
				String key1 = equalColumn.getTable().getName();
				String key2 = joins.get(1).getTable().getName();
				Set<String> joinedHistory = new HashSet<String>();
				joinedHistory.add(key1);
				joinedHistory.add(key2);
				
				Operator oper1 = operMap.get(key1);
				Operator oper2 = operMap.get(key2);
				if(oper1==null||oper2==null){
					try {
						throw new UnexpectedException("can not map the operator from operMap, key1:" + key1 + " key2:"+key2);
					} catch (UnexpectedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				Operator combineJoin = new OperatorEqualJoin(equalColumn, oper1, oper2);
				
				for(int i=2; i<joins.size(); i++){
					equalColumn = joins.get(i);
					key2 = equalColumn.getTable().getName();
					if(joinedHistory.contains(key2))
						continue;
					
					oper2 = operMap.get(key2);
					combineJoin = new OperatorEqualJoin(equalColumn,oper2, combineJoin);
				}
				
				oper = new OperatorCache(dump(combineJoin));
			}else{
				//no equal join, only one OperatorScan
				/*********************    Selection    ********************/	
				if(pselect.getWhere()!=null){
					oper = new OperatorSelection(oper,where);
				}
			}
					
			
			/*********************    Parsing selected items    ********************/
			SelectItemScanner selectItemScan = new SelectItemScanner(pselect, oper);
			Schema newSchema = selectItemScan.getSelectedColumns();
			Aggregator[] aggrs = selectItemScan.getAggregators();

			
			
			/*********************    Group By + Aggregate   ********************/
			if(pselect.getGroupByColumnReferences() != null){  //have group by
				@SuppressWarnings("rawtypes")
				List colRefs = pselect.getGroupByColumnReferences();
				OperatorGroupBy groupby = new OperatorGroupBy(oper, colRefs, aggrs);
				List<Tuple> tuples = groupby.getTuples();
				oper = new OperatorCache(tuples);
			 }else{
				//Only aggregate
				 if(aggrs.length>0){
					 List<Tuple> tuples = dump(oper);
						for(Tuple t : tuples){
							for(Aggregator aggr : aggrs){
								aggr.aggregate(t, "");	//"" means no group by
							}
						}
						if(tuples.size()>0){
							Tuple t = tuples.get(tuples.size()-1);	//get the last tuple
							tuples = new ArrayList<Tuple>();
							tuples.add(t);
						}						
						oper = new OperatorCache(tuples);
				 }		
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
		
		return SQLParser.dump(oper);
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
