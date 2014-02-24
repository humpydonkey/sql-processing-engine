package sql2ra;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import ra.Aggregator;
import ra.Operator;
import ra.OperatorCache;
import ra.OperatorGroupBy;
import ra.OperatorProjection;
import ra.OperatorSelection;
import dao.Schema;
import dao.Tuple;

public class SQLParser {
	private static FromScanner fromscan;
	
	public static FromScanner getFromScanner(){
		return fromscan;
	}
	
	public static void setFromScanner(FromScanner fs){
		fromscan = fs;
	}
	
	public static void create(Statement stmt,HashMap<String,CreateTable> tables)
	{
		if(stmt instanceof CreateTable){
			CreateTable ct = (CreateTable)stmt;
			//System.out.println("TABLE: " + ct.getTable().getName());
			
			tables.put(
				ct.getTable().getName(),
				ct
			);
		}
	}
	
	public static List<Tuple> select(SelectBody select, FromScanner fromscan){
		//System.out.println("I would now evaluate:" + select);
		Operator oper = null;
		
		if(select instanceof PlainSelect){
			PlainSelect pselect = (PlainSelect) select;
			pselect.getFromItem().accept(fromscan);
			
			oper = fromscan.source;
			
			/*********************    Selection    ********************/	
			if(pselect.getWhere()!=null){
				oper = new OperatorSelection(oper,pselect.getWhere());
			}
			
			/*********************    Parsing selected items    ********************/
			SelectItemScanner selectItemScan = new SelectItemScanner(pselect, oper);
			Schema newSchema = selectItemScan.getSelectedColumns();
			Aggregator[] aggrs = selectItemScan.getAggregators();
			
			/*********************    Group By    ********************/
			if(pselect.getGroupByColumnReferences() != null){  //have group by
				@SuppressWarnings("unchecked")
				List colRefs = pselect.getGroupByColumnReferences();
				OperatorGroupBy groupby = new OperatorGroupBy(oper, colRefs, aggrs);
				List<Tuple> tuples = groupby.getTuples();
				oper = new OperatorCache(tuples);
			 }
			 
			
			/*********************    Projection    ********************/
			if(!selectItemScan.getIfSelectAll()){
				oper = new OperatorProjection(oper, newSchema);
			}
		}
		
		return dump(oper);
	}
	
	public static List<Tuple> dump(Operator oper){
		List<Tuple> results = new LinkedList<Tuple>();
		Tuple tuple = oper.readOneTuple();
		while(tuple!=null){
			results.add(tuple);
			//tuple.printTuple();
			tuple = oper.readOneTuple();
		}
		return results;
	}
}
