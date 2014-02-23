package sql2ra;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import ra.Aggregator;
import ra.CacheOperator;
import ra.GroupByOperator;
import ra.Operator;
import ra.ProjectionOperator;
import ra.SelectionOperator;
import dao.Schema;
import dao.Tuple;

public class SQLParser {
	
	public static void create(Statement stmt,HashMap<String,CreateTable> tables)
	{
		if(stmt instanceof CreateTable){
			CreateTable ct = (CreateTable)stmt;
			System.out.println("TABLE: " + ct.getTable().getName());
			
			tables.put(
				ct.getTable().getName(),
				ct
			);
		}
	}
	
	public static List<Tuple> select(Statement stmt,HashMap<String,CreateTable> tables,File dataDir) //whether there should be a File var
	{
		SelectBody select = ((Select)stmt).getSelectBody();
		System.out.println("I would now evaluate:" + select);
		Operator oper = null;
		
		if(select instanceof PlainSelect){
			
			PlainSelect pselect = (PlainSelect) select;
			FromScanner fromscan = new FromScanner(dataDir, tables);
			pselect.getFromItem().accept(fromscan);
			
			oper = fromscan.source;
			
			/*********************    Selection    ********************/	
			if(pselect.getWhere()!=null){
				oper = new SelectionOperator(oper,pselect.getWhere());
			}
			
			/*********************    Parsing selected items    ********************/
			SelectItemScanner selectItemScan = new SelectItemScanner(pselect, oper);
			Schema newSchema = selectItemScan.getSelectedColumns();
			Aggregator[] aggrs = selectItemScan.getAggregators();
			/*********************    Group By    ********************/
			if(pselect.getGroupByColumnReferences() != null){  //have group by
				@SuppressWarnings("unchecked")
				List colRefs = pselect.getGroupByColumnReferences();
				GroupByOperator groupby = new GroupByOperator(oper, colRefs, aggrs);
				List<Tuple> tuples = groupby.getTuples();
				oper = new CacheOperator(tuples);
			 }
			 
			
			/*********************    Projection    ********************/
			if(!selectItemScan.getIfSelectAll()){
				oper = new ProjectionOperator(oper, newSchema);
			}
			
			
//			if(pselect.getHaving()!=null){  //have having, maybe no where, no groupby
//				oper = new HavingOperator(  //GroupByOperator dai ding yi
//						oper,
//						fromscan.columns,
//						pselect.getWhere(),
//						pselect.getGroupByColumnReferences(),
//						pselect.getHaving()
//					);
//			}
//			if(pselect.getOrderByElements()!=null){  //have orderby, maybe no where
//						oper = new OrderByOperator(
//						 oper,
//						 fromscan.columns,
//						 pselect.getWhere(),
//						 pselect.getOrderByElements()
//					);
//					
//			}
		}
		
		return dump(oper);
	}
	
	public static List<Tuple> dump(Operator oper){
		List<Tuple> results = new LinkedList<Tuple>();
		Tuple tuple = oper.readOneTuple();
		while(tuple!=null){
			results.add(tuple);
			tuple.printTuple();
			tuple = oper.readOneTuple();
		}
		return results;
	}
	
	public static void main(String args[]){
	
		System.out.println("begin");
		int i;
		String dataDirStr = "data/NBA/";
		String sqlFilePath = "data/cp1_graded_sqls/nba02.sql";
		
		File dataDir = null;
		//set arguments
        args = new String[]{
        		"--data",
        		dataDirStr,
        		sqlFilePath
        };
        
        ArrayList<File> sqlFiles = new ArrayList<File>();
        HashMap<String,CreateTable> tables = new HashMap<String, CreateTable>();
        
        for(i = 0; i<args.length;i++){
            if(args[i].equals("--data")){
                dataDir = new File(args[i+1]);
                i++;
            }else {
                sqlFiles.add(new File(args[i]));
            }
        }
        for (File sql : sqlFiles){
        	try{
        		FileReader stream = new FileReader(sql);
        		
        		CCJSqlParser parser = new CCJSqlParser(stream);
        		Statement stmt;

        		while((stmt = parser.Statement()) !=null){
        			
        			if(stmt instanceof CreateTable)	
        				create(stmt,tables);
        			else {
        				
        			 if(stmt instanceof Select){
        				List<Tuple> tuples = select(stmt,tables,dataDir);
        				
        				for(Tuple tuple : tuples)
        					tuple.printTuple();
        			 }
        			 else 
        				System.out.println("PANIC! I don't know how to handle" + stmt);
        			}
        		}
        		
        	}catch (IOException e){
        		e.printStackTrace();
        	}catch (ParseException e){
        		e.printStackTrace();
        	}
		
	}//end for
  }//end main
}
