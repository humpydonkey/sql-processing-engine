package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import sql2ra.FromScanner;
import sql2ra.SQLParser;
import dao.Tuple;

public class Main {

	public static void main(String args[]){
		
//		String testDir = "D:/Asia/tpch/";
//		List<File> files = FileAccessor.getInstance().getDataFiles(testDir, "dat");
//		System.out.println(FileAccessor.getInstance().readAll(testDir).toString());
//		
		//System.out.println("begin");
		int i;
		String dataDirStr = "data/tpch/";//"data/NBA/";  //"/data/tpch/";
		String sqlFilePath = "data/cp1_graded_sqls/tpch6.sql";
		
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
        
        SQLParser.setFromScanner(new FromScanner(dataDir, tables));
        FromScanner fromscan = SQLParser.getFromScanner();
        
        for (File sql : sqlFiles){
        	try{
        		FileReader stream = new FileReader(sql);
        		
        		CCJSqlParser parser = new CCJSqlParser(stream);
        		Statement stmt;

        		while((stmt = parser.Statement()) !=null){
        			
        			if(stmt instanceof CreateTable)	
        				SQLParser.create(stmt,tables);
        			else {
        				
        			 if(stmt instanceof Select){
        				//System.out.println("I would now evaluate:" + stmt);
        				Select sel = (Select)stmt;
        				List<Tuple> tuples = SQLParser.select(sel.getSelectBody(),fromscan);
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
        
  //  System.out.println("end");
  }//end main
	
}
