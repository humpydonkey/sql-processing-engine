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
		
		//System.out.println("begin");
		int i;
		String dataDirStr = "data/NBA/";
		String sqlFilePath = "data/cp1_graded_sqls/nba04.sql";
		
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
  }//end main
	
}
