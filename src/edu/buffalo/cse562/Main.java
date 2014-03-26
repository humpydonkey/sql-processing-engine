package edu.buffalo.cse562;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import sql2ra.SQLParser;
import dao.Tuple;

public class Main {

	public static void main(String args[]){
		//input example: --data [data] [sqlfile1] [sqlfile2] ...
		//           or  --data [data] --swap [swap] [sqlfile1] [sqlfile2] ...
		
		if(args.length>=3){
			if(args[0].equalsIgnoreCase("--data")){
				//data directory
				String dataDirStr = args[1];
				File dataDir = new File(dataDirStr);
				
				int i=2;	//index of of argument sqlfile

				//swap directory
				File swapDir = null;
				if(args[2].equalsIgnoreCase("--swap")){
					if(args.length>3){
						swapDir = new File(args[3]);
						i = 4;
					}
					else
						System.out.println("Input error: "+args.toString());
				}
				
				//sql files
				List<File> sqlFiles = new ArrayList<File>();
				for(; i<args.length; i++){
					sqlFiles.add(new File(args[i]));
				}
				
				runSQL(dataDir, swapDir, sqlFiles);
				
			}else
				System.out.println("Input error: "+args.toString());
		}
		
		
		testSpecificSQL();
		//testAll_CP1();
	}
	
	
	public static void testSpecificSQL(){
		//mocking input
		String dataDirStr = "test/data/";//"data/NBA/";  //"/data/tpch/";
		File sqlFile = new File("test/cp1_sqls/tpch3.sql");
		
		File dataDir = new File(dataDirStr);
		File swapDir = null;
		List<File> sqlfiles = new ArrayList<File>();
		sqlfiles.add(sqlFile);
		System.out.println("SQL file: "+sqlFile.getName());
		runSQL(dataDir, swapDir, sqlfiles);
		System.out.println("\nEnd.");
	}

	
	public static void testAll_CP1(){
		//mocking input
		String dataDirStr = "test/data/";//"data/NBA/";  //"/data/tpch/";
		String sqlFilePath = "test/cp1_sqls/";
		
		File dataDir = new File(dataDirStr);
		File swapDir = null;

		File sqlDir = new File(sqlFilePath);
		File[] sqls = sqlDir.listFiles();
		
		for(File sqlfile : sqls){
			List<File> sqlfiles = new ArrayList<File>();
			sqlfiles.add(sqlfile);
			System.out.println("SQL file: "+sqlfile.getName());
			runSQL(dataDir, swapDir, sqlfiles);
			
			pauseConsole();
		}
		System.out.println("\nEnd.");
	}

	
	public static void runSQL(File dataDir, File swapDir, List<File> sqlFiles){
		for (File sql : sqlFiles){
			try{
				FileReader stream = new FileReader(sql);   		
				CCJSqlParser parser = new CCJSqlParser(stream);
				Statement stmt;
		
				SQLParser myParser = new SQLParser(dataDir);
				
				while((stmt = parser.Statement()) !=null){		
					if(stmt instanceof CreateTable)	
						myParser.create(stmt);
					else {
						
					 if(stmt instanceof Select){
						//System.out.println("I would now evaluate:" + stmt);
						Select sel = (Select)stmt;
						List<Tuple> tuples = myParser.select(sel.getSelectBody());
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
	}

	
	public static void pauseConsole(){
		//pause console
		System.out.println("Press Enter to continue...");
		Scanner keyboard = new Scanner(System.in);
		keyboard.nextLine();
		System.out.println();	
	}
}
