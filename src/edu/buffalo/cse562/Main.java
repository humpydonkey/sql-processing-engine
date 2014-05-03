package edu.buffalo.cse562;

import io.FileTypeFilter;

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
import sqlparse.Config;
import sqlparse.SQLEngine;
import common.TimeCalc;
import common.Tools;
import dao.Tuple;

public class Main {

	public static void main(String args[]){
		//input example: --data [data] [sqlfile1] [sqlfile2] ...
		//           or  --data [data] --swap [swap] [sqlfile1] [sqlfile2] ...
		//--data test/cp2_grade --swap test/  test/cp2_grade/tpch07a.sql
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
				
				//print the results
				List<Tuple> results = runSQL(dataDir, swapDir, sqlFiles);
				for(Tuple t : results)
					System.out.println(t.toString());

			}else
				System.out.println("Input error: "+args.toString());
		}
		
		if(Config.DebugMode)
			testSpecificCP2();
		//testAll_CP1();
	}

	
	public static List<Tuple> testSpecificSQL(String dataDirStr, String... sqlPaths){	

		File dataDir = new File(dataDirStr);
		File swapDir = Config.getSwapDir();
		List<File> sqlfiles = new ArrayList<File>();
		for(String sqlPath : sqlPaths){
			sqlfiles.add(new File(sqlPath));
			Tools.debug("SQL file: "+sqlPath);
		}
			
		List<Tuple> results = runSQL(dataDir, swapDir, sqlfiles);
		
		return results;
	}

	
	public static List<Tuple> testSpecificCP2(){
		//mocking input		//cp2_grade   cp2_littleBig
		String dataDirStr = "test/cp2_littleBig/";
		String[] sqlFilePaths = {"test/cp2_littleBig/tpch07a.sql"};
		
		List<Tuple> tups = testSpecificSQL(dataDirStr, sqlFilePaths);
		for(Tuple t : tups)
			t.printTuple();
		Tools.debug("End.\n");
		return tups;
	}
	
	public static List<Tuple> testSpecificCP1(){
		//mocking input
		String dataDirStr = "test/cp1/";
		String[] sqlFilePaths = {"test/cp1/tpch5.sql"};

		List<Tuple> tups = testSpecificSQL(dataDirStr, sqlFilePaths);
		for(Tuple t : tups)
			t.printTuple();
		Tools.debug("End.\n");
		return tups;
	}
	
	public static void testAll_CP1(){
		//mocking input
		String dataDirStr = "test/cp1/";//"data/NBA/";  //"/data/tpch/";
		String sqlFilePath = "test/cp1/";
		//String swapPath = null;
		
		File dataDir = new File(dataDirStr);
		File swapDir = null;

		File sqlDir = new File(sqlFilePath);
		File[] sqls = sqlDir.listFiles(new FileTypeFilter("sql"));
		
		for(File sqlfile : sqls){
			List<File> sqlfiles = new ArrayList<File>();
			sqlfiles.add(sqlfile);
			Tools.debug("SQL file: "+sqlfile.getName());
			runSQL(dataDir, swapDir, sqlfiles);
			
			pauseConsole();
		}
		Tools.debug("\nEnd.");
	}

	
	public static List<Tuple> runSQL(File dataDir, File swapDir, List<File> sqlFiles){
		TimeCalc.begin();
		List<Tuple> results = null;
		Config.setSwapDir(swapDir);

		for (File sql : sqlFiles){
			try{
				FileReader stream = new FileReader(sql);
				CCJSqlParser parser = new CCJSqlParser(stream);
				Statement stmt;
				
				SQLEngine myParser = new SQLEngine(dataDir);
				
				while((stmt = parser.Statement()) !=null){		
					if(stmt instanceof CreateTable)	
						SQLEngine.create(stmt);
					else {
						
					 if(stmt instanceof Select){
						//System.out.println("I would now evaluate:" + stmt);
						Select sel = (Select)stmt;
						results = myParser.select(sel.getSelectBody());
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
		
		TimeCalc.end("Executing SQL finished!");
		return results;
	}

	
	public static void pauseConsole(){
		//pause console
		System.out.println("Press Enter to continue...");
		Scanner keyboard = new Scanner(System.in);
		keyboard.nextLine();
		System.out.println();	
		keyboard.close();
	}
}
