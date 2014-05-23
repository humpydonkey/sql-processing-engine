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
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import sqlparse.Config;
import sqlparse.SQLEngine;
import common.TimeCalc;
import common.Tools;
import dao.Schema;
import dao.Tuple;


public class Main {

	public static void main(String args[]){
		//input example: --data [data] [sqlfile1] [sqlfile2] ...
		//           or  --data [data] --swap [swap] [sqlfile1] [sqlfile2] ...
		//--build 
		//--data test/Checkpoint3DataTest/ --swap test/swap --index test/idx test/Checkpoint3DataTest/tpch_schemas.sql test/Checkpoint3DataTest/tpch07a.sql test/Checkpoint3DataTest/tpch10a.sql test/Checkpoint3DataTest/tpch12a.sql test/Checkpoint3DataTest/tpch16a.sql
		//buildForTest();	//"test/Checkpoint4_25Mb/query01.sql", "test/Checkpoint4_25Mb/query02.sql","test/Checkpoint4_25Mb/query03.sql", "test/Checkpoint4_25Mb/query04.sql", "test/Checkpoint4_25Mb/query05.sql", "test/Checkpoint4_25Mb/query06.sql"
		//args = new String[]{"--data", "test/Checkpoint4_25Mb/", "--swap", "test/swap", "--index", "test/idx", "test/Checkpoint4_25Mb/tpch_schemas.sql", "test/Checkpoint4_25Mb/query06.sql"};
		if(args.length>=3){
			boolean ifBuild = false;
			if(args[0].equals("--build")){
				ifBuild = true;
			}

			File dataDir = null;
			File swapDir = null;
			String indexDir = null;
			List<File> sqlFiles = new ArrayList<File>();
			for(int i=0; i<args.length; i++){
				if(args[i].equalsIgnoreCase("--data")){
					//data directory
					String dataDirStr = args[++i];
					dataDir = new File(dataDirStr);
				}else if(args[i].equalsIgnoreCase("--swap")){
					swapDir = new File(args[++i]);
				}else if(args[i].equalsIgnoreCase("--index")){
					//index
					indexDir = args[++i]+"/"+Config.IndexFileName;
					//data dir
					for(int j=i+1; j<args.length; j++)
						sqlFiles.add(new File(args[j]));
				}
			}
			
			if(dataDir==null||sqlFiles.size()==0){
				System.out.println("Input error, dataDir: " + dataDir + "SQL File size: " + sqlFiles.size());
				return;
			}
			
			if(ifBuild){
				//build index
				precompute(indexDir, dataDir, sqlFiles);
			}else{
				//print the results
				runSQL(dataDir, swapDir, indexDir, sqlFiles);				
			}
		}
		
//		if(Config.DebugMode)
//			testSpecificCP2();

	}

	
	/**
	 * PreComputation Step, build index
	 * @param indexDir: Index Directory
	 * @param dataDir: Data Directory
	 * @param sqlFiles: SQL Files will be executed in the next step
	 */
	public static void precompute(String indexDir, File dataDir, List<File> sqlFiles){
		
		for (File sql : sqlFiles){
			try{
				FileReader stream = new FileReader(sql);
				CCJSqlParser parser = new CCJSqlParser(stream);
				Statement stmt;
				while((stmt = parser.Statement()) !=null){		
					if(stmt instanceof CreateTable){
						//build index
						CreateTable ct = (CreateTable)stmt;
						Schema schema = Schema.schemaFactory(null, ct, ct.getTable());
						SQLEngine.buildIndex(indexDir, dataDir, schema);
					}
				}
			}catch (IOException e){
	    		e.printStackTrace();
	    	}catch (ParseException e){
	    		e.printStackTrace();
	    	}
		}
	}
	
	
	/**
	 * Execute SQL Files
	 * @param dataDir: Data Directory
	 * @param swapDir: Swap Directory
	 * @param indexDir: Index Directory
	 * @param sqlFiles: SQL Files will be executed
	 * @return
	 */
	public static List<Tuple> runSQL(File dataDir, File swapDir,
			String indexDir, List<File> sqlFiles) {
		TimeCalc.begin();
		List<Tuple> results = null;
		Config.setSwapDir(swapDir);

		for (File sql : sqlFiles) {
			try {
				FileReader stream = new FileReader(sql);
				CCJSqlParser parser = new CCJSqlParser(stream);
				Statement stmt;
				SQLEngine myParser = new SQLEngine(dataDir, indexDir);

				while ((stmt = parser.Statement()) != null) {
					Tools.debug("Now parsing: "+stmt.toString());
					if (stmt instanceof CreateTable) {
						SQLEngine.create(stmt);
					} else {
						if (stmt instanceof Select) {
							Select sel = (Select) stmt;
							results = myParser.select(sel.getSelectBody());
							for(Tuple t : results)
								System.out.println(t.toString());
						} else if (stmt instanceof Insert) {
							boolean res = myParser.insert((Insert) stmt);
							if(res)
								Tools.debug("Insert successful");
							else
								Tools.debug("Insert failed");
						} else if (stmt instanceof Update) {
							boolean res;
							Update updt = (Update)stmt;
							if(updt.getTable().getName().equalsIgnoreCase("lineitem")&&updt.getColumns().get(0).toString().equalsIgnoreCase("shipdate")){
								res = myParser.update2(updt);
							}else
								res = myParser.update((Update)stmt);
														
							if(res)
								Tools.debug("Update successful");
							else
								Tools.debug("Update failed");
						} else if (stmt instanceof Delete) {
							boolean res = myParser.delete((Delete)stmt);
							if(res)
								Tools.debug("Delete successful");
							else
								Tools.debug("Delete failed");
						} else {
							System.out.println("PANIC! I don't know how to handle"+ stmt);
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}

		}// end for

		TimeCalc.end("Executing SQL finished!");
		return results;
	}
	
	
	@SuppressWarnings("serial")
	public static void buildForTest(){
		precompute("test/idx/"+Config.IndexFileName, new File("test/Checkpoint4_25Mb/"), new ArrayList<File>(){{add(new File("test/Checkpoint4_25Mb/tpch_schemas.sql"));}});
	}
	
	public static List<Tuple> testSpecificSQL(String dataDirStr, String indexDirStr, String... sqlPaths){	
		File dataDir = new File(dataDirStr);
		File swapDir = Config.getSwapDir();
		List<File> sqlfiles = new ArrayList<File>();
		for(String sqlPath : sqlPaths){
			sqlfiles.add(new File(sqlPath));
			Tools.debug("SQL file: "+sqlPath);
		}

		List<Tuple> results = runSQL(dataDir, swapDir, indexDirStr, sqlfiles);
		
		return results;
	}

	
	public static List<Tuple> testSpecificCP2(){
		//mocking input		//cp2_grade   cp2_littleBig
		String dataDirStr = "test/cp2_littleBig/";
		String indexDirStr = "test/MyIndexManager";
		String[] sqlFilePaths = {"test/cp2_littleBig/tpch07a.sql"};
		
		List<Tuple> tups = testSpecificSQL(dataDirStr,indexDirStr, sqlFilePaths);
		for(Tuple t : tups)
			t.printTuple();
		Tools.debug("End.\n");
		return tups;
	}
	
	
	public static List<Tuple> testSpecificCP1(){
		//mocking input
		String dataDirStr = "test/cp1/";
		String indexDirStr = "test/MyIndexManager";
		String[] sqlFilePaths = {"test/cp1/tpch5.sql"};

		List<Tuple> tups = testSpecificSQL(dataDirStr, indexDirStr, sqlFilePaths);
		for(Tuple t : tups)
			t.printTuple();
		Tools.debug("End.\n");
		return tups;
	}
	
	
	public static void testAll_CP1(){
		//mocking input
		String dataDirStr = "test/cp1/";//"data/NBA/";  //"/data/tpch/";
		String sqlFilePath = "test/cp1/";
		String indexDirStr = "test/MyIndexManager";
		
		File dataDir = new File(dataDirStr);
		File swapDir = null;

		File sqlDir = new File(sqlFilePath);
		File[] sqls = sqlDir.listFiles(new FileTypeFilter("sql"));
		
		for(File sqlfile : sqls){
			List<File> sqlfiles = new ArrayList<File>();
			sqlfiles.add(sqlfile);
			Tools.debug("SQL file: "+sqlfile.getName());
			runSQL(dataDir, swapDir, indexDirStr, sqlfiles);
			
			pauseConsole();
		}
		Tools.debug("\nEnd.");
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
