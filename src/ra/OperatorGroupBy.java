package ra;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import sql2ra.Config;
import sql2ra.SQLEngine;

import common.Tools;

import dao.Datum;
import dao.Schema;
import dao.Tuple;

public class OperatorGroupBy implements Operator{

	private Operator input;
	private Aggregator[] aggregators;
	private List<Column> groupbyCols;	//group by columns
	private boolean noGroupBy;
	private File swapDir;
	private boolean swap;
	public final static String NOGROUPBYKEY = "!@#$%^&*Key"; 
	
	//store the group by column values as key, 
	//and the tuple of group as value. 
	private Map<String, Tuple> groupMap;	
	
	@SuppressWarnings("rawtypes")
	public OperatorGroupBy(Operator inputIn, File swapDirIn, List columnsIn, Aggregator... aggregatorsIn){
		input = inputIn;
		swapDir = swapDirIn;
		if(swapDir==null)
			swap=false;
		else
			swap=true;
		
		groupMap = new LinkedHashMap<String, Tuple>();
		aggregators = aggregatorsIn;	//could be null
		
		if(columnsIn==null||columnsIn.size()==0)
			noGroupBy = true;
		else{
			groupbyCols = new ArrayList<Column>(columnsIn.size());
			noGroupBy = false;
						
			if(columnsIn.get(0) instanceof Column){
				for(Object obj : columnsIn)		//convert to List<Column>
					groupbyCols.add((Column)obj);	
			}else{
				//should be the object of ColumnIndex
				throw new UnsupportedOperationException("Not supported yet."); 
			}
		}		
	}
	
	public List<File> dumpToDisk(){
		File rawFile=null;
		List<File> groupFiles = new ArrayList<File>();
		
		if(input instanceof OperatorScan){
			OperatorScan scan = (OperatorScan)input;
			rawFile = scan.getFile();
		}else
			swap=false;
		
		if(swap){			
			Schema schema = input.getSchema();
			int readCount=0;
			while((readOneTuple())!=null){
				readCount++;
				
				if(groupMap.size()>=Config.Buffer_SIZE){
					try(BufferedReader br = new BufferedReader(new FileReader(rawFile))){
						for(int i=0; i<readCount; i++)
							br.readLine();	//skip count lines that already read

						//continue reading
						String line;
						while((line = br.readLine())!=null){
							Tuple tup = new Tuple(line, schema);
							//if exist in groupMap then update
							updateGroupMap(tup);
						}

					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					flushBuffer(groupMap, groupFiles);
				}
			}
			
			flushBuffer(groupMap, groupFiles);
			
		}else
			Tools.debug("Error! Cannot dump to disk, it didn't satisfy swap condition.");
		
		return groupFiles;
	}
	
	public List<Tuple> dump(){
		//Doing Group By
		while((readOneTuple())!=null){}
		List<Tuple> groupedTuples = new ArrayList<Tuple>(groupMap.size());
		for(Entry<String, Tuple> entry : groupMap.entrySet()){
			groupedTuples.add(entry.getValue());
		}
		
		return groupedTuples;
	}
	
	private void flushBuffer(Map<String, Tuple> gMap, List<File> gFiles){
		File gfile = new File(swapDir.getPath()+"/GroupResult"+gFiles.size());
		gFiles.add(gfile);
		try(BufferedWriter bw = new BufferedWriter(new FileWriter(gfile))){
			for(Entry<String, Tuple> entry : gMap.entrySet()){
				bw.write(entry.getValue().toString());
				bw.newLine();
			}
			bw.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}				
		gMap.clear();
	}

	@Override
	public Tuple readOneTuple() {
		Tuple tuple = input.readOneTuple();

		if(tuple==null)
			return null;		
		else
			return groupby(tuple);
	}

	
	@Override
	public void reset() {
		input.reset();
	}
	
	@Override
	public Schema getSchema() {
		return input.getSchema();
	}
	
	
	public Tuple groupby(Tuple tuple){
		String key;
		if(noGroupBy)
			key = NOGROUPBYKEY;
		else
			key = generateKey(tuple);
		
		//update column value
		groupMap.put(key, tuple);
		//update aggregate value
		for(Aggregator aggr : aggregators){
			aggr.aggregate(tuple, key);
		}
		
		return tuple;
	}


	@Override
	public long getLength() {
		return input.getLength();
	}
	
	//The key is the combined values of group by columns
	private String generateKey(Tuple tuple){
		StringBuffer sb = new StringBuffer();
		for(Column col : groupbyCols){
			Datum data = tuple.getDataByName(col);
			sb.append(data.toString());
		}
		return sb.toString();
	}

	
	private boolean updateGroupMap(Tuple tup){
		String key = generateKey(tup);
		if(groupMap.containsKey(key)){
			groupMap.put(key, tup);
			//update aggregate value
			for(Aggregator aggr : aggregators){
				aggr.aggregate(tup, key);
			}
			return true;
		}else
			return false;
	}

	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception{
		File swap = new File("test/");
		File dataDir = new File("test/cp2_grade");
		File sql = new File("test/cp2_littleBig/tpch_schemas.sql");
		
		Config.setSwapDir(swap);
		FileReader stream = new FileReader(sql);
		CCJSqlParser parser = new CCJSqlParser(stream);
		Statement stmt;
		
		SQLEngine myParser = new SQLEngine(dataDir);
		
		while((stmt = parser.Statement()) !=null){		
			if(stmt instanceof CreateTable)	
				myParser.create(stmt);
		}
		
		Table tab = new Table(null,"lineitem");
		CreateTable ct = SQLEngine.globalCreateTables.get("LINEITEM");
		@SuppressWarnings("rawtypes")
		List groupCols = new ArrayList<Column>();
		Column col1 = new Column(tab, "partkey");
		Column col2 = new Column(tab, "returnflag");
		Column col3 = new Column(tab, "shipmode");
		Column col4 = new Column(tab, "orderkey");
		Column col5 = new Column(tab, "suppkey");
		Column col6 = new Column(tab, "shipdate");
		groupCols.add(col1);
		groupCols.add(col2);
		
		Map<String, Column> colsMapper = new HashMap<String, Column>();
		colsMapper.put(col1.toString(), col1);
		colsMapper.put(col2.toString(), col2);
		colsMapper.put(col3.toString(), col3);
		colsMapper.put(col4.toString(), col4);
		colsMapper.put(col5.toString(), col5);
		colsMapper.put(col6.toString(), col6);
		Schema schema = Schema.schemaFactory(colsMapper, ct, tab);
		OperatorScan scan = new OperatorScan(new File(dataDir+"/lineitem.dat"),schema);

		
		OperatorGroupBy gb = new OperatorGroupBy(scan, swap, groupCols);
		List<File> files = gb.dumpToDisk();
		for(File f : files)
			System.out.println(f.getPath());
	}
}
