package sql2ra;

import java.io.File;
import java.io.FileReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import ra.OperatorScan;

import common.TimeCalc;

import dao.CompareAttribute;
import dao.Schema;
import dao.Tuple;

public class Test {

	public static void main(String[] args) {
		try{
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

			Column col1 = new Column(tab, "suppkey");
			Column col2 = new Column(tab, "returnflag");
			Column col3 = new Column(tab, "shipmode");
			Column col4 = new Column(tab, "orderkey");
			Column col5 = new Column(tab, "partkey");
			Column col6 = new Column(tab, "shipdate");

			Map<String, Column> colsMapper = new HashMap<String, Column>();
			colsMapper.put(col1.toString(), col1);
			colsMapper.put(col2.toString(), col2);
//			colsMapper.put(col3.toString(), col3);
//			colsMapper.put(col4.toString(), col4);
			
			Schema schema = Schema.schemaFactory(null, ct, tab);
			OperatorScan scan = new OperatorScan(new File(dataDir+"/lineitem.dat"),schema);
			int length = 100000;
			List<Tuple> tups = new LinkedList<Tuple>();
			int count=0;
			while(count<=length){
				tups.add(scan.readOneTuple());
				count++;
			}
			TimeCalc.begin(0);
			Collections.sort(tups, Tuple.getComparator(new CompareAttribute[]{new CompareAttribute(col1,true)}));
			
//			for(Tuple tup : tups)
//				System.out.println(tup.toString());
			
			TimeCalc.end(0, "Finish Sorting!");
		}catch(Exception e){
			e.printStackTrace();
		}
	}

}
