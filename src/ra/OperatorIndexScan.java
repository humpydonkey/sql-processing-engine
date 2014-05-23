package ra;

import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;

import jdbm.PrimaryStoreMap;
import jdbm.SecondaryTreeMap;
import net.sf.jsqlparser.statement.create.table.Index;
import sqlparse.IndexManager;
import sqlparse.TestEnvironment;

import common.Tools;

import dao.Datum;
import dao.DatumDate;
import dao.Schema;
import dao.Tuple;
import dao.Tuple.Row;

public class OperatorIndexScan implements Operator{

	public static enum IndexScanType{
		All, 
		NotEqualsTo,
		GreaterThan, GreaterThanEquals, 
		MinorThan, MinorThanEquals, 
		MinorGreaterThan, 	// A < X < B
		MinorThanGreaterEquals, 	// A < X <= B
		MinorEqualsGreaterThan, 	// A <= X < B
		MinorGreaterBothEquals, 	// A <= X <= B
		EqualsTo } 
	
	private IndexManager manager;
	private Schema schema;
	private OperatorCache cache;
	private IndexScanType type;
	private Index index;
	private Datum[] values;
	
	public OperatorIndexScan(IndexManager managerIn, Schema schemaIn, Index indexIn, Datum[] valuesIn, IndexScanType typeIn){
		
		Tools.debug("IndexScan created! Table:"+ schemaIn.getTableName() 
				+ ", Index: "+indexIn.getType()+" "+indexIn.getColumnsNames().toString()
				+ ", IndexScanType: "+ typeIn
				+ ", Values: "+Arrays.toString(valuesIn));
		manager = managerIn;
		schema = schemaIn;
		type = typeIn;
		index = indexIn;
		values = valuesIn;
	}
		
	public List<Tuple> getData(){ return cache.getData(); }
	
	public void init(){
		String tabName = schema.getTableName();
		boolean isPK = index.getType().equalsIgnoreCase("primary key");
		List<Tuple> data = null;
		//TODO primary tree map is not working currently
		isPK = false; 
		if(isPK){
			//PrimaryTreeMap<Row, Row> indexFile = manager.getPrmyTreeIndex(tabName, schema.getPrmyKeyType(), schema.getColTypes());
			//data = pulloutData(indexFile, values, schema);
		}else{	
			try {
				SecondaryTreeMap<Row, Long, Row> indexFile = manager.getPStoreScdr(tabName.toUpperCase(), schema.getScdrKey(index), schema);	
				data = pulloutData(indexFile, values, type, schema);
			} catch (UnexpectedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		cache = new OperatorCache(data);
	}
	
	public List<Tuple> pulloutData(SecondaryTreeMap<Row, Long, Row> indexFile, Datum[] values, IndexScanType type, Schema schema){
		List<Tuple> data = new ArrayList<Tuple>();
		if(values==null&&type!=IndexScanType.All){
			Tools.debug("-----------------------Error! Input Datum[] values is null!------------------------");
			type = IndexScanType.All;
		}
		
		//fromKey, toKey
		Datum[] toKey;
		switch(type){
		case All: 	//pull out all tuples
			for(Iterable<Long> iter : indexFile.values()){
				for(Long key : iter){
					Row row = indexFile.getPrimaryValue(key);
					data.add(new Tuple(row, schema));
				}		
			}
			break;
		case NotEqualsTo:
			rangeSingleValScan(values, data, indexFile, false);
			rangeSingleValScan(values, data, indexFile, true);
			break;
		case GreaterThanEquals:
			toKey = new Datum[]{values[1]};
			equalsValueScan(toKey, data, indexFile);
		case GreaterThan:
			rangeSingleValScan(values, data, indexFile, false);
			break;
		case MinorThanEquals:
			equalsValueScan(values, data, indexFile);
		case MinorThan:
			rangeSingleValScan(values, data, indexFile, true);
			break;
		case MinorThanGreaterEquals:	//A<X<=B
		case MinorGreaterBothEquals:	//A<=X<=B
			toKey = new Datum[]{values[1]};
			equalsValueScan(toKey, data, indexFile);
		case MinorEqualsGreaterThan:	//A<=X<B
		case MinorGreaterThan:			//A<X<B
			rangeValScan(values, data, indexFile);
			break;
		case EqualsTo:
			equalsValueScan(values, data, indexFile);
			break;
		}	
		return data;
	}
	
	
	public List<Long> findDataKeys() throws UnexpectedException{
		String tabName = schema.getTableName();
		SecondaryTreeMap<Row, Long, Row> indexFile = manager.getPStoreScdr(tabName, schema.getScdrKey(index), schema);	

		List<Long> data = new ArrayList<Long>();
		if(values==null&&type!=IndexScanType.All){
			Tools.debug("-----------------------Error! Input Datum[] values is null!------------------------");
			type = IndexScanType.All;
		}
		
		//fromKey, toKey
		Datum[] toKey;
		Datum[] fromKey;
		switch(type){
		case All: 	//pull out all tuples
			for(Iterable<Long> iter : indexFile.values()){
				for(Long key : iter){
					data.add(key);
				}		
			}
			break;
		case NotEqualsTo:
			rangeSingleValKeyScan(values, data, indexFile, false);
			rangeSingleValKeyScan(values, data, indexFile, true);
			break;
		case GreaterThanEquals:	//>=
			toKey = new Datum[]{values[1]};
			equalsValueKeyScan(toKey, data, indexFile);
		case GreaterThan:
			rangeSingleValKeyScan(values, data, indexFile, false);
			break;
		case MinorThanEquals:
			fromKey = new Datum[]{values[0]};
			equalsValueKeyScan(fromKey, data, indexFile);
		case MinorThan:
			rangeSingleValKeyScan(values, data, indexFile, true);
			break;
		case MinorThanGreaterEquals:	//A<X<=B
		case MinorGreaterBothEquals:	//A<=X<=B
			toKey = new Datum[]{values[1]};
			equalsValueKeyScan(toKey, data, indexFile);
		case MinorEqualsGreaterThan:	//A<=X<B
		case MinorGreaterThan:			//A<X<B
			rangeKeyScan(values, data, indexFile);
			break;
		case EqualsTo:
			equalsValueKeyScan(values, data, indexFile);
			break;
		}
		
		return data;
	}
	
	public void updateStoreMap(int[] colPos, List<Datum> setVals) throws UnexpectedException{
		String tabName = schema.getTableName();		
		PrimaryStoreMap<Long, Row> storeMap = manager.getPrmyStoreIndex(tabName, schema.getColTypes());
		SecondaryTreeMap<Row, Long, Row> indexFile = manager.buildScdr(tabName, schema.getScdrKey(index), schema, storeMap);
		
		if(values==null&&type!=IndexScanType.All){
			Tools.debug("-----------------------Error! Input Datum[] values is null!------------------------");
			type = IndexScanType.All;
		}
		
		//fromKey, toKey
		Datum[] toKey;
		Datum[] fromKey;
		switch(type){
		case All: 	//pull out all tuples
			for(Iterable<Long> iter : indexFile.values()){
				for(Long key : iter){
					Tools.debug("!!!!!!!!!!");
				}		
			}
			break;
		case NotEqualsTo:
			Tools.debug("!!!!!!!!!!");
			//rangeSingleValKeyScan(values, data, indexFile, false);
			//rangeSingleValKeyScan(values, data, indexFile, true);
			break;
		case GreaterThanEquals:	//>=
			toKey = new Datum[]{values[1]};
			Tools.debug("!!!!!!!!!!");
			//equalsValueKeyScan(toKey, data, indexFile);
		case GreaterThan:
			Tools.debug("!!!!!!!!!!");
			//rangeSingleValKeyScan(values, data, indexFile, false);
			break;
		case MinorThanEquals:
			fromKey = new Datum[]{values[0]};
			Tools.debug("!!!!!!!!!!");
			//equalsValueKeyScan(fromKey, data, indexFile);
		case MinorThan:
			Tools.debug("!!!!!!!!!!");
			//rangeSingleValKeyScan(values, data, indexFile, true);
			break;
		case MinorThanGreaterEquals:	//A<X<=B
		case MinorGreaterBothEquals:	//A<=X<=B
			toKey = new Datum[]{values[1]};
			Tools.debug("!!!!!!!!!!");
			//equalsValueKeyScan(toKey, data, indexFile);
		case MinorEqualsGreaterThan:	//A<=X<B
		case MinorGreaterThan:			//A<X<B
			rangeKeyUpdate(values, colPos, setVals, storeMap, indexFile);
			break;
		case EqualsTo:
			Tools.debug("!!!!!!!!!!");
			//equalsValueKeyScan(values, data, indexFile);
			break;
		}
	}
	
	/**
	 * Range index scan from index file, 
	 * use subMap(from, to), inclusive from, exclusive to: A<=X<B
	 * @param values
	 * @param data
	 * @param indexFile
	 */
	private void rangeValScan(Datum[] values, List<Tuple> data, SecondaryTreeMap<Row, Long, Row> indexFile){
		Row from = new Row(new Datum[]{values[0]});
		Row to = new Row(new Datum[]{values[1]});
		SortedMap<Row, Iterable<Long>> vals = indexFile.subMap(from, to);
		for(Iterable<Long> iter : vals.values()){
			for(Long key : iter){
				Row row = indexFile.getPrimaryValue(key);
				data.add(new Tuple(row, schema));
			}
		}
	}
	
	private void rangeKeyScan(Datum[] values, List<Long> keys, SecondaryTreeMap<Row, Long, Row> indexFile){
		Row from = new Row(new Datum[]{values[0]});
		Row to = new Row(new Datum[]{values[1]});
		SortedMap<Row, Iterable<Long>> vals = indexFile.subMap(from, to);
		for(Iterable<Long> iter : vals.values()){
			for(Long key : iter)
				keys.add(key);
		}
	}

	private void rangeKeyUpdate(Datum[] values, int[] colPos, List<Datum> setVals, PrimaryStoreMap<Long, Row> storeMap, SecondaryTreeMap<Row, Long, Row> indexFile){
		Row from = new Row(new Datum[]{values[0]});
		Row to = new Row(new Datum[]{values[1]});
		SortedMap<Row, Iterable<Long>> vals = indexFile.subMap(from, to);
		for(Iterable<Long> iter : vals.values()){
			for(Long key : iter){
				Row row = indexFile.getPrimaryValue(key);
				int i=0;
				//set value of columns
				for(Datum cellVal : setVals){
					row.setDatum(colPos[i++], cellVal);
				}
				//delete
				storeMap.remove(key);
				//update
				storeMap.putValue(row);
			}//iterable end		
		}		
	}
	
	/**
	 * Range index scan from index file,
	 * use headMap or tailMap, strictly minor than or greater than
	 * @param values
	 * @param data
	 * @param indexFile
	 * @param isMinor
	 */
	private void rangeSingleValScan(Datum[] values, List<Tuple> data, SecondaryTreeMap<Row, Long, Row> indexFile, boolean isMinor){
		if(values.length!=1)
			throw new IllegalArgumentException("Wrong Input of Datum[] values:"+Arrays.toString(values));
		
		SortedMap<Row, Iterable<Long>> matched = null;
		if(isMinor){
			//strictly minor than, headMap
			matched = indexFile.headMap(new Row(values));		
		}else{
			//strictly greater than tailMap
			matched = indexFile.tailMap(new Row(values));
		}
		
		for(Iterable<Long> iter : matched.values()){
			for(Long key : iter){
				Row row = indexFile.getPrimaryValue(key);
				data.add(new Tuple(row, schema));
			}
		}
	}
	
	private void rangeSingleValKeyScan(Datum[] values, List<Long> keys, SecondaryTreeMap<Row, Long, Row> indexFile, boolean isMinor){
		if(values.length!=1)
			throw new IllegalArgumentException("Wrong Input of Datum[] values:"+Arrays.toString(values));
		
		SortedMap<Row, Iterable<Long>> matched = null;
		if(isMinor){
			//strictly minor than, headMap
			matched = indexFile.headMap(new Row(values));		
		}else{
			//strictly greater than tailMap
			matched = indexFile.tailMap(new Row(values));
		}
		
		for(Iterable<Long> iter : matched.values()){
			for(Long key : iter)
				keys.add(key);
		}
	}
	
	/**
	 * Read specific value from secondary index file
	 * @param values: specific value, the size of values must be 1
	 * @param data: store the data read from disk
	 * @param indexFile
	 */
	private void equalsValueScan(Datum[] values, List<Tuple> data, SecondaryTreeMap<Row, Long, Row> indexFile){
		if(values.length!=1)
			throw new IllegalArgumentException("Wrong Input of Datum[] values:"+Arrays.toString(values));
		//value lookup
		Iterable<Row> matchedVals = indexFile.getPrimaryValues(new Row(values));
		if(matchedVals!=null)
			for(Row row : matchedVals)
				data.add(new Tuple(row, schema));
	}
	
	private void equalsValueKeyScan(Datum[] values, List<Long> keys, SecondaryTreeMap<Row, Long, Row> indexFile){
		if(values.length!=1)
			throw new IllegalArgumentException("Wrong Input of Datum[] values:"+Arrays.toString(values));
		//value lookup
		Iterable<Long> matchedVals = indexFile.get(new Row(values));
		if(matchedVals!=null)
			for(Long key: matchedVals)
				keys.add(key);
	}
	
	@Override
	public Tuple readOneTuple() {
		return cache.readOneTuple();
	}

	@Override
	public void reset() {
		cache.reset();
	}

	@Override
	public long getLength() { 
		return cache.getLength();
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void close() {
		cache.close();
	}

	
	@SuppressWarnings("serial")
	public static void main(String[] args){
		TestEnvironment env = new TestEnvironment();
		Schema schema = env.generateSchema("LINEITEM");
		Index idx = new Index();
		idx.setType("primary key");
		idx.setColumnsNames(new ArrayList<String>(){{add("shipdate");}});
		Datum[] range = new Datum[]{new DatumDate(1992,2,7)};
		IndexManager manager = new IndexManager("test/idx/MyIndex");
		IndexScanType type = IndexScanType.EqualsTo;
		OperatorIndexScan scan = new OperatorIndexScan(manager, schema, idx, range, type);
		scan.init();
		Tuple tup;
		while((tup = scan.readOneTuple())!=null){
			System.out.println(tup);
		}
		scan.close();
	}
}
