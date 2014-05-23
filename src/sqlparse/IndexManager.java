package sqlparse;

import java.io.File;
import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;

import jdbm.PrimaryStoreMap;
import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.SecondaryKeyExtractor;
import jdbm.SecondaryTreeMap;
import jdbm.Serializer;
import jdbm.SerializerInput;
import jdbm.SerializerOutput;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import ra.Operator;
import ra.OperatorScan;
import common.TimeCalc;
import common.Tools;
import dao.Datum;
import dao.Datum.CastError;
import dao.DatumBool;
import dao.DatumDate;
import dao.DatumDouble;
import dao.DatumLong;
import dao.DatumString;
import dao.DatumType;
import dao.Schema;
import dao.Tuple;
import dao.Tuple.Row;

public class IndexManager {
	private static final int CommitCount = 5000;
	private RecordManager manager;
	private String name;
	
	public IndexManager(String nameIn){
		try {
			name = nameIn;
			manager = RecordManagerFactory.createRecordManager(name);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String getName(){ return name; }
	
	public void commit(){
		try {
			manager.commit();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void reopen(String nameIn){
		close();
		try {
			name = nameIn;
			manager = RecordManagerFactory.createRecordManager(name);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void close(){
		try {
			manager.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Get Primary Store Index by Table name 
	 * @param tabName: Table name
	 * @param types: Datum Types
	 * @return
	 */
	public PrimaryStoreMap<Long, Tuple.Row> getPrmyStoreIndex(String tabName, DatumType[] types){
		return manager.storeMap(
				tabName, 
				new RowSerializer(types)
				);
	}
	
	
	/**
	 * Get Primary B+ Tree Index by Table Name
	 * @param tabName: Table Name
	 * @param keyTypes: Key Column Types
	 * @param valTypes: Value Column Types
	 * @return
	 */
	public PrimaryTreeMap<Tuple.Row, Tuple.Row> getPrmyTreeIndex(String tabName, DatumType[] keyTypes, DatumType[] valTypes){
		return manager.treeMap(
				tabName,//index name
				new RowSerializer(valTypes), //value
				new RowSerializer(keyTypes) //key
				);
	}
	

	/**
	 * Get Secondary Index on Primary Store Index
	 * @param tabName: Table name
	 * @param keyCols: key Columns
	 * @param schema: Schema of Value of Primary Index
	 * @return 
	 * @throws UnexpectedException 
	 */
	public SecondaryTreeMap<Tuple.Row, Long, Tuple.Row> getPStoreScdr(String tabName, List<Column> keyCols, Schema schema) throws UnexpectedException{
		PrimaryStoreMap<Long, Tuple.Row> storeMap = getPrmyStoreIndex(tabName.toUpperCase(), schema.getColTypes());
		if(storeMap.size()==0){
			Tools.debug("Cannot find Primary Stroe Map: "+tabName);
			return null;
		}else
			return buildScdr(tabName, keyCols, schema, storeMap);
	}
	
	/*
	 * Get SecondaryTreeMap on Primary Tree Index 
	 */
	public SecondaryTreeMap<Tuple.Row, Tuple.Row, Tuple.Row> getPTreeScdr(String tabName, int[] keyIndex, Schema schema) throws UnexpectedException{
		int keyLen = keyIndex.length;
		DatumType[] keyTypes = new DatumType[keyLen];
		List<Column> keyCols = new ArrayList<Column>(keyLen);
		for(int i=0; i<keyLen; i++){
			keyTypes[i] = schema.getColType(keyIndex[i]);
			keyCols.add((Column)schema.getColSource(keyIndex[i]));
		}			
		
		PrimaryTreeMap<Tuple.Row, Tuple.Row> treeMap = getPrmyTreeIndex(tabName, keyTypes, schema.getColTypes());
		if(treeMap==null){
			Tools.debug("Cannot find Primary Stroe Map: "+tabName);
			return null;
		}else
			return buildScdr(tabName, keyCols, schema, treeMap);
	}
	
	/**
	 * Create a Primary Store Map, and based on it, create some Secondary Indexes
	 * @param oper: data source
	 * @return
	 * @throws IOException 
	 */
	public PrimaryStoreMap<Long, Tuple.Row> buildPrmyStore(String tabName, Operator oper, List<Column> prmyCols, List<Column>[] scdrColsArr) throws IOException{
		int scdrSize = 1;	//at least one fake primary secondary index
		if(scdrColsArr!=null)
			scdrSize += scdrColsArr.length;
		
		Schema schema = oper.getSchema();
	
		//Primary Store index
		PrimaryStoreMap<Long, Tuple.Row> storeMap = manager.storeMap(
				tabName, 
				new RowSerializer(schema.getColTypes())
				);
		
		//Secondary index (primary)
		buildScdr(tabName, prmyCols, schema, storeMap);

		//Secondary index (secondary)
		if(scdrColsArr!=null){
			for(int i=1; i<scdrSize; i++){
				List<Column> scdrCols = scdrColsArr[i-1];
				buildScdr(tabName, scdrCols, schema, storeMap);
			}
		}
		
		List<Tuple> tups = new ArrayList<Tuple>();
		Tuple tup;	//read out all tuples
		while((tup = oper.readOneTuple())!=null){ tups.add(tup); }
		boolean[] isAsc = new boolean[prmyCols.size()];
		Arrays.fill(isAsc, true);
		Collections.sort(tups, Tuple.getComparator(prmyCols, isAsc));
		
		int i=0;
		for(Tuple t : tups){
			Tuple.Row row = t.getRow();
			storeMap.putValue(row);
			i++;
			if(i>CommitCount){i=0; manager.commit();manager.clearCache();}
		}
		manager.commit();
		manager.clearCache();
		return storeMap;
	}
	
	
	/**
	 * Build primary B+ tree index 
	 * @param colNames: column keys
	 * @param oper: data source
	 * @return B+ tree index
	 * @throws IOException
	 */
	public PrimaryTreeMap<Tuple.Row, Tuple.Row> buildPrmyTree(String tabName, List<Column> keyCols, List<Column>[] scdrKeys, Operator oper) throws IOException{
		if(manager==null){
			throw new UnexpectedException("RecordManager is null!");
		}
		
		int keyLen = keyCols.size();
		DatumType[] keyTypes = new DatumType[keyLen];
		int[] keyIndice = new int[keyLen];
		Schema schema = oper.getSchema();
		for(int i=0; i<keyLen; i++){
			Column col = keyCols.get(i);
			int index = schema.getColIndex(col.getColumnName());
			if(index<0)
				throw new UnexpectedException("Can not find column index, column name: "+col.getColumnName());
			else{
				keyIndice[i] = index;
				keyTypes[i] = schema.getColType(index);
			}
		}
		
		PrimaryTreeMap<Tuple.Row, Tuple.Row> index = manager.treeMap(
				tabName,//index name
				new RowSerializer(schema.getColTypes()), //value
				new RowSerializer(keyTypes) //key
				);

		//create secondary index
		if(scdrKeys!=null)
			for(List<Column> scdrKey : scdrKeys)
				buildScdr(tabName, scdrKey, schema, index);
		
		int i=0;
		Tuple tup;
		while((tup = oper.readOneTuple())!=null){
			Datum[] dataArr = tup.getDataArr();
			Datum[] keyVals = new Datum[keyLen];
			for(int j=0; j<keyLen; j++){			
				keyVals[j] = tup.getData(keyIndice[j]);			
			}
			
			index.put(new Tuple.Row(keyVals), new Tuple.Row(dataArr));

			i++;
			if(i>CommitCount){i=0; manager.commit();manager.clearCache();}
		}
		
		manager.commit();

		return index;
	}
	
	
	public void insertInStoreMap(Row row, Schema schema) throws IOException{
		if(manager==null){
			throw new UnexpectedException("RecordManager is null!");
		}
		String tabName = schema.getTableName();
	
		//Primary Store index
		PrimaryStoreMap<Long, Tuple.Row> storeMap = manager.storeMap(
				tabName, 
				new RowSerializer(schema.getColTypes())
				);
		
		//Secondary index (primary)
		buildScdr(tabName, schema.getPrmyKey(), schema, storeMap);

		//Secondary index (secondary)
		List<Column>[] scdrColsArr = schema.getAllScdrKeys();
		if(scdrColsArr!=null){
			for(int i=0; i<scdrColsArr.length; i++){
				List<Column> scdrCols = scdrColsArr[i];
				buildScdr(tabName, scdrCols, schema, storeMap);
			}
		}

		storeMap.putValue(row);
	
		manager.commit();
		manager.clearCache();
	}
	
	public void deleteFromStoreMap(List<Long> keys, Schema schema) throws IOException{
		if(manager==null){
			throw new UnexpectedException("RecordManager is null!");
		}
		String tabName = schema.getTableName();
	
		//Primary Store index
		PrimaryStoreMap<Long, Tuple.Row> storeMap = manager.storeMap(
				tabName, 
				new RowSerializer(schema.getColTypes())
				);
		
		//Secondary index (primary)
		buildScdr(tabName, schema.getPrmyKey(), schema, storeMap);

		//Secondary index (secondary)
		List<Column>[] scdrColsArr = schema.getAllScdrKeys();
		if(scdrColsArr!=null){
			for(int i=0; i<scdrColsArr.length; i++){
				List<Column> scdrCols = scdrColsArr[i];
//				if(i==1)
//					continue;
				buildScdr(tabName, scdrCols, schema, storeMap);
			}
		}

		for(Long key : keys)
			storeMap.remove(key);
	
		manager.commit();
		manager.clearCache();
	}
	
	
	/**
	 * Build Secondary B+ tree index
	 * @param colNames: key columns
	 * @param valueSchema: the schema of the value
	 * @param storeMap: Primary Store Map Index
	 * @return Secondary B+ tree index
	 * @throws UnexpectedException 
	 */
	private SecondaryTreeMap<Tuple.Row, Long, Tuple.Row> buildScdr(String tabName, List<Column> colNames, Schema schema, PrimaryStoreMap<Long, Tuple.Row> storeMap) throws UnexpectedException{
		//get key column indice
		final int keyLen = colNames.size();
		final int[] keyIndice = new int[keyLen];
		DatumType[] keyTypes = new DatumType[keyLen];
		for(int i=0; i<keyLen; i++){
			Column col = colNames.get(i);
			int index = schema.getColIndex(col.getColumnName());
			if(index<0)
				throw new UnexpectedException("Can not find column index, column name: "+col.getColumnName());
			else{
				keyIndice[i] = index;
				keyTypes[i] = schema.getColType(index);
			}
		}
		
		//construct secondary tree map
		return storeMap.secondaryTreeMap(
				generateIndexName(tabName, colNames),
				new SecondaryKeyExtractor<Tuple.Row, Long, Tuple.Row>(){
					@Override
					public Tuple.Row extractSecondaryKey(Long key, Tuple.Row value) {
						Tuple.Row row = new Tuple.Row(keyLen);
						for(int i=0; i<keyLen; i++){
							row.setDatum(i, value.getDatum(keyIndice[i]));
						}
						return row;
					}
				}, 
				new RowSerializer(keyTypes)
			);
	}


	private SecondaryTreeMap<Tuple.Row, Tuple.Row, Tuple.Row> buildScdr(String tabName, List<Column> colNames, Schema schema, PrimaryTreeMap<Tuple.Row, Tuple.Row> prmyTreeMap) throws UnexpectedException{
		//get key column indice
		final int keyLen = colNames.size();
		final int[] keyIndice = new int[keyLen];
		DatumType[] keyTypes = new DatumType[keyLen];
		for(int i=0; i<keyLen; i++){
			Column col = colNames.get(i);
			int index = schema.getColIndex(col.getColumnName());
			if(index<0)
				throw new UnexpectedException("Can not find column index, column name: "+col.getColumnName());
			else{
				keyIndice[i] = index;
				keyTypes[i] = schema.getColType(index);
			}
		}
		
		//construct secondary tree map
		return prmyTreeMap.secondaryTreeMap(
				generateIndexName(tabName, colNames),
				new SecondaryKeyExtractor<Tuple.Row, Tuple.Row, Tuple.Row>(){
					@Override
					public Tuple.Row extractSecondaryKey(Tuple.Row key, Tuple.Row value) {
						Tuple.Row row = new Tuple.Row(keyLen);
						for(int i=0; i<keyLen; i++){
							row.setDatum(i, value.getDatum(keyIndice[i]));
						}
						return row; //key
					}
				}, 
				new RowSerializer(keyTypes)
			);
	}

	
	
	private String generateIndexName(String tabName, List<Column> keyCols){
		StringBuilder sb = new StringBuilder(tabName);
		for(Column col : keyCols)	//construct index name
			sb.append("_"+col.getColumnName());
		return sb.toString();
	}
	
	
	
	/****************************	Inner Class		**********************************/
	public static class RowSerializer implements Serializer<Tuple.Row>{
		private DatumType[] types ;
		
		public RowSerializer(DatumType[] typesIn){
			types = typesIn;
		}
		
		@Override
		public Tuple.Row deserialize(SerializerInput in) throws IOException,
				ClassNotFoundException {
			Datum[] data = new Datum[types.length];
			for(int i=0; i<types.length; i++){
				DatumType type = types[i];
				Datum d;	
				switch(type){
				case Bool:	
					d = new DatumBool(in.readBoolean());
					break;
				case Long:
					d = new DatumLong(in.readLong());
					break;
				case Double:
					d = new DatumDouble(in.readDouble());
					break;
				case String:
					d = new DatumString(in.readUTF());
					break;
				case Date:
					d = new DatumDate(in.readInt(), in.readInt(), in.readInt());
					break;
				default:
					throw new IOException("Unknown DatumType: "+type.toString());
				}
				data[i] = d;
			}

			return new Tuple.Row(data);
		}

		@Override
		public void serialize(SerializerOutput out, Tuple.Row row)
				throws IOException {
			for(Datum data : row.getData()){
				DatumType type = data.getType();
				try{
					switch (type) {
					case Bool:	
						out.writeBoolean(data.toBool());
						break;
					case Long:
						out.writeLong(data.toLong());
						break;
					case Double:
						out.writeDouble(data.toDouble());
						break;
					case String:
						out.writeUTF(data.toString());
						break;
					case  Date:
						DatumDate date = (DatumDate)data;
						out.writeInt(date.getYear());
						out.writeInt(date.getMonth());
						out.writeInt(date.getDay());
						break;
					default:
						throw new IOException("Unknown DatumType: "+type.toString());
					}	
				}catch(CastError e){
					e.printStackTrace();
				}
			}
		}
	}

	
	public static void main(String[] args) throws IOException {
		String name = "test/idx/1/MyIndexManager";
		IndexManager indxMngr = new IndexManager(name);
		TestEnvironment envir = new TestEnvironment();
		Schema schema = envir.generateSchema("lineitem");
		File lineitem = new File("test/Checkpoint3DataTest/lineitem.dat");
		Operator data = new OperatorScan(lineitem, schema);
		List<Column> cols = new ArrayList<Column>();
		cols.add(new Column(new Table(null,"lineitem"),"orderkey"));
		
		List<Column> scdrKey = new ArrayList<Column>();
		Column shipdate = new Column(new Table(null, "lineitem"), "shipdate");
		scdrKey.add(shipdate);
		@SuppressWarnings("unchecked")
		List<Column>[] scdrKeys = new ArrayList[1];
		scdrKeys[0] = scdrKey;
		
//		testPrimaryTree(indxMngr, cols, scdrKeys, data, schema);
		
		testPrimaryStore(indxMngr, cols, scdrKeys, data, schema, name);
	}
	
	private static void testPrimaryTree(IndexManager indxMngr, List<Column> keys, List<Column>[] scdrKeys, Operator data, Schema schema) throws IOException{
		/****************     Primary Tree Map    ******************/
		TimeCalc.begin();
		PrimaryTreeMap<Tuple.Row, Tuple.Row> prmy = indxMngr.buildPrmyTree("lineitem", keys, scdrKeys, data);
		TimeCalc.end("Finish Primary Tree Map Building! " + prmy.size());
		
		Tuple.Row row1 = prmy.get(new Tuple.Row(new Datum[]{new DatumLong(42598)}));
		Tuple.Row row2 = prmy.get(new Tuple.Row(new Datum[]{new DatumLong(19776)}));
		Tuple.Row row3 = prmy.get(new Tuple.Row(new Datum[]{new DatumLong(8675)}));
		Tuple.Row row4 = prmy.get(new Tuple.Row(new Datum[]{new DatumLong(39077)}));
		System.out.println(row1.toString());
		System.out.println(row2.toString());
		System.out.println(row3.toString());
		System.out.println(row4.toString());
		
		System.out.println(prmy.get(new Tuple.Row((new Datum[]{new DatumLong(33510)}))));
		/***************     Secondary Tree Map       *****************/
		SecondaryTreeMap<Tuple.Row, Tuple.Row, Tuple.Row> scdrTreeIndex = indxMngr.getPTreeScdr(schema.getTableName(), new int[]{10}, schema);
		//1997-08-25
		SortedMap<Row, Iterable<Row>> srow1 = scdrTreeIndex.subMap(new Tuple.Row(new Datum[]{new DatumDate(1992,2,28)}), new Tuple.Row(new Datum[]{new DatumDate(1996,2,1)}));
		SortedMap<Row, Iterable<Row>> srow2 = scdrTreeIndex.headMap(new Tuple.Row(new Datum[]{new DatumDate(1997,8,24)}));

		for(Row row : scdrTreeIndex.getPrimaryValues(new Tuple.Row(new Datum[]{new DatumDate(1992,2,26)})))
			System.out.println(row);
		
//		for(Row row : scdrTreeIndex.getPrimaryValues(new Tuple.Row(new Datum[]{new DatumDate(1992,2,27)})))
//			System.out.println(row);
		
		
		for(Entry<Row, Iterable<Row>> keyMatch : srow1.entrySet()){
			for(Row row : scdrTreeIndex.getPrimaryValues(keyMatch.getKey())){
				System.out.print(row+" ");
			}
			System.out.println();
		}
		
//		for(Entry<Row, Iterable<Row>> keyMatch : srow2.entrySet()){
//			for(Row row : keyMatch.getValue()){
//				Row value = scdrTreeIndex.getPrimaryValue(row);
//				System.out.print(value+" ");
//			}
//			System.out.println();
//		}
			
	}

	
	private static void testPrimaryStore(IndexManager indxMngr, List<Column> key, List<Column>[] scdrKeys, Operator data, Schema schema,String name) throws IOException{
		/****************     Primary Store Map  & Secondary Tree Map   ******************/
		TimeCalc.begin();
		data.reset();
		PrimaryStoreMap<Long, Tuple.Row> storeMap = indxMngr.buildPrmyStore("lineitem", data, key, scdrKeys);
		TimeCalc.end("Finish Primary Store Map & Secondary Tree Map Building! " + storeMap.size());
		
		indxMngr.close();
		indxMngr.reopen(name);
		
		SecondaryTreeMap<Tuple.Row, Long, Tuple.Row> prmyTreeIndex = indxMngr.getPStoreScdr(schema.getTableName(), scdrKeys[0], schema);
		Iterable<Tuple.Row> srow1 = prmyTreeIndex.getPrimaryValues(new Tuple.Row(new Datum[]{new DatumDate(1992,2,26)}));
		Iterable<Tuple.Row> srow2 = prmyTreeIndex.getPrimaryValues(new Tuple.Row(new Datum[]{new DatumDate(1994,2,27)}));
		Iterable<Tuple.Row> srow3 = prmyTreeIndex.getPrimaryValues(new Tuple.Row(new Datum[]{new DatumDate(1997,8,24)}));
		Iterable<Tuple.Row> srow4 = prmyTreeIndex.getPrimaryValues(new Tuple.Row(new Datum[]{new DatumDate(1992,2,27)}));
		
		for(Tuple.Row row : srow1)
			System.out.println(row.toString());
		for(Tuple.Row row : srow2)
			System.out.println(row.toString());
		for(Tuple.Row row : srow3)
			System.out.println(row.toString());
		for(Tuple.Row row : srow4)
			System.out.println(row.toString());
		
		/***************     Secondary Tree Map       *****************/
		SecondaryTreeMap<Tuple.Row, Long, Tuple.Row> scdrTreeIndex = indxMngr.getPStoreScdr(schema.getTableName(), scdrKeys[0], schema);
		//1997-08-25
		SortedMap<Row, Iterable<Long>> ssrow1 = scdrTreeIndex.subMap(new Tuple.Row(new Datum[]{new DatumDate(1996,1,1)}), new Tuple.Row(new Datum[]{new DatumDate(1996,2,1)}));
		SortedMap<Row, Iterable<Long>> ssrow2 = scdrTreeIndex.headMap(new Tuple.Row(new Datum[]{new DatumDate(1997,8,24)}));

		for(Row row : scdrTreeIndex.getPrimaryValues(new Tuple.Row(new Datum[]{new DatumDate(1992,2,26)})))
			System.out.println(row);

		for(Row row : scdrTreeIndex.getPrimaryValues(new Tuple.Row(new Datum[]{new DatumDate(1992,2,27)})))
			System.out.println(row);

		for(Entry<Row, Iterable<Long>> keyMatch : ssrow1.entrySet()){
			for(Row row : scdrTreeIndex.getPrimaryValues(keyMatch.getKey())){
				System.out.print(row+" ");
			}
			System.out.println();
		}
		
		for(Entry<Row, Iterable<Long>> keyMatch : ssrow2.entrySet()){
			for(Row row : scdrTreeIndex.getPrimaryValues(keyMatch.getKey())){
				System.out.print(row+" ");
			}
			System.out.println();
		}
	}
}
