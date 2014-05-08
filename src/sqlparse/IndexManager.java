package sqlparse;

import java.io.File;
import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
	 * Get Secondary Index
	 * @param tabName: Table name
	 * @param keyCols: key Columns
	 * @param schema: Schema of Value of Primary Index
	 * @return 
	 * @throws UnexpectedException 
	 */
	public SecondaryTreeMap<Tuple.Row, Long, Tuple.Row> getScdrIndex(String tabName, List<Column> keyCols, Schema schema) throws UnexpectedException{
		PrimaryStoreMap<Long, Tuple.Row> storeMap = getPrmyStoreIndex(tabName, schema.getColTypes());
		if(storeMap==null){
			Tools.debug("Cannot find Primary Stroe Map: "+tabName);
			return null;
		}else
			return buildScdr(tabName, keyCols, schema, storeMap);
	}
	
	/**
	 * Create a Primary Store Map, and based on it, create some Secondary Indexes
	 * @param oper: data source
	 * @return
	 * @throws IOException 
	 */
	public PrimaryStoreMap<Long, Tuple.Row> buildAllIndice(String tabName, Operator oper, List<Column> prmyCols, List<Column>[] scdrColsArr) throws IOException{
		
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
		for(int i=1; i<scdrSize; i++){
			List<Column> scdrCols = scdrColsArr[i-1];
			buildScdr(tabName, scdrCols, schema, storeMap);
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
	public PrimaryTreeMap<Tuple.Row, Tuple.Row> buildPrmy(String tabName, List<Column> colNames, Operator oper) throws IOException{
		if(manager==null){
			throw new UnexpectedException("RecordManager is null!");
		}
		
		int keyLen = colNames.size();
		DatumType[] keyTypes = new DatumType[keyLen];
		int[] keyIndice = new int[keyLen];
		Schema schema = oper.getSchema();
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
		
		PrimaryTreeMap<Tuple.Row, Tuple.Row> index = manager.treeMap(
				tabName,//index name
				new RowSerializer(schema.getColTypes()), //value
				new RowSerializer(keyTypes) //key
				);

		
		int i=0;
		Tuple tup;
		while((tup = oper.readOneTuple())!=null){
			Datum[] dataArr = tup.getDataArr();
			Datum[] keyVals = new Datum[keyLen];
			for(int j=0; j<keyLen; j++){			
				keyVals[j] = tup.getData(keyIndice[j]);			
			}
			
			Tuple.Row key = new Tuple.Row(keyVals);
			Tuple.Row val = new Tuple.Row(dataArr);
			
			index.put(key, val);
			//Row row = index.get(key);
			i++;
			if(i>CommitCount){i=0; manager.commit();manager.clearCache();}
		}
		
		manager.commit();

		return index;
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
		IndexManager indxMngr = new IndexManager("test/MyIndexManager");
		TestEnvironment envir = new TestEnvironment();
		Schema schema = envir.generateSchema("lineitem");
		File lineitem = new File("test/Checkpoint3DataTest/lineitem.dat");
		Operator data = new OperatorScan(lineitem, schema);
		List<Column> cols = new ArrayList<Column>();
		cols.add(new Column(new Table(null,"lineitem"),"orderkey"));

		/****************     Primary Tree Map     ******************/
//		TimeCalc.begin();
//		PrimaryTreeMap<Tuple.Row, Tuple.Row> prmy = indxMngr.buildPrmy(cols, data);
//		TimeCalc.end("Finish Primary Tree Map Building! " + prmy.size());
//		
//		Tuple.Row row1 = prmy.get(new Tuple.Row(new Datum[]{new DatumLong(42598)}));
//		Tuple.Row row2 = prmy.get(new Tuple.Row(new Datum[]{new DatumLong(19776)}));
//		Tuple.Row row3 = prmy.get(new Tuple.Row(new Datum[]{new DatumLong(8675)}));
//		Tuple.Row row4 = prmy.get(new Tuple.Row(new Datum[]{new DatumLong(39077)}));
//		System.out.println(row1.toString());
//		System.out.println(row2.toString());
//		System.out.println(row3.toString());
//		System.out.println(row4.toString());
		
		/****************     Primary Store Map  & Secondary Tree Map   ******************/
		TimeCalc.begin();
		data.reset();
		PrimaryStoreMap<Long, Tuple.Row> storeMap = indxMngr.buildAllIndice("lineitem", data, cols, null);
		TimeCalc.end("Finish Primary Store Map & Secondary Tree Map Building! " + storeMap.size());
		SecondaryTreeMap<Tuple.Row, Long, Tuple.Row> scdrTreeIndex = indxMngr.getScdrIndex(schema.getTableName(), cols, schema);
		Iterable<Tuple.Row> srow1 = scdrTreeIndex.getPrimaryValues(new Tuple.Row(new Datum[]{new DatumLong(42598)}));
		Iterable<Tuple.Row> srow2 = scdrTreeIndex.getPrimaryValues(new Tuple.Row(new Datum[]{new DatumLong(19776)}));
		Iterable<Tuple.Row> srow3 = scdrTreeIndex.getPrimaryValues(new Tuple.Row(new Datum[]{new DatumLong(8675)}));
		Iterable<Tuple.Row> srow4 = scdrTreeIndex.getPrimaryValues(new Tuple.Row(new Datum[]{new DatumLong(39077)}));
		
		for(Tuple.Row row : srow1)
			System.out.println(row.toString());
		for(Tuple.Row row : srow2)
			System.out.println(row.toString());
		for(Tuple.Row row : srow3)
			System.out.println(row.toString());
		for(Tuple.Row row : srow4)
			System.out.println(row.toString());
	}

}
