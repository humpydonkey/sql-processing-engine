package dao;

import java.io.Serializable;
import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import ra.Aggregator;
import sqlparse.TestEnvironment;

/**
 * Schema of a table
 * @author Asia
 * 
 */
public class Schema implements Serializable {

	private static final long serialVersionUID = -3104984314917863119L;
	private Map<String, Integer> colIndexMap;
	private Map<String, Integer> fullNameColIndexMap;
	private int length;
	private Table tableName;
	private Column[] columnNames;
	private Expression[] columnSources;
	private DatumType[] colTypes;
	private List<Index> indexes;
	private Map<Function, Aggregator> aggregatorMap;
	private int[] rawPosition;
	private int[] pkeyPos;
	private List<int[]> scdkeyPosList;

	public static Schema schemaFactory(Map<String, Column> colsMapper,
			CreateTable ct, Table tableName) {
		@SuppressWarnings("unchecked")
		List<ColumnDefinition> allColDefs = ct.getColumnDefinitions();

		List<Column> colsInUse = new ArrayList<Column>();
		List<ColumnDefinition> colDefsInUse = new ArrayList<ColumnDefinition>();
		List<Integer> rawPosition = new ArrayList<Integer>();
		for (int i = 0; i < allColDefs.size(); i++) {
			ColumnDefinition colDef = allColDefs.get(i);
			Column col = new Column(tableName, colDef.getColumnName());

			// if colsMapper == null, it will map all the attributes
			if (colsMapper != null && colsMapper.size()!=0) {
				// Add columns that only appeared(in use) in SQL
				String colFullName = null;
				if (col.getTable().getAlias() != null)
					colFullName = col.getTable().getAlias() + "."
							+ col.getColumnName();
				else
					colFullName = col.toString();
				if (colsMapper.containsKey(colFullName)) {
					// Tools.debug("add column: "+col.toString()+",  position: "+i);
					rawPosition.add(i);
					colsInUse.add(col);
					colDefsInUse.add(colDef);
				}
			} else {
				rawPosition.add(i);
				colsInUse.add(col);
				colDefsInUse.add(colDef);
			}
		}

		// to array
		int size = colsInUse.size();
		Column[] cols = new Column[size];
		ColumnDefinition[] colDefs = new ColumnDefinition[size];
		int[] rawPos = new int[size];
		colsInUse.toArray(cols);
		colDefsInUse.toArray(colDefs);
		for (int i = 0; i < size; i++)
			rawPos[i] = rawPosition.get(i);

		@SuppressWarnings("unchecked")
		List<Index> indexes = ct.getIndexes();

		return new Schema(cols, colDefs, rawPos, indexes);
	}

	
	@SuppressWarnings("unchecked")
	public static boolean indexEquals(Index idx1, Index idx2){
		List<String> cols1 = idx1.getColumnsNames();
		List<String> cols2 = idx2.getColumnsNames();
		if(cols1.size()!=cols2.size())
			return false;
		else{
			for(int i=0; i<cols1.size(); i++)
				if(!cols1.get(i).equalsIgnoreCase(cols2.get(i)))
					return false;			
		}
		return true;
	}
	
	public Schema(Column[] colsIn, ColumnDefinition[] colDefsIn, int[] rawPosIn, List<Index> indexesIn) {
		if (colsIn.length == 0 || colDefsIn.length == 0)
			throw new IllegalArgumentException(
					"the number of columns/column definitions is 0.");
		if (colsIn.length != colDefsIn.length)
			throw new IllegalArgumentException(
					"Column[] size and DatumType[] size doesn't match : "
							+ colsIn.length + "," + colDefsIn.length);

		length = colsIn.length;
		columnNames = colsIn;
		colTypes = new DatumType[length];
		columnSources = new Expression[length];
		rawPosition = rawPosIn;
		indexes = indexesIn;
		
		for (int i = 0; i < length; i++) {
			colTypes[i] = convertColType(i, colDefsIn[i]);
			columnSources[i] = colsIn[i]; // assign it as a column
		}

		initialIndexMap(length, columnNames);
		initialTableName();
		initialAllIndexes(indexes);
	}

	public Schema(Column[] colsIn, DatumType[] colTypesIn,
			Expression[] columnSourcesIn, List<Index> indexesIn, Map<Function, Aggregator> aggreMapIn) {
		if (colsIn.length == 0 || colTypesIn.length == 0)
			throw new IllegalArgumentException(
					"the number of columns/column definitions is 0.");
		if (colsIn.length != colTypesIn.length)
			throw new IllegalArgumentException(
					"Column[] size and DatumType[] size doesn't match : "
							+ colsIn.length + "," + colTypesIn.length);

		length = colsIn.length;
		columnNames = colsIn;
		colTypes = colTypesIn;
		columnSources = columnSourcesIn;
		indexes = indexesIn;
		rawPosition = new int[length];
		for (int i = 0; i < length; i++)
			rawPosition[i] = i;

		if (aggreMapIn != null)
			aggregatorMap = aggreMapIn;

		initialIndexMap(length, columnNames);
		initialTableName();
		initialAllIndexes(indexes);
	}

	private void initialIndexMap(int size, Column[] columnNamesIn) {
		colIndexMap = new HashMap<String, Integer>(size);
		fullNameColIndexMap = new HashMap<String, Integer>(size);

		for (int i = 0; i < size; i++) {
			String colName = columnNamesIn[i].getColumnName().toUpperCase();
			colIndexMap.put(colName, i);

			Table colTab = columnNamesIn[i].getTable();
			if (colTab != null) {
				if (colTab.getAlias() != null) // alias + colName
					fullNameColIndexMap.put(colTab.getAlias().toUpperCase() + "."
							+ colName, i);
				else
					// table name + colName
					fullNameColIndexMap.put(columnNamesIn[i].toString()
							.toUpperCase(), i);
			}

		}
	}

	private void initialTableName() {
		Table tab = columnNames[0].getTable();
		StringBuilder tname = new StringBuilder(tab.getName());
		StringBuilder talias = new StringBuilder("");
		;
		if (tab.getAlias() != null)
			talias.append(tab.getAlias());

		for (int i = 1; i < columnNames.length; i++) {
			Table currColTab = columnNames[i].getTable();
			Table lastColTab = columnNames[i - 1].getTable();
			if (currColTab != null && lastColTab != null) {
				if (currColTab.getAlias() != null
						&& lastColTab.getAlias() != null) {
					if (!lastColTab.getAlias().equals(currColTab.getAlias())) {
						talias.append('^');
						talias.append(currColTab.getAlias());
					}
				}

				if (currColTab.getName() != null
						&& lastColTab.getName() != null) {
					if (!lastColTab.getName().equals(currColTab.getName())) {
						tname.append('^');
						tname.append(currColTab.getName());
					}
				}
			}
		}
		tableName = new Table();
		tableName.setName(tname.toString());
		if (talias.length() != 0)
			tableName.setAlias(talias.toString());
	}
	
	
	private void initialAllIndexes(List<Index> indexes){
		//initial key positions		
		if(indexes!=null){
			scdkeyPosList = new ArrayList<int[]>(indexes.size()-1);
			try{
				for(Index index : indexes){
					if(index.getType().equalsIgnoreCase("PRIMARY KEY")){
						pkeyPos = initialIndex(index, colIndexMap);
					}else{
						scdkeyPosList.add(initialIndex(index, colIndexMap)); 
					}
				}
			}catch(UnexpectedException e){
				//TODO to be handled
				e.printStackTrace();
			}
				
		}
	}


	private int[] initialIndex(Index index, Map<String, Integer> colIndexMap) throws UnexpectedException {
		@SuppressWarnings("unchecked")
		List<String> colNames = index.getColumnsNames();
		int[] indexPosition = new int[colNames.size()];
		int i = 0;
		for (String name : colNames) {
			Integer pos = colIndexMap.get(name.toUpperCase());
			if(pos==null)
				return null;
			indexPosition[i++] = pos.intValue();
		}

		return indexPosition;
	}
	
	/**
	 * Convert column type from String to DatumType
	 * 
	 * @param index
	 * @return
	 * @throws Exception
	 */
	private DatumType convertColType(int index, ColumnDefinition colDef) {
		// ColumnDefinition colDef = getColDefinition(index);
		String typeStr = colDef.getColDataType().getDataType();
		DatumType type;
		typeStr = DataType.recognizeType(typeStr);
		switch (typeStr) {

		case DataType.LONG:
			type = DatumType.Long;
			return type;

		case DataType.DOUBLE:
			type = DatumType.Double;
			return type;

		case DataType.BOOL:
			type = DatumType.Bool;
			return type;

		case DataType.STRING:
			type = DatumType.String;
			return type;

		case DataType.DATE:
			type = DatumType.Date;
			return type;

		default:
			throw new IllegalArgumentException("Wrong input type : " + typeStr);
		}
	}

	public String getTableName() {
		return tableName.getName();
	}

	public String getTableAlias() {
		return tableName.getAlias();
	}

	/**
	 * Get column type
	 * 
	 * @param index
	 * @return
	 */
	public DatumType getColType(int index) {
		if (index >= columnNames.length)
			throw new IndexOutOfBoundsException();
		return colTypes[index];
	}

	/**
	 * Get column type by column name
	 * 
	 * @param name
	 * @return
	 */
	public DatumType getColTypeByName(String name) {
		int index = getColIndex(name);
		if (index < 0) {
			if (name.contains(".")) {
				name = name.split("\\.")[1];
				index = getColIndex(name);
				if (index >= 0)
					return colTypes[index];
				else
					return null;
			} else
				return null;
		} else
			return colTypes[index];
	}

	/**
	 * Get column source
	 * 
	 * @param index
	 * @return
	 */
	public Expression getColSource(int index) {
		if (index >= columnNames.length)
			throw new IndexOutOfBoundsException();
		return columnSources[index];
	}

	public int getLength() { return length; }

	public Aggregator getAggregator(Function func) { return aggregatorMap.get(func); }

	public Column[] getColumnNames() { return columnNames; }

	public Expression[] getColumnSources() { return columnSources; }

	public DatumType[] getColTypes() { return colTypes; }

	public Map<String, Integer> getColIndexMap() { return colIndexMap; }

	public int getRawPosition(int i) { return rawPosition[i]; }

	public int[] getPrmyKeyIndex() { return pkeyPos; }
	
	public List<int[]> getScdrKeyIndexes(){ return scdkeyPosList; }
	
	public List<Index> getIndexesInfo(){ return indexes; }
	
	public List<Column> getPrmyKey(){
		List<Column> cols = new ArrayList<Column>(pkeyPos.length);
		for(int i=0; i<pkeyPos.length; i++){
			Expression colSource = columnSources[pkeyPos[i]];
			if(colSource instanceof Column)
				cols.add((Column)colSource);
			else
				try {
					throw new UnexpectedException("Wrong Schema, column source is not a Column:"+colSource.toString());
				} catch (UnexpectedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		return cols;
	}

	
	public DatumType[] getPrmyKeyType() {
		DatumType[] types = new DatumType[pkeyPos.length];
		for (int i = 0; i < pkeyPos.length; i++) {
			types[i] = colTypes[pkeyPos[i]];
		}
		return types;
	}
	
	
	/**
	 * Get secondary index type
	 * @param idxCmp
	 * @return
	 */
	public DatumType[] getScdrKeyType(Index idxCmp){		
		int[] pos = getScdrKeyPos(idxCmp);
		if(pos==null)
			return null;
		
		DatumType[] types = new DatumType[pos.length];
		for(int j=0; j<pos.length; j++)
			types[j] = colTypes[pos[j]];
		return types;
	}
	
	/**
	 * Get secondary index column
	 * @param idxCmp
	 * @return
	 */
	public List<Column> getScdrKey(Index idxCmp){
		int[] pos = getScdrKeyPos(idxCmp);
		if(pos==null)
			return null;
		List<Column> cols = new ArrayList<Column>(pos.length);
		for(int i : pos)
			cols.add(columnNames[i]);
		return cols;
	}
	
	/**
	 * Get secondary index position
	 * @param idxCmp
	 * @return
	 */
	public int[] getScdrKeyPos(Index idxCmp){
		Index pk  = indexes.get(0);
		if(indexEquals(pk, idxCmp))
			return pkeyPos;
		
		for(int i=1; i<indexes.size(); i++){
			Index idx = indexes.get(i);
			if(indexEquals(idx, idxCmp)){
				return scdkeyPosList.get(i-1);
			}
		}
		
		return null;
	}
	
	
	/**
	 * Get All secondary keys
	 * @return
	 */
	public List<Column>[] getAllScdrKeys(){
		@SuppressWarnings("unchecked")
		List<Column>[] scdrIndexes = new List[scdkeyPosList.size()];
		for(int i=0; i<scdkeyPosList.size(); i++){
			//create List<Column> of each secondary index
			int[] scdrPos = scdkeyPosList.get(i);
			List<Column> scdrCols = new ArrayList<Column>();
			for(int j=0; j<scdrPos.length; j++){
				//link column source for each secondary index positions
				Expression colSource = columnSources[scdrPos[j]];
				if(colSource instanceof Column)
					scdrCols.add((Column)colSource);
				else
					try {
						throw new UnexpectedException("Wrong Schema, column source is not a Column:"+colSource.toString());
					} catch (UnexpectedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}
			scdrIndexes[i] = scdrCols;
		}
		return scdrIndexes;
	}
	

	/**
	 * Get Column name by index
	 * @param index
	 * @return
	 */
	public Column getColNameByIndex(int index) {
		if (index >= columnNames.length)
			throw new IndexOutOfBoundsException();
		return columnNames[index];
	}
	

	/**
	 * Get Column name by String name
	 * 
	 * @param name
	 * @return
	 */
	public Column getColNameByName(String name) {
		int index = getColIndex(name);
		if (index < 0) {
			if (name.contains(".")) {
				name = name.split("\\.")[1];
				index = getColIndex(name);
				if (index >= 0)
					return columnNames[index];
				else
					return null;
			} else
				return null;
		} else
			return columnNames[index];
	}

	/*
	 * First search full name index,if not found, then search attribute name
	 * index
	 */
	public int getColIndex(String colName) {
		colName = colName.toUpperCase();

		Integer index = fullNameColIndexMap.get(colName);
		if (index != null)
			return index;
		else
			index = colIndexMap.get(colName);

		if (index == null)
			return -1;
		else
			return index.intValue();
	}


	public void setRawPosition(int i, int position) { rawPosition[i] = position; }

	public void setRawPosition(String colName, int position) {
		int index = getColIndex(colName);
		if (index >= 0)
			setRawPosition(index, position);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (Column col : columnNames)
			sb.append(col.getWholeColumnName() + "|");
		sb.deleteCharAt(sb.length() - 1);
		sb.append("\n");

		for (Expression source : columnSources)
			sb.append(source.toString() + "|");
		sb.deleteCharAt(sb.length() - 1);
		sb.append("\n");

		for (DatumType type : colTypes)
			sb.append(type.toString() + " | ");
		sb.deleteCharAt(sb.length() - 2);
		sb.append("\n");

		return sb.toString();
	}

	public static void main(String[] args) {

		TestEnvironment envir = new TestEnvironment();
		Schema schema = envir.generateSchema("lineitem");
		System.out.println(schema.toString());

	}

}
