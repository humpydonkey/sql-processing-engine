package dao;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import ra.Aggregator;

public class Schema  implements Serializable {

	private static final long serialVersionUID = -3104984314917863119L;
	private Map<String, Integer> indexMap;
	private Map<String, Integer> fullNameIndexMap;
	private int length;
	private Table tableName;
	private Column[] columnNames;
	private Expression[] columnSources;
	private DatumType[] colTypes;
	private Map<Function, Aggregator> aggregatorMap;
	
	public Schema(CreateTable table, Table tableName) throws Exception{	
		@SuppressWarnings("unchecked")
		List<ColumnDefinition> colDefs = table.getColumnDefinitions();
		Column[] cols = new Column[colDefs.size()];
		for(int i = 0; i < colDefs.size(); i++){
			ColumnDefinition col = (ColumnDefinition)colDefs.get(i);
			cols[i] = new Column(tableName, col.getColumnName());
		}
		
		length = cols.length;
		columnNames = cols;
		colTypes = new DatumType[length];
		columnSources = new Expression[length];
//		indexMap = new HashMap<String, Integer>(length);
		
		for(int i=0; i<length; i++){
//			indexMap.put(columnNames[i].getColumnName().toUpperCase(), i);
			colTypes[i] = convertColType(i, colDefs.get(i));
			columnSources[i] = cols[i];	//assign it as a column
			//compare the Column[] and ColumnDefinition has the same order index, if not throw exception 
			if(!columnNames[i].getColumnName().equals(colDefs.get(i).getColumnName()))
				throw new Exception("Column[] and ColumnDefinition has not the same order index.");
		}
		
		initialIndexMap(length, columnNames);
		initialTableName();
	}
	
	public Schema(Column[] colsIn, List<ColumnDefinition> colDefsIn) throws Exception{
		if(colsIn.length==0||colDefsIn.size()==0)
			throw new IllegalArgumentException("the number of columns/column definitions is 0.");
		if(colsIn.length!=colDefsIn.size())
			throw new IllegalArgumentException("Column[] size and DatumType[] size doesn't match : " + colsIn.length + "," + colDefsIn.size());
		
		length = colsIn.length;
		columnNames = colsIn;
		colTypes = new DatumType[length];
		columnSources = new Expression[length];
//		indexMap = new HashMap<String, Integer>(length);
		
		for(int i=0; i<length; i++){
//			indexMap.put(columnNames[i].getColumnName().toUpperCase(), i);
			colTypes[i] = convertColType(i, colDefsIn.get(i));
			columnSources[i] = colsIn[i];	//assign it as a column
			//compare the Column[] and ColumnDefinition has the same order index, if not throw exception 
			if(!columnNames[i].getColumnName().equals(colDefsIn.get(i).getColumnName()))
				throw new Exception("Column[] and ColumnDefinition has not the same order index.");
		}
		
		initialIndexMap(length, columnNames);
		initialTableName();
	}
	
	public Schema(Column[] colsIn, DatumType[] colTypesIn, Expression[] columnSourcesIn,  Map<Function, Aggregator> aggreMapIn){
		if(colsIn.length==0||colTypesIn.length==0)
			throw new IllegalArgumentException("the number of columns/column definitions is 0.");
		if(colsIn.length!=colTypesIn.length)
			throw new IllegalArgumentException("Column[] size and DatumType[] size doesn't match : " + colsIn.length + "," + colTypesIn.length);
		
		length = colsIn.length;
		columnNames = colsIn;
		colTypes = colTypesIn;
		columnSources = columnSourcesIn;

		initialIndexMap(length, columnNames);
		
//		indexMap = new HashMap<String, Integer>(length);
//		
//		for(int i=0; i<length; i++){
//			indexMap.put(columnNames[i].getColumnName().toUpperCase(), i);
//		}
//		
		if(aggreMapIn!=null)
			aggregatorMap = aggreMapIn;
		
		initialTableName();
	}
	
	
	private void initialIndexMap(int size, Column[] columnNamesIn){
		indexMap = new HashMap<String, Integer>(size);
		fullNameIndexMap = new HashMap<String, Integer>(size);
		
		for(int i=0; i<size; i++){
			String colName = columnNamesIn[i].getColumnName().toUpperCase();
			indexMap.put(colName, i);
			
			Table colTab = columnNamesIn[i].getTable();
			if(colTab!=null){
				if(colTab.getAlias()!=null)	//alias + colName
					fullNameIndexMap.put(colTab.getAlias().toUpperCase()+"."+colName, i);
				else						//table name + colName
					fullNameIndexMap.put(columnNamesIn[i].toString().toUpperCase(), i);
			}
				
		}
	}
	
	private void initialTableName(){
		StringBuilder tname = new StringBuilder("");		
		StringBuilder talias = new StringBuilder("");;
		
		for(int i=1; i<columnNames.length; i++){
			Table currColTab = columnNames[i].getTable();
			Table lastColTab = columnNames[i-1].getTable();
			if(currColTab!=null&&lastColTab!=null){
				if(currColTab.getAlias()!=null&&lastColTab.getAlias()!=null){
					if(!lastColTab.getAlias().equals(currColTab.getAlias())){
						talias.append('^');
						talias.append(currColTab.getAlias());
					}	
				}
				
				if(currColTab.getName()!=null&&lastColTab.getName()!=null){
					if(!lastColTab.getName().equals(currColTab.getName())){
						tname.append('^');
						tname.append(currColTab.getName());		
					}	
				}			
			}		
		}
		tableName = new Table();
		tableName.setName(tname.toString());
		if(talias.length()!=0)
			tableName.setAlias(talias.toString());
	}
	
	public String getTableName(){
		return tableName.getName();
	}
	
	public String getTableAlias(){
		return tableName.getAlias();
	}
	
	/**
	 * Get column name
	 * @param index
	 * @return
	 */
	public String getColName(int index){
		if(index>=columnNames.length)
			throw new IndexOutOfBoundsException();		
		return columnNames[index].getColumnName();
	}
	
	/**
	 * Get column type
	 * @param index
	 * @return
	 */
	public DatumType getColType(int index){
		if(index>=columnNames.length)
			throw new IndexOutOfBoundsException();		
		return colTypes[index];
	}
	
	/**
	 * Get column source
	 * @param index
	 * @return
	 */
	public Expression getColSource(int index){
		if(index>=columnNames.length)
			throw new IndexOutOfBoundsException();		
		return columnSources[index];
	}
	
	
	/**
	 * Convert column type from String to DatumType
	 * @param index
	 * @return
	 * @throws Exception
	 */
	private DatumType convertColType(int index, ColumnDefinition colDef) throws Exception{
//		ColumnDefinition colDef = getColDefinition(index);
		String typeStr = colDef.getColDataType().getDataType();
		DatumType type;
		typeStr = DataType.recognizeType(typeStr);
		switch(typeStr){

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
			
		default :
			throw new Exception("Wrong input type : " + typeStr);
		}
	}
	
	public int getLength(){
		return length;
	}
	
	public Column getColumnByIndex(int index){
		if(index>=columnNames.length)
			throw new IndexOutOfBoundsException();		
		return columnNames[index];
	}
	
	public Aggregator getAggregator(Function func){
		return aggregatorMap.get(func);
	}
	
	public Column[] getColumnNames(){		
		return columnNames;
	}
	
	public Expression[] getColumnSources(){
		return columnSources;
	}
	
	public DatumType[] getColTypes(){
		return colTypes;
	}
	
	/*
	 * First search full name index,if not found,
	 * then search attribute name index
	 */
	public int getColIndex(String colName){
		colName = colName.toUpperCase();
		
		Integer index = fullNameIndexMap.get(colName);
		if(index!=null)
			return index;
		else
			index = indexMap.get(colName);
		
		if(index==null)
			return -1;
		else
			return index.intValue();
	}
	
	public Map<String, Integer> getIndexMap(){
		return indexMap;
	}
	
	
	public static void main(String[] args) {

		try {
			CCJSqlParser parser = new CCJSqlParser(new FileInputStream(new File("data/NBA/nba11.sql")));
			CreateTable ct = parser.CreateTable();
			@SuppressWarnings("unchecked")
			List<ColumnDefinition> list = ct.getColumnDefinitions();
			for(ColumnDefinition cd : list){
				System.out.println(cd.getColumnName() + " : " + cd.getColDataType().getDataType());
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
