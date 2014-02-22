package dao;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

public class Schema {

	private Map<String, Integer> indexMap;
	private int length;
	private Column[] columns;
	private DatumType[] colTypes;
	
	public Schema(Column[] colsIn, List<ColumnDefinition> colDefsIn) throws Exception{
		if(colsIn.length==0||colDefsIn.size()==0)
			throw new IllegalArgumentException("the number of columns/column definitions is 0.");
		if(colsIn.length!=colDefsIn.size())
			throw new IllegalArgumentException("Column[] size and DatumType[] size doesn't match : " + colsIn.length + "," + colDefsIn.size());
		
		length = colsIn.length;
		columns = colsIn;
		colTypes = new DatumType[length];
		indexMap = new HashMap<String, Integer>(length);
		
		for(int i=0; i<length; i++){
			indexMap.put(columns[i].getColumnName(), i);
			colTypes[i] = convertColType(i, colDefsIn.get(i));
			//compare the Column[] and ColumnDefinition has the same order index, if not throw exception 
			if(!columns[i].getColumnName().equals(colDefsIn.get(i).getColumnName()))
				throw new Exception("Column[] and ColumnDefinition has not the same order index.");
		}
	}
	
	public Schema(Column[] colsIn, DatumType[] colTypesIn){
		if(colsIn.length==0||colTypesIn.length==0)
			throw new IllegalArgumentException("the number of columns/column definitions is 0.");
		if(colsIn.length!=colTypesIn.length)
			throw new IllegalArgumentException("Column[] size and DatumType[] size doesn't match : " + colsIn.length + "," + colTypesIn.length);
		
		length = colsIn.length;
		columns = colsIn;
		colTypes = colTypesIn;
		indexMap = new HashMap<String, Integer>(length);
		for(int i=0; i<length; i++){
			indexMap.put(columns[i].getColumnName(), i);
		}
	}
	
	
	public Table getTable(){
		return columns[0].getTable();
	}
	
	/**
	 * Get column name
	 * @param index
	 * @return
	 */
	public String getColName(int index){
		if(index>=columns.length)
			throw new IndexOutOfBoundsException();		
		return columns[index].getColumnName();
	}
	
	/**
	 * Get column type
	 * @param index
	 * @return
	 */
	public DatumType getColType(int index){
		if(index>=columns.length)
			throw new IndexOutOfBoundsException();		
		return colTypes[index];
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
		DatumType type = DatumType.Int;
		switch(typeStr){
		case DataType.INT:
			type = DatumType.Int;
			return type;
			
		case DataType.LONG:
			type = DatumType.Long;
			return type;
			
		case DataType.FLOAT:
			type = DatumType.Float;
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
	
	
	public Column getColumnByIndex(int index){
		if(index>=columns.length)
			throw new IndexOutOfBoundsException();		
		return columns[index];
	}
	
	
//	public ColumnDefinition getColDefinition(int index){
//		if(index>=colDefs.size())
//			throw new IndexOutOfBoundsException();
//		return colDefs.get(index);
//	}
	
	public Column[] getColumns(){		
		return columns;
	}
	
//	public List<ColumnDefinition> getColDefs(){
//		return colDefs;
//	}
	
	public int getIndex(String colName){
		Integer index = indexMap.get(colName);
		if(index==null)
			return -1;
		else
			return index.intValue();
	}
	
	public static void main(String[] args) {

		try {
			CCJSqlParser parser = new CCJSqlParser(new FileInputStream(new File("data/NBA/nba11.sql")));
			CreateTable ct = parser.CreateTable();
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
