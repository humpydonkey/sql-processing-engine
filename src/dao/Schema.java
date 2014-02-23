package dao;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

public class Schema {

	private Map<String, Integer> indexMap;
	private int length;
	private Column[] columnNames;
	private Expression[] columnSources;
	private DatumType[] colTypes;
	private Map<Function, Aggregator> aggregatorMap;
	
	public Schema(Column[] colsIn, List<ColumnDefinition> colDefsIn) throws Exception{
		if(colsIn.length==0||colDefsIn.size()==0)
			throw new IllegalArgumentException("the number of columns/column definitions is 0.");
		if(colsIn.length!=colDefsIn.size())
			throw new IllegalArgumentException("Column[] size and DatumType[] size doesn't match : " + colsIn.length + "," + colDefsIn.size());
		
		length = colsIn.length;
		columnNames = colsIn;
		colTypes = new DatumType[length];
		columnSources = new Expression[length];
		indexMap = new HashMap<String, Integer>(length);
		
		for(int i=0; i<length; i++){
			indexMap.put(columnNames[i].getColumnName(), i);
			colTypes[i] = convertColType(i, colDefsIn.get(i));
			columnSources[i] = colsIn[i];	//assign it as a column
			//compare the Column[] and ColumnDefinition has the same order index, if not throw exception 
			if(!columnNames[i].getColumnName().equals(colDefsIn.get(i).getColumnName()))
				throw new Exception("Column[] and ColumnDefinition has not the same order index.");
		}
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
		indexMap = new HashMap<String, Integer>(length);
		for(int i=0; i<length; i++){
			indexMap.put(columnNames[i].getColumnName(), i);
		}
		
		if(aggreMapIn!=null)
			aggregatorMap = aggreMapIn;
	}
	
	
	public Table getTable(){
		return columnNames[0].getTable();
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
