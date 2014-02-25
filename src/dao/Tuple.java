package dao;

import java.util.Date;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import ra.Aggregator;
import ra.Evaluator;


public class Tuple{
	
	private Datum[] dataArr;
	private Schema schema;
	
	/**
	 * Constructor
	 * @param splitedData
	 * @throws Exception 
	 */
	public Tuple(String[] splitedData, Schema schemaIn){
		dataArr = new Datum[splitedData.length];
		schema = schemaIn;
		
		for(int i=0; i<splitedData.length; i++){
			if(splitedData[i].equals(""))
				continue;
			DatumType type = schemaIn.getColType(i);
			dataArr[i] = DatumFactory.create(splitedData[i], type);
		}
	}
	
	
	public Tuple(String rawLine, Schema schemaIn){
		String[] splitedData = rawLine.split("\\|");
		dataArr = new Datum[splitedData.length];
		schema = schemaIn;
		
		for(int i=0; i<splitedData.length; i++){
			if(splitedData[i].equals(""))
				continue;
			DatumType type = schemaIn.getColType(i);
			dataArr[i] = DatumFactory.create(splitedData[i], type);
		}
	}
	
	
	public Datum[] getTuple(){
		return dataArr;
	}
	
	/**
	 * Change tuple by new schema
	 * @param newSchema
	 * @return
	 */
	public void changeTuple(Schema newSchema){
		int length = newSchema.getLength();
		Datum[] newDataArr = new Datum[length];
		Column[] newColNames = newSchema.getColumnNames();
		Expression[] newColSources = newSchema.getColumnSources();
		
		for(int i=0; i<length; i++){
			//Get data from old tuple
			Datum data = null;
			Expression newSource = newColSources[i];
			Column newName = newColNames[i]; 
			if(newSource instanceof Column){
				//is a column
				data = getDataByName(newName.getColumnName());
			}else if(newSource instanceof Function){ 
				//is an aggregate function
				Function func = (Function)newSource;
		
				//find the key
				Aggregator aggre = newSchema.getAggregator(func);
				String groupbyKey = "";
				for(String colName : aggre.getGroupByColumns()){
					Datum groupbyColumn = getDataByName(colName);
					groupbyKey += groupbyColumn.toString();
				}
				//map to the aggregated Datum value
				data = aggre.getValue(groupbyKey);
				
			}else{
				//should be a constant or expression
				Evaluator eval = new Evaluator(this);
				newSource.accept(eval);
				data = eval.copyDatum();
				if(data.getNumericValue()==1168){
					System.out.println("!!");
				}
				
			}
			newDataArr[i] = data;
		}
		dataArr = newDataArr;
		schema = newSchema;
	}
	
	/**
	 * Get specific data block by column index
	 * @param index
	 * @return
	 * @throws Exception 
	 */
	public Datum getData(int index) throws Exception{
		if(index>=dataArr.length){
			throw new Exception("Index out of range! index: " + index + ", Length: " + dataArr.length);
		}else{
			return dataArr[index];
		}		
	}
	
	/**
	 * Get data by column name
	 * @param colName
	 * @return
	 * @throws Exception
	 */
	public Datum getDataByName(String colName){
		int index = schema.getIndex(colName.toUpperCase());
		if(index<0)
			return null;
		else
			return dataArr[index];
	}
	
	/**
	 * Get Datum type by column name
	 * @param colName
	 * @return
	 */
	public DatumType getDataTypeByName(String colName){
		int index = schema.getIndex(colName);
		if(index<0)
			return null;
		else
			return schema.getColType(index);
	}
	
	
	/**
	 * Print tuple on screen
	 */
	public void printTuple(){
		int i=0;
		for(Datum data : dataArr){
			i++;
			if(i==dataArr.length)
				System.out.println(data.toString());
			else
				System.out.print(data.toString()+"|");
		}
	}
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		for(Datum data : dataArr)
			sb.append(data.toString()+'|');
		sb.deleteCharAt(sb.length()-1);
		return sb.toString();
	}
	
	public Schema getSchema(){
		return schema;
	}
	
//	public static void main(String[] args){
//		 Date d1 = new Date(1994,01,01);
//		 Date d2 = new Date(1993,12,12);
//		 System.out.println(d1.compareTo(d2));
//	}

}
