package dao;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import ra.Aggregator;
import ra.Evaluator;


public class Tuple {
	
	private Datum[] dataArr;
	private Schema schema;
	
	/**
	 * Constructor
	 * @param dataIn
	 * @throws Exception 
	 */
	public Tuple(String[] dataIn, Schema schemaIn){
		dataArr = new Datum[dataIn.length];
		schema = schemaIn;
		
		for(int i=0; i<dataIn.length; i++){
			DatumType type = schemaIn.getColType(i);
			dataArr[i] = DatumFactory.create(dataIn[i], type);
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
	public boolean changeTuple(Schema newSchema){
		 int length = newSchema.getLength();
		Datum[] newDataArr = new Datum[length];
		Column[] newColNames = newSchema.getColumnNames();
		Expression[] newColSources = newSchema.getColumnSources();
		
		for(int i=0; i<length; i++){
			//Get data from old tuple
			Datum data = null;
			Expression newSource = newColSources[i];
			Column newName = newColNames[i]; 
			if(newSource instanceof Column)
				data = getDataByName(newName.getColumnName());
			else if(newSource instanceof Function){ 
				//is an aggregate function
				Function func = (Function)newSource;
				if(func.isAllColumns()){
					data = newSchema.getAggregator(func).getValue("*");
				}else{
					//find the key
					Aggregator aggre = newSchema.getAggregator(func);
					String groupbyKey = "";
					for(String colName : aggre.getGroupByColumns()){
						Datum groupbyColumn = getDataByName(colName);
						groupbyKey += groupbyColumn.toString();
					}
					//map to the aggregated Datum value
					data = aggre.getValue(groupbyKey);
				}
			}else{
				// TODO should parse the expression
				//may be a constant or expression
				data = DatumFactory.create(newColNames[i].getColumnName(), DatumType.String);
			}
			newDataArr[i] = data;
		}
		dataArr = newDataArr;
		schema = newSchema;
		return true;
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
		int index = schema.getIndex(colName);
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
}
