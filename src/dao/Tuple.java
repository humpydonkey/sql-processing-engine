package dao;

import net.sf.jsqlparser.schema.Column;


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
		Column[] newCols = newSchema.getColumns();
		Datum[] newDataArr = new Datum[newCols.length];
		
		int i=0; 
		for(Column newCol : newCols){
			//Get data from old tuple
			Datum data = getDataByName(newCol.getColumnName());
			if(data==null){
				//not from the original column, 
				//may be a constant or aggregate function
				data = DatumFactory.create(newCol.getColumnName(), DatumType.String);
				if(data==null)
					return false;
			}
			newDataArr[i] = data;
			i++;
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
