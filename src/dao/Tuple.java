package dao;


public class Tuple {
	
	private Datum[] columns;
	private Schema schema;
	
	/**
	 * Constructor
	 * @param dataIn
	 * @throws Exception 
	 */
	public Tuple(String[] dataIn, Schema schemaIn) throws Exception{
		columns = new Datum[dataIn.length];
		schema = schemaIn;
		
		for(int i=0; i<dataIn.length; i++){
			DatumType type = schemaIn.getColType(i);
			columns[i] = DatumFactory.create(dataIn[i], type);
		}
	}
	
	
	public Datum[] getTuple(){
		return columns;
	}
	
	
	/**
	 * Get specific data block by column index
	 * @param index
	 * @return
	 * @throws Exception 
	 */
	public Datum getData(int index) throws Exception{
		if(index>=columns.length){
			throw new Exception("Index out of range! index: " + index + ", Length: " + columns.length);
		}else{
			return columns[index];
		}		
	}
	
	/**
	 * Get data by column name
	 * @param colName
	 * @return
	 * @throws Exception
	 */
	public Datum getDataByName(String colName) throws Exception{
		int index = schema.getIndex(colName);
		if(index<0)
			throw new Exception("There is no such a column name : " + colName);
		else
			return columns[index];
	}
	
	
	/**
	 * Print tuple on screen
	 */
	public void printTuple(){
		int i=0;
		for(Datum data : columns){
			i++;
			if(i==columns.length)
				System.out.println(data.toString());
			else
				System.out.print(data.toString()+"|");
		}
	}
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		for(Datum data : columns)
			sb.append(data.toString()+'|');
		sb.deleteCharAt(sb.length()-1);
		return sb.toString();
	}
}
