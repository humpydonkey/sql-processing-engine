package dao;

public class DataType {
	public final static String INT = "int";
	public final static String LONG = "long";
	public final static String FLOAT = "float";
	public final static String DATE = "date";
	public final static String BOOL = "bool";
	public final static String STRING = "string";
	
	public static String recognizeType(String typeIn){
		if(typeIn.toUpperCase().contains("CHAR")){
			return DataType.STRING;
		}else if(typeIn.toUpperCase().contains("DECIMAL")){
			return DataType.FLOAT;
		}else if(typeIn.toUpperCase().contains("INT")){
			return DataType.INT;
		}else if(typeIn.toUpperCase().contains("LONG")){
			return DataType.LONG;
		}else if(typeIn.toUpperCase().contains("DATE")){
			return DataType.DATE;
		}else if(typeIn.toUpperCase().contains("FLOAT")){
			return DataType.FLOAT;
		}else if(typeIn.toUpperCase().contains("DOUBLE")){
			return DataType.FLOAT;
		}else if(typeIn.toUpperCase().contains("STRING")){
			return DataType.STRING;
		}else if(typeIn.toUpperCase().contains("BOOL")){
			return DataType.BOOL;
		}
		else{
			throw new UnsupportedOperationException("Not supported yet. " + typeIn.toUpperCase()); 
		}
	}
}
