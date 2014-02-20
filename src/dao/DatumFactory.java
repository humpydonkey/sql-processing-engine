package dao;

public class DatumFactory {
	public static Datum create(String dataIn, DatumType type) throws Exception{
		switch(type){
		case Long:
			return new DatumLong(dataIn);
		case Int:
			return new DatumInt(dataIn);
		case Float:
			return new DatumFloat(dataIn);
		case Bool:
			return new DatumBool(dataIn);
		case String:
			return new DatumString(dataIn);
		default:
			throw new Exception("unknown datum type : " + type);
		}
	}
}
