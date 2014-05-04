package dao;

/**
 * Datum factory, 
 * simple factory that creates Datum by type
 * @author Asia
 *
 */
public class DatumFactory {
	public static Datum create(String dataIn, DatumType type){
		switch(type){
		case Long:
			return new DatumLong(Long.parseLong(dataIn));
		case Double:
			return new DatumDouble(Double.parseDouble(dataIn));
		case Bool:
			return new DatumBool(Boolean.parseBoolean(dataIn));
		case String:
			return new DatumString(dataIn);
		case Date:
			String[] s = dataIn.split("-");
			int y = Integer.parseInt(s[0]);
			int m = Integer.parseInt(s[1]);
			int d = Integer.parseInt(s[2]);
			return new DatumDate(y,m,d);
		default:
			try {
				throw new Exception("unknown datum type : " + type);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}
	}
}
