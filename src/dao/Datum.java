package dao;



public abstract class Datum implements Comparable<Datum> {
	
	private DatumType type;
	
	public Datum(DatumType typeIn){
		type = typeIn;
	}
	
	public DatumType getType(){
		return type;
	}
	
	@Override
	public String toString(){
		switch(type){
			case String:
				DatumString valStr = (DatumString)this;
				return valStr.getValue();
			case Date:
				DatumDate valDate = (DatumDate)this;
				return valDate.toString();
			case Bool:
				DatumBool valBool = (DatumBool)this;
				return String.valueOf(valBool.getValue());
			case Float:
				DatumFloat valFloat = (DatumFloat)this;
				return valFloat.toString();
			default:	//Int or Long
				return String.valueOf((long)this.getNumericValue());
		}
	}
	
	public abstract double getNumericValue();
	public abstract void setNumericValue(double valueIn);
	
	
	/**
	 * compare whether two Datum objects are equal (have the same value).
	 * @param data1
	 * @param data2
	 * @return
	 * @throws Exception
	 */
	public static boolean equals(Datum data1, Datum data2) throws Exception{
		if(data1 instanceof DatumBool && data2 instanceof DatumBool){
			DatumBool bool1 = (DatumBool)data1;
			DatumBool bool2 = (DatumBool)data2;
			if(bool1.getValue()==bool2.getValue())
				return true;
			else
				return false;
		}else if(data1 instanceof DatumString && data2 instanceof DatumString){
			DatumString str1 = (DatumString)data1;
			DatumString str2 = (DatumString)data2;
			if(str1.getValue().equalsIgnoreCase(str2.getValue()))
				return true;
			else
				return false;
		}else if(data1 instanceof DatumDate && data2 instanceof DatumDate){
			DatumDate date1 = (DatumDate)data1;
			DatumDate date2 = (DatumDate)data2;
			if(compare(date1,date2)==0)	return true;
			else return false;	
		}else if(data1 instanceof DatumInt||data1 instanceof DatumLong||data1 instanceof DatumFloat){
			if(data2 instanceof DatumInt||data1 instanceof DatumLong||data1 instanceof DatumFloat){
				if(compare(data1,data2)==0)	return true;
				else return false;	
			}else
				return false;
		}else
			return false;
	}
	
	/**
	 * Compare two Datum objects, larger return 1, equal return 0, less return -1
	 * Cannot compare DatumString, DatumBool
	 * @param data1
	 * @param data2
	 * @return
	 * @throws Exception
	 */
	public static int compare(Datum data1, Datum data2) throws Exception{
		if(data1 instanceof DatumString||data2 instanceof DatumString)
			throw new Exception("can not compare string, it might be variables.");
		if(data1 instanceof DatumBool||data2 instanceof DatumBool)
			throw new Exception("can not compare boolean value.");
			
		if(data1 instanceof DatumDate){
			if(data2 instanceof DatumDate){
				DatumDate val1 = (DatumDate)data1;
				DatumDate val2 = (DatumDate)data2;
				
				if(val1.getValue().compareTo(val2.getValue())>0)
					return 1;	//Date1 is after the Date2 
				else if(val1.getValue().compareTo(val2.getValue())<0)
					return -1;
				else
					return 0;
			}else
				throw new Exception("can not compare two different type.");
		}else{
			double val1 = data1.getNumericValue();
			double val2 = data2.getNumericValue();
			if(val1>val2) return 1;
			else if(val1<val2) return -1;
			else return 0;
		}
	}

	
	public static void main(String[] args){
		Datum d1 = new DatumString("ABc");
		Datum d2 = new DatumString("abc");
		Datum d3 = new DatumDate("1989-09-29");
		Datum d4 = new DatumBool(false);
		Datum d5 = new DatumFloat(0.23f);
		Datum d6 = new DatumLong(112112);
		Datum d7 = new DatumInt(12);
		try {
			System.out.println(Datum.equals(d2, d1) + "\n");
			System.out.println(d1.toString());
			System.out.println(d2.toString());
			System.out.println(d3.toString());
			System.out.println(d4.toString());
			System.out.println(d5.toString());
			System.out.println(d6.toString());
			System.out.println(d7.toString());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
