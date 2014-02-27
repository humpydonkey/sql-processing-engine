package dao;

public class DatumBool extends Datum {

	private boolean value;
	
	public DatumBool(String data) {
		super(DatumType.Bool);
		value = Boolean.parseBoolean(data);
	}
	
	public DatumBool(boolean data) {
		super(DatumType.Bool);
		value = data;
	}
	
	public boolean getValue(){
		return value;
	}

	@Override
	public double getNumericValue() {
		System.out.println("Wrong get value.");
		return 0;
	}

	@Override
	public void setNumericValue(double valueIn) {
		System.out.println("Wrong set value.");
	}

	@Override
	public int compareTo(Datum o) {
		if(o instanceof DatumBool){
			DatumBool obj = (DatumBool)o;
			if(this.value==obj.value)
				return 0;
			else if(this.value==true){
				return 1;
			}else
				return -1;
		} else{
			try {
				throw new IllegalArgumentException("Wrong type (" + o.getClass().getCanonicalName() + ") of this Object.");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return -99;
		}
	}

	@Override
	public Datum clone() {
		Datum copy = new DatumBool(this.getValue());
		return copy;
	}
	
	public static void main(String[] args){
		DatumBool bool1 = new DatumBool(true);
		bool1.compareTo(null);
	}
}
