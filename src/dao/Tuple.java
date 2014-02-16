package dao;

public class Tuple {
	
	private Datum[] columns;
	
	/**
	 * Constructor
	 * @param columnsIn
	 */
	public Tuple(String[] columnsIn){
		columns = new Datum[columnsIn.length];
		for(int i=0; i<columnsIn.length; i++){
			columns[i] = new Datum(columnsIn[i]);
		}
	}
	
	
	public Datum[] getTuple(){
		return columns;
	}
	
	
	/**
	 * Get specific data block by column index
	 * @param index
	 * @return
	 */
	public Datum getData(int index){
		if(index>=columns.length){
			try {
				throw new Exception("Index out of range!");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}else{
			return columns[index];
		}		
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
