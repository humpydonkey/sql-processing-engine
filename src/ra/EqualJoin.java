package ra;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class EqualJoin {
	private Column leftCol;
	private Column rightCol;
	private String colName;
	
	public EqualJoin(Column left, Column right){
		if(left.getColumnName().equalsIgnoreCase(right.getColumnName())&&!left.getTable().getName().equalsIgnoreCase(right.getTable().getName())){
			leftCol = left;
			rightCol = right;
			colName = left.getColumnName();
		}else{
			throw new IllegalArgumentException("Illegal argument: "+left.toString()+", "+right.toString());
		}
	}
	
	public String getColName(){
		return colName;
	}
	
	public Column getLeftColumn(){
		return leftCol;
	}
	
	public Column getRightColumn(){
		return rightCol;
	}
	
	public String getLeftTableName(){
		return leftCol.getTable().getName();
	}
	
	public String getRightTableName(){
		return rightCol.getTable().getName();
	}
}
