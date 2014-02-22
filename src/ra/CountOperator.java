package ra;

import java.util.HashSet;
import java.util.Set;

import net.sf.jsqlparser.expression.Function;
import dao.Datum;
import dao.Tuple;

public class CountOperator implements Operator{

	private Operator input;
	private int count;
	private String colName;
	private boolean countAll;
	private boolean distinct;
	private Set<Datum> dataSet;
	
	public int getCount(){
		if(distinct)
			return dataSet.size();
		else
			return count;
	}
	
	
	public CountOperator(Operator inputIn, Function func){
		input = inputIn;
		count = 0;
		
		if(func.isAllColumns()){
			countAll = true;
			//do not need anything else
		}else{
			countAll = false;
			
			if(func.isDistinct()){
				distinct = true;
				dataSet = new HashSet<Datum>();
			}
			else
				distinct = false;	
		}
	}
	
	
	@Override
	public Tuple readOneTuple() {
		Tuple tuple = input.readOneTuple();
		if(countAll==true)
			count++;
		else{
			Datum data = tuple.getDataByName(colName);
			if(distinct){
				dataSet.add(data);
			}else{
				if(data!=null)
					count++;
			}			
		}
		return tuple;
	}

	@Override
	public void reset() {
		input.reset();
	}

}
