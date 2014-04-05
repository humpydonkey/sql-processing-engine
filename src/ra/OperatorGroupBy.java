package ra;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.jsqlparser.schema.Column;
import dao.Datum;
import dao.Tuple;

public class OperatorGroupBy implements Operator{

	private Operator input;
	private Aggregator[] aggregators;
	private List<Column> groupbyCols;	//group by columns
	private boolean noGroupBy;
	public final static String NOGROUPBYKEY = "!@#$%^&*Key"; 
	
	//store the group by column values as key, 
	//and the tuple of group as value. 
	private Map<String, Tuple> groupMap;	
	
	@SuppressWarnings("rawtypes")
	public OperatorGroupBy(Operator inputIn, List columnsIn, Aggregator... aggregatorsIn){
		input = inputIn;
		groupMap = new LinkedHashMap<String, Tuple>();
		aggregators = aggregatorsIn;	//could be null
		
		if(columnsIn==null||columnsIn.size()==0)
			noGroupBy = true;
		else{
			groupbyCols = new ArrayList<Column>(columnsIn.size());
			noGroupBy = false;
						
			if(columnsIn.get(0) instanceof Column){
				for(Object obj : columnsIn)		//convert to List<Column>
					groupbyCols.add((Column)obj);	
			}else{
				//should be the object of ColumnIndex
				throw new UnsupportedOperationException("Not supported yet."); 
			}
		}		
	}
	
	
	public List<Tuple> dump(){
		while(readOneBlock().size()!=0){}
		
		List<Tuple> groupedTuples = new LinkedList<Tuple>();
		for(Entry<String, Tuple> entry : groupMap.entrySet()){
			groupedTuples.add(entry.getValue());
		}
		
		return groupedTuples;
	}
	
	
	@Override
	public List<Tuple> readOneBlock() {
		List<Tuple> tuples = input.readOneBlock();
		for(Tuple tuple : tuples){
			groupby(tuple);
		}
		return tuples;
	}

	
	@Override
	public Tuple readOneTuple() {
		Tuple tuple = input.readOneTuple();
		
		if(tuple==null)
			return null;		

		return groupby(tuple);
	}

	
	@Override
	public void reset() {
		input.reset();
	}
	
	
	//The key is the combined values of group by columns
	private String generateKey(Tuple tuple){
		StringBuffer sb = new StringBuffer();
		for(Column col : groupbyCols){
			Datum data = tuple.getDataByName(col);
			sb.append(data.toString());
		}
		return sb.toString();
	}
	
	
	public Tuple groupby(Tuple tuple){
		String key;
		if(noGroupBy)
			key = NOGROUPBYKEY;
		else{
			key = generateKey(tuple);
		}
		groupMap.put(key, tuple);
		
		for(Aggregator aggr : aggregators){
			aggr.aggregate(tuple, key);
		}
		
		return tuple;
	}

}
