package ra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import dao.Datum;
import dao.Tuple;

public class OperatorGroupBy implements Operator{

	private Operator input;
	private Map<String, Integer> groupMap;
	private List<Tuple> groupTuples;
	private List<Column> columns;	//group by columns
	private Aggregator[] aggregators;
	
	@SuppressWarnings("rawtypes")
	public OperatorGroupBy(Operator inputIn, List columnsIn, Aggregator... aggregatorsIn){
		input = inputIn;
		groupMap = new HashMap<String, Integer>();
		groupTuples = new LinkedList<Tuple>();
		columns = new ArrayList<Column>(columnsIn.size());
		
		if(columnsIn.get(0) instanceof Column){
			for(Object obj : columnsIn)
				columns.add((Column)obj);
		}else{
			//should be the object of ColumnIndex
			throw new UnsupportedOperationException("Not supported yet."); 
		}
		
		if(aggregatorsIn!=null){
			aggregators = aggregatorsIn;
		}
	}
	
	public List<Tuple> getTuples(){
		
		//while(readOneTuple()!=null){}
		while(readOneBlock().size()!=0){}
		
		List<Tuple> returnSet = groupTuples;
		groupTuples = new LinkedList<Tuple>();
		
		return returnSet;
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
	
	
	private String generateKey(Tuple tuple){
		StringBuffer sb = new StringBuffer();
		for(Column col : columns){
			Datum data = tuple.getDataByName(col.getColumnName());
			sb.append(data.toString());
		}
		return sb.toString();
	}
	
	
	public Tuple groupby(Tuple tuple){
		String key = generateKey(tuple);	
		if(!groupMap.containsKey(key)){		
			//if not contains, insert new value
			int index = groupTuples.size();
			groupMap.put(key, new Integer(index));
			groupTuples.add(tuple);

		}else{
			//update value
			int index = groupMap.get(key).intValue();
			groupTuples.set(index, tuple);
		}
		
		for(Aggregator aggr : aggregators){
			aggr.aggregate(tuple, key);
		}
		return tuple;
	}

}
