package ra;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Function;
import dao.Datum;
import dao.DatumInt;
import dao.Tuple;

public class AggregatorCount extends Aggregator{

	private Map<String, Datum> countMap;
	private String colName;
	private boolean countAll;
	private boolean distinct;
	private Map<String, Set<Datum>> distinctCountMap;
	
	
	public AggregatorCount(Function funcIn, String[] groupByCols){
		super(funcIn, groupByCols);
		
		countMap = new HashMap<String, Datum>();
		
		if(func.isAllColumns()){
			countAll = true;
			countMap.put("*", new DatumInt(0));
		}else{
			countAll = false;
			//assume Count() function only take one variable
			colName = func.getParameters().getExpressions().get(0).toString();
			if(func.isDistinct()){
				distinct = true;
				distinctCountMap = new HashMap<String, Set<Datum>>();
			}
			else
				distinct = false;	
		}
	}
	
	@Override
	public void aggregate(Tuple tuple, String key) {
		if(countAll)
			countPlusPlus("*");
		else{
			Datum data = tuple.getDataByName(colName);
			if(data!=null){	//only count non-null value
				if(distinct){
					if(!distinctCountMap.containsKey(key)){
						//insert new
						Set<Datum> dataSet = new HashSet<Datum>();
						dataSet.add(data);
						distinctCountMap.put(key, new HashSet<Datum>());
					}else{
						//update old
						Set<Datum> dataSet = distinctCountMap.get(key);
						dataSet.add(data);
					}
				}else{	//no distinct
					if(!countMap.containsKey(key)){
						//insert new
						countMap.put(key, new DatumInt(1));
					}else{
						//else count ++
						countPlusPlus(key);
					}
				}
			}
		}
	}


	@Override
	public Datum getValue(String key) {
		if(distinct){
			distinctCountMap.get(key).size();	
		}
		
		return countMap.get(key);
	}
	
	private void countPlusPlus(String key){
		Datum data = countMap.get(key);
		data.setNumericValue(data.getNumericValue()+1);
	}

	@Override
	public Aggregator clone() {
		return new AggregatorCount(func, groupbyNames);
	}

	
}
