package ra;

import java.rmi.UnexpectedException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import sql2ra.SQLEngine;
import dao.Datum;
import dao.Tuple;

public class OperatorHashJoin_Mem extends OperatorHashJoin{

	private Operator input;
	private Map<String, LinkedList<Tuple>> joinMap;
	//private String hashedTableName;
	private String equalColName;
	
	public OperatorHashJoin_Mem(String equalColIn, Operator hashMapSource, Operator inputIn){
		input = inputIn;
		
		List<Tuple> tups = SQLEngine.dump(hashMapSource);
		
		joinMap = new HashMap<String, LinkedList<Tuple>>(tups.size());
		equalColName = equalColIn;
		//hashedTableName = equalColIn.getTable().getName();
		for(int i=0; i<tups.size(); i++){
			Tuple tuple = tups.get(i);
			Datum keyData = tuple.getDataByName(equalColName);
			if(keyData==null){
				try {
					throw new UnexpectedException("Can't get data from tuple : " + tuple.toString());
				} catch (UnexpectedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			String key = keyData.toString();
			LinkedList<Tuple> list = joinMap.get(key);
			if(list==null){
				list = new LinkedList<Tuple>();
				list.add(tuple);
				joinMap.put(key, list);
			}else
				list.add(tuple);
				joinMap.put(key, list);
		}
	}
	
	@Override
	public Tuple readOneTuple() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Tuple> readOneBlock() {
		List<Tuple> results = new LinkedList<Tuple>();
		List<Tuple> inputTups = input.readOneBlock();
		for(Tuple inputTup : inputTups){
			Datum keyData = inputTup.getDataByName(equalColName);
			String key = keyData.toString();
			if(joinMap.containsKey(key)){
				List<Tuple> matches = joinMap.get(key);
				for(Tuple matchTup : matches){
					Tuple joined = joinTuple(inputTup, matchTup, equalColName);
					results.add(joined);
				}
			}
		}
		return results;
	}


	@Override
	public void reset() {
		input.reset();
	}

}
