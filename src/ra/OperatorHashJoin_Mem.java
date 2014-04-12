package ra;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import common.TimeCalc;
import common.Tools;

import dao.Tuple;

public class OperatorHashJoin_Mem extends OperatorHashJoin{

	private String equalColName;
	private Operator joinResult;
	
	public OperatorHashJoin_Mem(String equalColIn,  Operator smallInput, Operator largeInput){
		Tools.debug("[Mem Join] " + smallInput.getSchema().getTableName() + " "
				+ smallInput.getLength() + " *" +equalColIn+"* "
				+ largeInput.getSchema().getTableName() + " "
				+ largeInput.getLength() + " Created!");
		
		equalColName = equalColIn;
		//keep the order of input argument as the same in joinTuples() method
		Operator hashSource = smallInput;
		Operator dataSource = largeInput;
		
		//construct hashMap
		TimeCalc.begin(7);
		Map<String, List<Tuple>> hashMap = new HashMap<String, List<Tuple>>();
		Tuple hashTup;
		while((hashTup = hashSource.readOneTuple())!=null){
			addTuple(equalColName, hashTup, hashMap);
		}
		TimeCalc.end(7,"construct hash map");
		
		//do join
		List<Tuple> buffer = new LinkedList<Tuple>();
		Tuple data;
		//store all in buffer
		TimeCalc.begin(7);
		while((data=dataSource.readOneTuple())!=null){
			joinAndBuffer(equalColName, data, hashMap, buffer);
		}
		
		joinResult = new OperatorCache(buffer);
		TimeCalc.end(7, "join and buffer all");
	}
	

	@Override
	public Tuple readOneTuple() {
		return joinResult.readOneTuple();
	}


	@Override
	public void reset() {
		joinResult.reset();
	}

	@Override
	public long getLength() {
		return joinResult.getLength();
	}

}
