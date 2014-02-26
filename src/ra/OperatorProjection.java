package ra;

import java.util.List;

import dao.Schema;
import dao.Tuple;

public class OperatorProjection implements Operator{
	private Operator input;
	private Schema newSchema;
	
	public OperatorProjection(Operator inputIn, Schema schemaIn){
		input = inputIn;
		newSchema = schemaIn;
	}


	@Override
	public List<Tuple> readOneBlock() {
		List<Tuple> tuples = input.readOneBlock();
		for(Tuple tuple : tuples)
			tuple.changeTuple(newSchema);
		
		return tuples;
	}
	
	
	@Override
	//project only variable in readOneTuple() method
	public Tuple readOneTuple() {
		Tuple tuple = input.readOneTuple();
		if(tuple==null)
			return null;
		tuple.changeTuple(newSchema);
		return tuple;
	}

	
	@Override
	public void reset() {
		input.reset();
	}
	
	

}
