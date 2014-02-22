package ra;

import dao.Schema;
import dao.Tuple;

public class ProjectionOperator implements Operator{
	private Operator input;
	private Schema newSchema;
	
	public ProjectionOperator(Operator inputIn, Schema schemaIn){
		input = inputIn;
		newSchema = schemaIn;
	}

	
	@Override
	//project only variable in readOneTuple() method
	public Tuple readOneTuple() {
		Tuple tuple = input.readOneTuple();
		if(tuple==null)
			return null;
		
		if(tuple.changeTuple(newSchema))
			return tuple;
		else{
			try {
				throw new Exception("tuple.changeTuple() Error.");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}
	}

	
	@Override
	public void reset() {
		input.reset();
	}

}
