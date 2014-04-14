package ra;

import java.io.File;
import java.util.List;

import dao.Schema;
import dao.Tuple;

public class OperatorExternalMergeSort implements Operator {

	public OperatorExternalMergeSort(List<File> groupFiles){
		
	}
	
	@Override
	public Tuple readOneTuple() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	@Override
	public long getLength() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Schema getSchema() {
		// TODO Auto-generated method stub
		return null;
	}

}
