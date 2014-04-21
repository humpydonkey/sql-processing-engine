package ra;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import common.Tools;

import dao.Tuple;

/**
 * Hash join operator implemented by half external,
 * one table resides in memory, one in disk  
 * @author Asia
 *
 */
public class OperatorHashJoin_HalfMem extends OperatorHashJoin{

	private String equalColName;
	private Operator joinResult;
	
	public OperatorHashJoin_HalfMem(String equalColIn, Operator smallInput, Operator largeInput, File joinF){	
		Tools.debug("[Half-Mem Join] " + smallInput.getSchema().getTableName()
				+ " " + smallInput.getLength() + " *" +equalColIn+"* "
				+ largeInput.getSchema().getTableName() + " "
				+ largeInput.getLength() + " Created!");
		
		equalColName = equalColIn;
		
		Operator hashSource = smallInput;
		Operator dataSource = largeInput;
		
		//construct hashMap
		Map<String, List<Tuple>> hashMap = new HashMap<String, List<Tuple>>();
		Tuple hashTup;
		while((hashTup=hashSource.readOneTuple())!=null){
			addTuple(equalColName, hashTup, hashMap);
		}	
		
		//do join
		Tuple data;
		try(BufferedWriter writer = new BufferedWriter(new FileWriter(joinF))){		
			while((data=dataSource.readOneTuple())!=null){
				joinAndWrite(equalColName, data, hashMap, writer);
			}
			writer.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		joinResult = new OperatorScan(joinF, getSchema());
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
