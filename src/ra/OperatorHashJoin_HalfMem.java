package ra;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import common.Tools;

import sql2ra.Config;
import dao.Tuple;

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
		List<Tuple> buffer = new ArrayList<Tuple>(Config.Buffer_SIZE);
		Tuple data;
		try(BufferedWriter writer = new BufferedWriter(new FileWriter(joinF))){		
			while((data=dataSource.readOneTuple())!=null){
				joinAndBuffer(equalColName, data, hashMap, buffer);
				//write the join result in disk
				if(buffer.size()>=Config.Buffer_SIZE-10){
					flush(writer, buffer);
				}
			}
			flush(writer, buffer);
			writer.flush();
			buffer = null;
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
