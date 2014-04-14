package ra;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import sql2ra.Config;

import common.Tools;

import dao.Tuple;

public class OperatorHashJoin_Block extends OperatorHashJoin{

	private String attribute;
	private Operator dataInput;
	private Operator hashInput;
	
	//could be a OperatorScan or OperatorCache
	private Operator joinResult;
	
	public OperatorHashJoin_Block(String attrIn, Operator hashSource, Operator dataSource, File resultFile){
		Tools.debug("[Block Join] " + hashSource.getSchema().getTableName()
				+ " " + hashSource.getLength() + " *" +attrIn+"* "
				+ dataSource.getSchema().getTableName() + " "
				+ dataSource.getLength() + " Created!");
		
		attribute = attrIn;
		dataInput = dataSource;	
		hashInput = hashSource;

		try(BufferedWriter resultWriter = new BufferedWriter(new FileWriter(resultFile))){	

			doJoin(resultWriter, attribute, hashInput, dataInput);
			resultWriter.flush();
			
			joinResult = new OperatorScan(resultFile, getSchema());
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void doJoin(BufferedWriter writer, String attr, Operator hashSource, Operator dataSource) throws IOException{
		//construct hashMap
		Map<String, List<Tuple>> hashMap = new HashMap<String, List<Tuple>>(Config.Buffer_SIZE);

		int count = 0;
		Tuple hashTup;
		Tuple dataTup;
		//read into block
		while((hashTup=hashSource.readOneTuple())!=null){
			addTuple(attr, hashTup, hashMap);
			count++;
			
			//do block join
			if(count>=Config.Buffer_SIZE){
				count=0;		
				while((dataTup=dataSource.readOneTuple())!=null){
					joinAndWrite(attr, dataTup, hashMap, writer);
				}
				dataSource.reset();
				hashMap.clear();
			}//block join end	
		}//end while
		
		//do block join for the last part for hashInput
		while((dataTup=dataSource.readOneTuple())!=null){
			joinAndWrite(attr, dataTup, hashMap, writer);		
		}//block join end	
		hashMap = null;
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
