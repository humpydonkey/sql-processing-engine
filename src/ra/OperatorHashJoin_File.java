package ra;

import io.BufferedRandomAccessFile;

import java.io.File;
import java.io.IOException;
import java.rmi.UnexpectedException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import dao.Datum;
import dao.Schema;
import dao.Tuple;

public class OperatorHashJoin_File extends OperatorHashJoin{
	private Operator input;
	private Map<String, LinkedList<Long>> joinMap;
	private String equalColName;
	private BufferedRandomAccessFile hashIndex;
	private Schema hashSourceSchema;
	
	public OperatorHashJoin_File(String equalColIn, Operator hashMapSource, Operator inputIn, File hashIndexFile) throws IOException{
		input = inputIn;
		joinMap = new HashMap<String, LinkedList<Long>>();
		equalColName = equalColIn;
		
		//Write data into file, construct a hash index file
		hashIndex = new BufferedRandomAccessFile(hashIndexFile,"rw");
		//hashIndex.seek(hashIndex.length());
		List<Tuple> tups = hashMapSource.readOneBlock();
		hashSourceSchema = tups.size()==0?null:tups.get(0).getSchema();
		
		while(tups.size()!=0){
			for(Tuple tup : tups){
				//datum of join column
				Datum keyData = tup.getDataByName(equalColName);
				if(keyData==null){
					try {
						throw new UnexpectedException("Can't get data from tuple : " + tup.toString());
					} catch (UnexpectedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				//construct index
				String key = keyData.toString();
				long pointer = hashIndex.getFilePointer();
				LinkedList<Long> list = joinMap.get(key);
				if(list==null){
					list = new LinkedList<Long>();
					list.add(pointer);
					joinMap.put(key, list);
				}else
					list.add(pointer);
				
				//write into file
				hashIndex.writeUTF(tup.toString()+"\n");
			}
			
			tups = hashMapSource.readOneBlock();
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
				List<Long> matches = joinMap.get(key);
				for(Long pointer : matches){
					try {
						hashIndex.seek(pointer);
						Tuple matchTup = new Tuple(hashIndex.readLine(), hashSourceSchema);
						Tuple joined = joinTuple(inputTup, matchTup, equalColName);
						results.add(joined);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
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
