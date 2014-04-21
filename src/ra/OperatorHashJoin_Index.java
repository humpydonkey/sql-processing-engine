package ra;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import sqlparse.Config;
import net.sf.jsqlparser.schema.Table;
import dao.Datum;
import dao.HashIndex;
import dao.Schema;
import dao.Tuple;

/**
 * Use HashIndex(Random Access File) implemented
 * external hash join
 * @author Asia
 *
 */
public class OperatorHashJoin_Index extends OperatorHashJoin{
	
	private Operator input;
	private String equalColName;
	private HashIndex hashIndex;
	private Schema hashSourceSchema;
	private List<Tuple> bufferedResult;
	private RandomAccessFile indexFile;
	private File swapDir;
	//result hash index
	
	public OperatorHashJoin_Index(String equalColIn, HashIndex hashIndexIn, Operator inputIn, File swapDirIn) throws IOException{
		input = inputIn;
		hashIndex = hashIndexIn;
		equalColName = equalColIn;
		//10 is for resizing the ArrayList
		bufferedResult = new ArrayList<Tuple>(Config.Buffer_SIZE+10);	
		indexFile = new RandomAccessFile(hashIndex.getIndexFile(),"rw");
		hashSourceSchema = hashIndexIn.getSchema();
		swapDir = swapDirIn;
	}
	
	public HashIndex doJoin(String futureAttr){
		Table tab = new Table(null,equalColName+"_Join");
		HashIndex resultHashIndex = new HashIndex(tab, futureAttr, getSchema(), swapDir);

		while(readOneTuple()!=null){
			//while readOneTuple(), joined tuples added to bufferedResult
			if(bufferedResult.size()>=Config.Buffer_SIZE){
				resultHashIndex.insertBlock(new OperatorCache(bufferedResult), true);
			}
		}//finish join
		
		return resultHashIndex;
	}
	
	public void flushBuffer(){
				
		return;
	}
	
	@Override
	public Tuple readOneTuple() {
		Tuple inputTup = input.readOneTuple();
		if(inputTup==null)
			return null;
		
		Datum keyData = inputTup.getDataByName(equalColName);
		String key = keyData.toString();
		if(hashIndex.containsKey(key)){
			List<Long> matches = hashIndex.get(key);
			for(Long pointer : matches){			
				try {
					indexFile.seek(pointer.longValue());
					String readline =indexFile.readLine();
					Tuple matchTup = new Tuple(readline, hashSourceSchema);
					Tuple joined = joinTuple(inputTup, matchTup, equalColName);

					bufferedResult.add(joined);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		return inputTup;
	}


	
	@Override
	public void reset() {
		input.reset();
		try {
			indexFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public long getLength() {
		return input.getLength();
	}
	

}
