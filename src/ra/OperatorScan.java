/**
 * 
 */
package ra;

import io.BufferedRandomAccessFile;
import io.FileAccessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import common.TimeCalc;

import dao.Schema;
import dao.Tuple;

/**
 * @author Asia
 *
 */
public class OperatorScan implements Operator {

	private BufferedReader inputReader;
	private File file;
	private Schema schema;
	private final static int BLOCKSIZE =  100000;  // 100000000;	//100MB
	
	public OperatorScan(File f, Schema schemaIn){
		file = f;
		schema = schemaIn;
		reset();
	}
	
	@Override
	public List<Tuple> readOneBlock() {
		List<Tuple> tuples = new LinkedList<Tuple>();

		try {
			int i=0;
			String line;
			while(i<=BLOCKSIZE&&(line=inputReader.readLine())!=null){
				tuples.add(new Tuple(line, schema));
				i++;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return tuples;	
	}
	
	@Override
	public Tuple readOneTuple() {
		checkInput();
		Tuple tuple = null;
		try {
			String line = inputReader.readLine();
			if(line==null)
				tuple = null;			
			else
				tuple = new Tuple(line, schema);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tuple;
	}
	

	@Override
	public void reset() {
		try {
			inputReader = new BufferedReader(new FileReader(file)); //new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			inputReader = null;
		}
	}
	
	/**
	 * Check inputReader whether is null
	 * @return
	 */
	private boolean checkInput(){
		if(inputReader==null){
			try {
				throw new Exception("inputReader is null!");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return false;
		}else
			return true;
	}

	
	public static void main(String[] args){
		try {
			BufferedRandomAccessFile braf = new BufferedRandomAccessFile(new File("test/cache.dat"),"rw");
			for(int i=0; i<5; i++)
				System.out.println(braf.readLine());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
				
		
		StringReader stream = new StringReader("CREATE TABLE ORDERS (orderkey INT,custkey INT,orderstatus CHAR(1),totalprice DECIMAL,orderdate  DATE,orderpriority CHAR(15),clerk CHAR(15),shippriority INT,comment VARCHAR(79));");
		CCJSqlParser parser = new CCJSqlParser(stream);
		File dataFile = new File("test/data/orders.dat");
		try {
			CreateTable ct = parser.CreateTable();
			OperatorScan scan = new OperatorScan(dataFile, new Schema(ct, null));
			List<Tuple> tuples;
			int size = 0;
			TimeCalc.begin(0);
			do{
				tuples = scan.readOneBlock();
				FileAccessor.getInstance().writeTuples(tuples, new File("test/cache.dat"));
				size = size+tuples.size();
			}while(tuples.size()!=0);
			System.out.println("ReadOneBlock: "+size);
			TimeCalc.end(0);
			
			
			scan = new OperatorScan(dataFile, new Schema(ct, null));
			TimeCalc.begin(1);
			tuples = new LinkedList<Tuple>();
			Tuple t;
			while((t = scan.readOneTuple())!=null)
				tuples.add(t);
			System.out.println("ReadOneTuple: "+tuples.size());
			TimeCalc.end(1);
			System.out.println("End");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
