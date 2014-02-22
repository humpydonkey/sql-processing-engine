/**
 * 
 */
package ra;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Iterator;

import dao.Schema;
import dao.Tuple;

/**
 * @author Asia
 *
 */
public class ScanOperator implements Operator, Iterable<Tuple>, Iterator<Tuple> {

	protected BufferedReader inputReader;
	protected File file;
	protected Tuple tuple = null;
	protected Schema schema;
	
	public ScanOperator(File f, Schema schemaIn){
		file = f;
		schema = schemaIn;
		reset();
	}
	
	@Override
	public Tuple readOneTuple() {
		checkInput();
		try {
			String line = inputReader.readLine();
			if(line==null)
				tuple = null;			
			else
				tuple = new Tuple(line.split("\\|"), schema);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tuple;
	}

	@Override
	public void reset() {
		try {
			inputReader = new BufferedReader(new FileReader(file));
			tuple = null;
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

	@Override
	public boolean hasNext() {
		readOneTuple();
		return (tuple!=null);
	}

	@Override
	public Tuple next() {
		return tuple;
	}

	@Override
	public void remove() {
		try {
			throw new UnsupportedOperationException("Remove not supported on BufferedReader iteration.");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public Iterator<Tuple> iterator() {
		return this;
	}

	
	public static void main(String[] args){
		System.out.println("test");
		String text = "wgaweg|gaweg|bqeqwg|12|fqweg2|23fasdf|fs.w';eg";
		String[] sArr = text.split("\\|");
		for(String s : sArr)
			System.out.println(s);
	}
}
