/**
 * 
 */
package ra;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import dao.Tuple;

/**
 * @author Asia
 *
 */
public class ScanOperator implements Operator, Iterator<Tuple> {

	protected BufferedReader inputReader;
	protected File file;
	protected String line = null;
	
	public ScanOperator(File f){
		file = f;
		reset();
	}
	
	@Override
	public Tuple readOneTuple() {
		checkInput();
		try {
			line = inputReader.readLine();
			if(line==null) 
				return null;
			else{
				return new Tuple(line.split("|"));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void reset() {
		try {
			inputReader = new BufferedReader(new FileReader(file));
			readOneTuple();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			inputReader = null;
		}
	}
	
	public boolean checkInput(){
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
		return (line!=null);
	}

	@Override
	public Tuple next() {
		return new Tuple(line.split("|"));
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

}
