package ra;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import dao.Tuple;

public class ScanOperatorTest {
	ScanOperator scan;
	
	@Before
	public void testScanOperator() {
		String addr = "data/NBA/nba11.expected.dat";
		//scan = new ScanOperator(new File(addr));
	}

	@Test
	public void testIterable() {
		for(Tuple t : scan){
			t.printTuple();			
		}
	}
	
	@Test
	public void testReadOneTuple() {
		Tuple t = null;
		while((t=scan.readOneTuple())!=null){
			t.printTuple();
		}
	}
	
	@Test
	public void testReset() {
		int i=0;
		for(Tuple t : scan){
			t.printTuple();
			i++;
			if(i==2){
				scan.reset();
			}
			if(i==4)
				scan.reset();
			if(i==6)
				scan.reset();
			if(i==8){
				scan.reset();
			}
			if(i==10)
				scan.reset();
			if(i==12)
				scan.reset();
			if(i==15)
				break;
		}
	}
}
