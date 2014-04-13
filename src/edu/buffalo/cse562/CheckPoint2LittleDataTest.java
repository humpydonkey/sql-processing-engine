package edu.buffalo.cse562;

import io.FileAccessor;

import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import dao.Schema;
import dao.Tuple;

public class CheckPoint2LittleDataTest {

	private final static String TestFileDir = "test/cp2_littleBig";
	
	@Test
	public void testTestSpecificSQL_tpch07a() {
		String sqlPath = TestFileDir + "/" + "tpch07a.sql";
		String resultPath = TestFileDir + "/" + "tpch07a.expected.dat";
		
		List<Tuple> results = Main.testSpecificSQL(TestFileDir, sqlPath);
		if(results.size()>0){
			Schema schema = results.get(0).getSchema();	
			List<String> correctResults = FileAccessor.getInstance().readAllLines(resultPath);
			
			for(int i=0; i<results.size(); i++){
				System.out.println(correctResults.get(i)+"\n"+results.get(i)+"\n");	
				Assert.assertEquals(results.get(i).toString(), correctResults.get(i));
			}

		}else
			Assert.fail("0 result.");
	}
	
	
	@Test
	public void testTestSpecificSQL_tpch10a() {
		String sqlPath = TestFileDir + "/" + "tpch10a.sql";
		String resultPath = TestFileDir + "/" + "tpch10a.expected.dat";
		
		List<Tuple> results = Main.testSpecificSQL(TestFileDir, sqlPath);
		if(results.size()>0){

			List<String> correctResults = FileAccessor.getInstance().readAllLines(resultPath);
			
			for(int i=0; i<results.size(); i++){
				System.out.println(results.get(i));
				if(i<correctResults.size())
					System.out.println(correctResults.get(i)+"\n");
				
				//Assert.assertEquals(results.get(i), correctResults.get(i).toString());
			}

		}else
			Assert.fail("0 result.");
	}
	
	
	@Test
	public void testTestSpecificSQL_tpch12a() {
		String sqlPath = TestFileDir + "/" + "tpch12a.sql";
		String resultPath = TestFileDir + "/" + "tpch12a.expected.dat";
		
		List<Tuple> results = Main.testSpecificSQL(TestFileDir, sqlPath);
		if(results.size()>0){

			List<String> correctResults = FileAccessor.getInstance().readAllLines(resultPath);
			
			for(int i=0; i<results.size(); i++){
				System.out.println(results.get(i));
				if(i<correctResults.size())
					System.out.println(correctResults.get(i)+"\n");
				
				//Assert.assertEquals(results.get(i), correctResults.get(i).toString());
			}

		}else
			Assert.fail("0 result.");
	}
}
