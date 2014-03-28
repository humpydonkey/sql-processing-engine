package edu.buffalo.cse562;

import io.FileAccessor;

import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import dao.Schema;
import dao.Tuple;

public class MainTest {

	private final static String TestResultPath = "test/test_result.txt";
	@Test
	public void testTestSpecificSQL_nba01() {
		List<Tuple> results = Main.testSpecificSQL("test/cp1_sqls/nba01.sql");
		
		if(results.size()>0){
			Schema schema = results.get(0).getSchema();
			List<Tuple> correctResults = FileAccessor.getInstance().readSpecificBlock(TestResultPath, schema, 2, 2129);
			
			for(int i=0; i<results.size(); i++){
//				System.out.println(correctResults.get(i)+"\n"+results.get(i)+"\n");		
				Assert.assertEquals(results.get(i).toString(), correctResults.get(i).toString());
			}

		}else
			Assert.fail();
	}
	
	@Test
	public void testTestSpecificSQL_nba02() {
		List<Tuple> results = Main.testSpecificSQL("test/cp1_sqls/nba02.sql");
		
		if(results.size()>0){
			Schema schema = results.get(0).getSchema();
			List<Tuple> correctResults = FileAccessor.getInstance().readSpecificBlock(TestResultPath, schema, 2136, 2199);
			
			for(int i=0; i<results.size(); i++){
//				System.out.println(correctResults.get(i)+"\n"+results.get(i)+"\n");		
				Assert.assertEquals(results.get(i).toString(), correctResults.get(i).toString());
			}

		}else
			Assert.fail();
	}
	
	@Test
	public void testTestSpecificSQL_nba03() {
		List<Tuple> results = Main.testSpecificSQL("test/cp1_sqls/nba03.sql");
		
		if(results.size()>0){
			Schema schema = results.get(0).getSchema();
			List<Tuple> correctResults = FileAccessor.getInstance().readSpecificBlock(TestResultPath, schema, 2206, 3442);
			
			for(int i=0; i<results.size(); i++){
//				System.out.println(correctResults.get(i)+"\n"+results.get(i)+"\n");		
				Assert.assertEquals(results.get(i).toString(), correctResults.get(i).toString());
			}

		}else
			Assert.fail();
	}
	
	@Test
	public void testTestSpecificSQL_nba04() {
		List<Tuple> results = Main.testSpecificSQL("test/cp1_sqls/nba04.sql");
		
		if(results.size()>0){
			Schema schema = results.get(0).getSchema();
			List<Tuple> correctResults = FileAccessor.getInstance().readSpecificBlock(TestResultPath, schema, 3449, 3470);
			
			for(int i=0; i<results.size(); i++){
//				System.out.println(correctResults.get(i)+"\n"+results.get(i)+"\n");		
				Assert.assertEquals(results.get(i).toString(), correctResults.get(i).toString());
			}

		}else
			Assert.fail();
	}
	@Test
	public void testTestSpecificSQL_tpch1() {
		List<Tuple> results = Main.testSpecificSQL("test/cp1_sqls/tpch1.sql");
		
		if(results.size()>0){
			Schema schema = results.get(0).getSchema();
			List<Tuple> correctResults = FileAccessor.getInstance().readSpecificBlock(TestResultPath, schema, 3478, 3481);
			
			for(int i=0; i<results.size(); i++){
				System.out.println(correctResults.get(i)+"\n"+results.get(i)+"\n");		
				Assert.assertEquals(results.get(i).toString(), correctResults.get(i).toString());
			}

		}else
			Assert.fail();
	}
	
	@Test
	public void testTestSpecificSQL_tpch3() {
		List<Tuple> results = Main.testSpecificSQL("test/cp1_sqls/tpch3.sql");
		
		if(results.size()>0){
			Schema schema = results.get(0).getSchema();
			List<Tuple> correctResults = FileAccessor.getInstance().readSpecificBlock(TestResultPath, schema, 3488, 3502);
			
			for(int i=0; i<results.size(); i++){
				System.out.println(correctResults.get(i)+"\n"+results.get(i)+"\n");		
				Assert.assertEquals(results.get(i).toString(), correctResults.get(i).toString());
			}

		}else
			Assert.fail();
		
	}

	@Test
	public void testTestSpecificSQL_tpch5() {
		List<Tuple> results = Main.testSpecificSQL("test/cp1_sqls/tpch5.sql");
		
		if(results.size()>0){
			Schema schema = results.get(0).getSchema();
			List<Tuple> correctResults = FileAccessor.getInstance().readSpecificBlock(TestResultPath, schema, 3509, 3533);
			
			for(int i=0; i<results.size(); i++){
				System.out.println(correctResults.get(i)+"\n"+results.get(i)+"\n");		
				Assert.assertEquals(results.get(i).toString(), correctResults.get(i).toString());
			}

		}else
			Assert.fail();
		
	}
	
	@Test
	public void testTestSpecificSQL_tpch6() {
		List<Tuple> results = Main.testSpecificSQL("test/cp1_sqls/tpch6.sql");
		
		if(results.size()>0){
			Schema schema = results.get(0).getSchema();
			List<Tuple> correctResults = FileAccessor.getInstance().readSpecificBlock(TestResultPath, schema, 3539, 3539);
			
			for(int i=0; i<results.size(); i++){
				System.out.println(correctResults.get(i)+"\n"+results.get(i)+"\n");		
				Assert.assertEquals(results.get(i).toString(), correctResults.get(i).toString());
			}

		}else
			Assert.fail();
		
	}
}
