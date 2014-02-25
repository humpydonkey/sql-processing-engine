package io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import common.TimeCalc;

import dao.Schema;
import dao.Tuple;

public class FileAccessor {

	private static FileAccessor instance = new FileAccessor();
	private final int BUFFERSIZE = 200000000;	//200MB
	
	private FileAccessor(){} 
	
	public static FileAccessor getInstance(){
		return instance;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String add0 = "data/NBA/";
		String add1 = "data/NBA/nba11.sql";
		String add2 = "data/NBA/nba16.expected.dat";
		String add3 = "data/tpch/partsupp.tbl";
		System.out.println("start");
		
		TimeCalc.begin(1);
		//String content = FileAccessor.getInstance().readLine(add2);
		//StringBuilder content = FileAccessor.getInstance().readBlock(add2);
		List<File> test =FileAccessor.getInstance().getDataFiles(add0, "dat");
		List<String> sqls = FileAccessor.getInstance().readAllSqls(add1);
		
		StringBuilder sb = FileAccessor.getInstance().readPartOfFile(new File(add3), 7);
		try {
			FileAccessor.getInstance().writeFile(sb,"partsupp.dat");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//System.out.println(content);
		for(String sql : sqls){
			System.out.println(sql);
		}
		
		TimeCalc.end(1);
		System.out.println("end");
	}
	
	
	/**
	 * Get a file list with a specified type of file
	 * @param addr : file path
	 * @param type : file type
	 * @return
	 */
	public List<File> getDataFiles(String addr, String type){
		File file = new File(addr);
		File[] files = file.listFiles();
		
		List<File> filelist = new ArrayList<File>();
		for(File f : files){
			if(f.getName().indexOf(type)>0){
				filelist.add(f);
			}
		}
		return filelist;
	}
	
	
	/**
	 * Read all content from a file
	 * @param addr : file path
	 * @return : String content
	 */
	public List<Tuple> readBlock(String addr, Schema schema){
		List<Tuple> tuples = new LinkedList<Tuple>();
		try {
			BufferedReader reader = getBR(addr);
			String line = null;
			while((line = reader.readLine())!=null){
				String [] data = line.split("\\|");
				tuples.add(new Tuple(data, schema));
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tuples;
	}
	
	public StringBuilder readBlock(File f){
		StringBuilder sb = new StringBuilder();
		char[] content = new char[BUFFERSIZE];
		try {
			BufferedReader reader = getBR(f);
			reader.read(content);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		sb.append(content);
		return sb;
	}
	
	
	public StringBuilder readPartOfFile(File f, int parts){
		StringBuilder sb = new StringBuilder();
		try {
			BufferedReader reader = getBR(f);
			String line = "";
			int i=0;
			while((line = reader.readLine())!=null){
				if(i==parts){
					sb.append(line+"\n");
					i=0;
				}
					
				i++;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return sb;
	}
	
	public void writeFile(StringBuilder sb, String fileName) throws IOException{
		FileWriter writer = new FileWriter(new File("data/tpch/" + fileName));
		writer.write(sb.toString());
		writer.close();
	}
	
	/**
	 * Read all the SQLs from a file
	 * @param addr : file path
	 * @return
	 */
	public List<String> readAllSqls(String addr){
		List<String> sqls = new ArrayList<String>();
		try {
			BufferedReader br = getBR(addr);
			String line = "";
			while((line = br.readLine())!=null){
				sqls.add(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return sqls;
	}
	
	
	/**
	 * Get BufferedReader
	 * @param addr : file path
	 * @return
	 * @throws FileNotFoundException
	 */
	private BufferedReader getBR(String addr){
		try {
			return new BufferedReader(new InputStreamReader(new FileInputStream(new File(addr))));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	private BufferedReader getBR(File f){
		try {
			return new BufferedReader(new InputStreamReader(new FileInputStream(f)));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
}
