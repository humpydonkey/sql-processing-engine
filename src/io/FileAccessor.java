package io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.CharBuffer;
import java.util.Timer;

import common.TimeCalc;

public class FileAccessor {

	private static FileAccessor instance = new FileAccessor();
	
	private FileAccessor(){} 
	
	public static FileAccessor getInstance(){
		return instance;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String add1 = "data/NBA/nba11.sql";
		String add2 = "data/NBA/nba16.expected.dat";
		System.out.println("start");
		TimeCalc.begin(1);
		//String content = FileAccessor.getInstance().readLine(add2);
		StringBuilder content = FileAccessor.getInstance().readAll(add2);

		System.out.println(content);
		
		TimeCalc.end(1);
		System.out.println("end");
	}
	
	
	public StringBuilder readAll(String addr){
		StringBuilder sb = new StringBuilder();
		char[] content = new char[100000];
		try {
			BufferedReader reader = getBR(addr);
			reader.read(content);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		sb.append(content);
		return sb;
	}
	
	public String readLine(String addr){
		BufferedReader reader;
		try {
			reader = getBR(addr);
			String line = reader.readLine();
			reader.close();
			return line;
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return "";
	}
	
	private BufferedReader getBR(String addr) throws FileNotFoundException{
		return new BufferedReader(new InputStreamReader(new FileInputStream(new File(addr))));
	}
	
}
