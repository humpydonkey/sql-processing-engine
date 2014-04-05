package sql2ra;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

public class Test {

	public static void main(String[] args) {
		FileReader stream;
		try {
			Map<String, List<String>> test = new HashMap<String, List<String>>();
			test.put("1", new ArrayList<String>());
			List<String> val = test.get("1");
			val.add("original1");
			val.add("original2");
			
			val = new ArrayList<String>();
			val.add("changed");
			test.put("1", val);
			
			for(String str : test.get("1")){
				System.out.println(str);
			}
			
			stream = new FileReader(new File("test/cp1_sqls/test_join.sql"));

			CCJSqlParser parser = new CCJSqlParser(stream);
			Statement stmt;
			while ((stmt = parser.Statement()) != null) {
				if (stmt instanceof Select) {
					System.out.println("I would now evaluate:\n" + stmt);
					Select sel = (Select) stmt;
					SelectBody selBody = sel.getSelectBody();
					if(selBody instanceof PlainSelect){
						PlainSelect psel = (PlainSelect)selBody;
						System.out.println(psel.getWhere());
						FromItem from = psel.getFromItem();
						System.out.println("From: "+from.toString());
						@SuppressWarnings("unchecked")
						List<Join> joins = psel.getJoins();
						for(Join j : joins){
							System.out.println("Join: "+j.toString()+","+j.getRightItem());
						}
					}
				}
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
