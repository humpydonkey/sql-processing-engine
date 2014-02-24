package ra;

import java.io.*;
import java.util.*;

import ra.Operator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.OrderByElement;
import dao.*;

public class OperatorOrderBy{

	private List<Tuple> list;
	private List attr;
	private OrderByElement orderby;
	private Set<Datum> allData;
	private HashMap<Datum, List<Tuple>> orders;
	/*Operator input;
	Column[] schema;
	Expression condition;*/
	
	public static void main(String[] args){
		
		try {
			CCJSqlParser parser = new CCJSqlParser(new FileInputStream(new File("data/NBA/nba11.sql")));
			CreateTable ct = parser.CreateTable();
			List<ColumnDefinition> list = ct.getColumnDefinitions();
			for(ColumnDefinition cd : list){
				System.out.println(cd.getColumnName() + " : " + cd.getColDataType().getDataType());
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public boolean isAsc(){
		String attrAll = attr.toString();
		if(attrAll.contains("ASC") == true){
			return true;
		}
		else return false;
	}
	
	public OperatorOrderBy(List<Tuple> listIn,List attrIn) throws Exception{
		list = listIn;
		attr = attrIn;
		
		//todo find column
		//todo find all the tuples that have same attr value
		for(int i = 0;i<list.size();i++){
			String colName = attr.toString();
			Tuple tup;
			tup = list.get(i);
			Datum data = tup.getDataByName(colName);
			
			if(allData.add(data)){	//new attr value
				List<Tuple> allTuple = null;
				allTuple.add(list.get(i));
				orders.put(data, allTuple);
			}
			else{  //already have that attr value
				List<Tuple> thisTuple = orders.get(data);
				thisTuple.add(list.get(i));
				orders.put(data, thisTuple);
			}
		}
		
		//todo order the set
		//set to array
		Datum[] allDatum = allData.toArray(new Datum[0]);  //string type..... what if the attr type is int?? string(10)<string(2)
		//array to list
		List<Datum> datumlist = Arrays.asList(allDatum);  //<-could this happen...
		//quick sort that list
		qsort(datumlist);
		if(isAsc()==true){
			for(int i = 0;i<datumlist.size()-1;i++){
				Datum temp = datumlist.get(i);
				System.out.println(temp);  //shuchu
			}
			
		}
		
		else {  //DESC
			for(int i = datumlist.size()-1;i>=0;i++){
				Datum temp = datumlist.get(i);
				System.out.println(temp);  //shuchu
			}
			
		}
		//output tuples
		for(int k = 0;k<allDatum.length;k++){
			Datum temp = allDatum[k];
			List<Tuple> tmp = orders.get(temp);
			System.out.println(tmp); //print to screen or send to others
		}
	}
	
	//quick sort http://ppd.fnal.gov/experiments/cdms/old_files/software/net/dist/sort.java
	public void qsort(List<Datum> datumlist) throws Exception {
//		super(list);
	    quicksort(datumlist, 0, datumlist.size()-1);
	  }

	public void quicksort(List<Datum> datumlist, int p, int r) throws Exception {
	    if (p < r) {
	      int q = partition(datumlist,p,r);
	      if (q == r) {
		q--;
	      }
	      quicksort(datumlist,p,q);
	      quicksort(datumlist,q+1,r);
	    }
	  }

	 public int partition (List<Datum> datumlist, int p, int r) throws Exception {
	    Datum pivot = datumlist.get(p);
	    int lo = p;
	    int hi = r;
	    
	    while (true) {   
	      int cmprst1 = Datum.compare(datumlist.get(hi),pivot);  //???
	      int cmprst2 = Datum.compare(datumlist.get(lo),pivot);
	      while (cmprst1 >= 0 &&lo < hi) {hi--;}  //hi >= pivot
	      while (cmprst2 < 0 &&lo < hi)  {lo++;}   //lo < pivot
	      if (lo < hi) {
	    	  Datum T = datumlist.get(lo);
	    	  datumlist.set(lo, datumlist.get(hi)) ;
	    	  datumlist.set(hi, T) ;
	      }
	      else return hi;
	    }
	    
	  }      
	//---quick sort
	 
}

