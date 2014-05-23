package dao;

import java.io.Serializable;
import java.rmi.UnexpectedException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import ra.Aggregator;
import ra.EvaluatorArithmeticExpres;
import ra.OperatorGroupBy;
import dao.Datum.CastError;

/**
 * A row of a table
 * @author Asia
 *
 */
public class Tuple implements Serializable{
	
	private static final long serialVersionUID = -6349689782189417493L;
	
	private Row row;
	private Schema schema;
	
	/**
	 * Constructor
	 */	
	public Tuple(String rawLine, Schema schemaIn){
		String[] splitedData = rawLine.split("\\|");
		
		int n = schemaIn.getLength();
		Datum[] dataArr = new Datum[n];	
		row = new Row(dataArr);
		schema = schemaIn;
		
		for(int i=0; i<n; i++){
			DatumType type = schemaIn.getColType(i);
			dataArr[i] = DatumFactory.create(splitedData[schema.getRawPosition(i)], type);
		}		
	}
	
	public Tuple(Row rowIn, Schema schemaIn){
		row  = rowIn;
		schema = schemaIn;
	}
	
	public Tuple(Datum[] dataIn, Schema schemaIn){
		row = new Row(dataIn);
		schema = schemaIn;
	}
	
	public Tuple.Row getRow(){ return row; }
	public Datum[] getDataArr(){ return row.getData(); }
	
	public void changeTuple(Schema newSchema){
		int length = newSchema.getLength();
		Datum[] newDataArr = new Datum[length];

		Expression[] newColSources = newSchema.getColumnSources();
		//Get new data from newSchema.source
		for(int i=0; i<length; i++){
			//Get data from old tuple
			Datum oldData = null;
			Expression newSource = newColSources[i];

			if(newSource instanceof Column){
				//is a column
				Column ns = (Column)newSource;
				oldData = getDataByName(ns);
			}else if(newSource instanceof Function){ 
				//is an aggregate function
				Function func = (Function)newSource;

				//find the key
				Aggregator aggre = newSchema.getAggregator(func);
				StringBuilder groupbyKey = new StringBuilder();
				for(String colName : aggre.getGroupByColumns()){
					if(colName.equals(OperatorGroupBy.NOGROUPBYKEY)){
						//no group by
						groupbyKey.append(OperatorGroupBy.NOGROUPBYKEY);
						break;
					}
						
					Datum groupbyColumn = getDataByName(colName); //key
					groupbyKey.append(groupbyColumn.toString());
				}
				//map to the aggregated Datum value
				try {
					oldData = aggre.getValue(groupbyKey.toString());
					if(oldData==null)
						throw new UnexpectedException("Can not find aggregate value, key:"+groupbyKey.toString());
				} catch (CastError e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}catch (UnexpectedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}else{
				//should be a constant or expression
				EvaluatorArithmeticExpres eval = new EvaluatorArithmeticExpres();
				oldData = eval.parse(newSource, this);
			}
			newDataArr[i] = oldData;
		}
		row = new Row(newDataArr);
		schema = newSchema;
	}
	
	
	public int compareTo(Tuple compTup, CompareAttribute[] attrs){
		for(CompareAttribute attr : attrs){
			String colName = attr.getColName();
			Datum selfVal = this.getDataByName(colName);
			Datum compVal = compTup.getDataByName(colName);
			int res = selfVal.compareTo(compVal);
			if(res==0)
				continue;
			
			//res!=0, two attributes are not equal
			if(attr.isAsc())
				return res;
			else
				return -res;		
		}
		
		return 0;
	}

	
	/**
	 * Get specific data block by column index
	 * @param index
	 * @return
	 * @throws Exception 
	 */
	public Datum getData(int index) throws UnexpectedException{
		if(index>=row.length()){
			throw new UnexpectedException("Index out of range! index: " + index + ", Length: " + row.length());
		}else{
			return row.getDatum(index);
		}		
	}
	
	/**
	 * Get data by column name
	 * @param colName
	 * @return
	 * @throws Exception
	 */
	public Datum getDataByName(String colName){
		int index = schema.getColIndex(colName);
		if(index<0){
			if(colName.contains(".")){
				colName = colName.split("\\.")[1];
				index = schema.getColIndex(colName);
				if(index>=0)
					return row.getDatum(index);
				else
					return null;
			}else
				return null;	
		}else
			return row.getDatum(index);
	}
	
	
	public Datum getDataByName(Column col){
		if(col.getTable()==null)
			return getDataByName(col.getColumnName());
		else{
			//return full name
			Datum data = getDataByName(col.toString());
			if(data==null)
				data = getDataByName(col.getColumnName());
			return data;
		}
	}
	
	
	public boolean setDataByName(Datum data, Column col){
		int pos = schema.getColIndex(col.getColumnName());
		if(pos<0)
			return false;
		row.setDatum(pos, data);
		return true;
	}
	
	
	/**
	 * Get Datum type by column name
	 * @param colName
	 * @return
	 */
	public DatumType getDataTypeByName(Column colName){
		String name = colName.getTable()==null?colName.getColumnName():colName.toString();
		int index = schema.getColIndex(name);
		if(index<0)
			return null;
		else
			return schema.getColType(index);
	}
	
	
	
	public String getTableName(){
		return schema.getTableName();
	}
	
	
	public String getTableAlias(){
		return schema.getTableAlias();
	}
	
	
	/**
	 * Print tuple on screen
	 */
	public void printTuple(){
		int i=0;
		for(Datum data : row.getData()){
			i++;
			if(i==row.length())
				System.out.println(data.toString());
			else
				System.out.print(data.toString()+"|");
		}
	}
	
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		for(Datum data : row.getData())
			sb.append(data.toString()+'|');
		sb.deleteCharAt(sb.length()-1);
		return sb.toString();
	}
	
	
	public Schema getSchema(){
		return schema;
	}
	
	
	public long getBytes(){
		long size = 0;
		for(Datum data : row.getData()){
			size += data.getBytes();
		}
		return size;
	}
	
	
	public Tuple.Row getPrmyKey(){
		int[] keyIndex = schema.getPrmyKeyIndex();
		Datum[] key = new Datum[keyIndex.length];
		Datum[] data = row.getData();
		for(int i=0; i<keyIndex.length; i++){
			key[i] = data[i];
		}
		return new Tuple.Row(key);
	}
	
	/**
	 * Get Comparator by Compare Attribute
	 * @param attrs
	 * @return
	 */
	public static Comparator<Tuple> getComparator(final CompareAttribute[] attrs){
		Comparator<Tuple> comptr = new Comparator<Tuple>(){
			@Override
			public int compare(Tuple arg0, Tuple arg1) {
				return arg0.compareTo(arg1, attrs);
			}
		};
		return comptr;
	}
	
	
	/**
	 * Get Comparator by Columns
	 * @param attrs: Columns
	 * @param isAsc: Is Asc or Desc
	 * @return
	 */
	public static Comparator<Tuple> getComparator(final Column[] attrs, final boolean[] isAsc){
		CompareAttribute[] compAttr = new CompareAttribute[attrs.length];
		for(int i=0; i<attrs.length; i++){
			compAttr[i] = new CompareAttribute(attrs[i], isAsc[i]);
		}
		return getComparator(compAttr);		
	}
	
	
	public static Comparator<Tuple> getComparator(final List<Column> attrs, final boolean[] isAsc){
		CompareAttribute[] compAttr = new CompareAttribute[attrs.size()];
		for(int i=0; i<attrs.size(); i++){
			compAttr[i] = new CompareAttribute(attrs.get(i), isAsc[i]);
		}
		return getComparator(compAttr);		
	}
	
	
	public static class Row implements Serializable, Comparable<Row>{
		private static final long serialVersionUID = -5862096214159025225L;
		private Datum[] data;
		
		public Row(int size){ data = new Datum[size]; }
		public Row(Datum[] d){ data = d; }
		
		public int length(){ return data.length; }
		public Datum[] getData(){ return data; }
		public Datum getDatum(int i){ return data[i]; }
		public void setDatum(int i, Datum d){ data[i] = d; }
		public String toString(){ return Arrays.toString(data); }
		public int compareTo(Row o) { return Row.compareRows(data, o.getData()); }
		public boolean equals(Object o){  
			if(o instanceof Tuple.Row){
				Tuple.Row obj = (Tuple.Row)o;
				return this.compareTo(obj)==0?true:false;
			}else
				return false;
		}
		
		public static int compareRows(Datum[] a, Datum[] b) {
			int cmp;
			for (int i = 0; i < a.length; i++) {
				if (i >= b.length) {
					return 0;
				}
				cmp = a[i].compareTo(b[i]);
				if (cmp != 0) {
					return cmp;
				}
			}
			return 0;
		}
	}
	
//	public static void main(String[] args){
//		 Date d1 = new Date(1994,01,01);
//		 Date d2 = new Date(1993,12,12);
//		 System.out.println(d1.compareTo(d2));
//	}

}
