package sqlparse;

import java.io.File;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import ra.EvaluatorConditionForIndex;
import ra.Operator;
import ra.OperatorCache;
import ra.OperatorIndexScan;
import ra.OperatorIndexScan.IndexScanType;
import dao.Datum;
import dao.Schema;
import dao.Tuple;


/**
 * Convert a FromItem to an Operator, data source
 * A FromItem could be a Table/Subselect/Subjoin object
 * @author Asia
 *
 */
public class FromItemConvertor implements FromItemVisitor{
	//private final File dataPath;
	private Map<String,CreateTable> ctMapper;
	private String tableName;
	private Map<String, Column> colsInUseMapper;
	private SQLEngine engine;
	private Schema schema = null;
	private Operator source = null;
	private SelectionParser selParser;
	private EvaluatorConditionForIndex eval;
	public FromItemConvertor(File dataDir, SelectionParser selParserIn, Map<String,CreateTable> tablesIn, Map<String, Column> colsInUse, SQLEngine engineIn)
	{
		colsInUseMapper = colsInUse;
		//dataPath = dataDir;
		ctMapper = tablesIn;
		engine = engineIn;
		selParser = selParserIn;
		eval = new EvaluatorConditionForIndex();
	}
	
	public String getTableName(){
		return tableName;
	}
	
	public Schema getSchema(){
		return schema;
	}
	
	public Operator getSourceOperator(){
		return source;
	}
	
	public void visit (SubJoin subjoin)
	{
		throw new UnsupportedOperationException("Unexpected......"); 
	}
	
	public void visit(SubSelect subselect)
	{
		if(subselect.getAlias()==null)
			this.tableName = subselect.toString();
		else
			this.tableName = subselect.getAlias();
		
		List<Tuple> tuples = engine.select(subselect.getSelectBody());
		source = new OperatorCache(tuples); 
	}
	
	public void visit(Table tableName)
	{
		String alias = tableName.getAlias();
		if(alias!=null){
			this.tableName = alias;
		}else{
			this.tableName =tableName.getName();
		}
		
		CreateTable ctable = ctMapper.get(tableName.getName().toUpperCase());

		try {
			if(ctable==null)
				throw new Exception("No such table : " + tableName.getName());
			
			schema = Schema.schemaFactory(null, ctable, tableName);
			//source = new OperatorScan(new File(dataPath, tableName.getName() + ".dat"),schema);
			
			OperatorIndexScan scan = createIndexScan(selParser.getSeparateExprs(schema.getTableName()), schema);
			scan.init();
			source = scan;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public OperatorIndexScan createIndexScan(List<Expression> expList, Schema schema){
		//decide which index to use		
		if(expList.size()==0){	//no condition in this table
			//0 is primary key index
			Index pk = schema.getIndexesInfo().get(0);
			OperatorIndexScan scan = new OperatorIndexScan(engine.getIndexManager(), schema, pk, null, IndexScanType.All);
			return scan;
		}else{
			Index indexToUse = null;
			Datum[] range = null;
			int priority = 0;
			IndexScanType type = IndexScanType.All;
			for(Expression exp : expList){
				exp.accept(eval);	//parsing
				Column col = eval.getColumn();
				Datum[] val = eval.getData();
				//the condition must have one column and at least one value
				if(col==null||val==null)//it's not a valid
					continue;
				
				int current_priority = eval.getPriority(); 
				//if have match in Indexes
				for(Index idx : schema.getIndexesInfo()){
					@SuppressWarnings("unchecked")
					List<String> colNames = idx.getColumnsNames();
					//TODO now only compare to index that has only one column
					if(colNames.size()==1){	
						String idxName = colNames.get(0);
						//the column has a index
						if(col.getColumnName().equalsIgnoreCase(idxName)){
							if(current_priority>priority){
								priority = current_priority;
								indexToUse = idx;
								range = val;
								type = eval.getType();
							}
						}
					}
				}//end for index
			}//end for expression

			OperatorIndexScan scan;
			if(indexToUse == null){
				//user primary key index
				indexToUse = schema.getIndexesInfo().get(0);
				scan =  new OperatorIndexScan(engine.getIndexManager(), schema, indexToUse, null, type);
			}else{
				//use this expression to do index scan 
				scan =  new OperatorIndexScan(engine.getIndexManager(), schema, indexToUse, range, type);
			}

			return scan;
		}
	}
	
	
	public static void main(String[] a){
//		List<String> strs = new ArrayList<String>(){{add("111"); add("222");}};
//		System.out.println(Arrays.toString(strs.toArray()));
	}

}
