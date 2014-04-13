package ra;

import io.FileAccessor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;
import sql2ra.Config;
import sql2ra.SQLEngine;

public class ConditionReorganizer implements ExpressionVisitor{
	
	public static void main(String[] args){
		File files = new File("D:/testDecompose/");
		File schemaFile = new File("test/cp2_sqls/tpch_schemas.sql");
		File dataFile = new File("test/data");		
		File swapPath = new File("test/");
		for(File f : files.listFiles()){
			try {
				Config.setSwapDir(swapPath);
				SQLEngine engine = new SQLEngine(dataFile);

				//create schema
				FileReader schemaReader = new FileReader(schemaFile);
				Statement create;
				CCJSqlParser createParser = new CCJSqlParser(schemaReader);
				while((create =createParser.Statement()) !=null){		
					if(create instanceof CreateTable)	
						engine.create(create);
				}
				
				//read sql
				PlainSelect psel = FileAccessor.getInstance().parsePSelect(f);
				Map<String, Table>  tableMap = new HashMap<String, Table>();
				engine.extractLocalTable(psel, tableMap);	
				
				System.out.println(f.toString());
				System.out.println(psel.toString());
				
				Collection<String> tableNames = tableMap.keySet();
				if(tableNames.size()>1){	
					System.out.println("****parsing where: \n****"+psel.getWhere().toString());
					System.out.println("global table:"+tableNames.toString());
					Expression where = psel.getWhere();
					ConditionReorganizer decomposer = new ConditionReorganizer(tableNames);
					where.accept(decomposer);
					decomposer.printResults();
				}else{
					System.out.println("There is only one table, do not use EvaluatorDecomposeWhere.");
				}
				System.out.println();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JSQLParserException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public final static String MultiTable = "MultiTable";
	public final static String EqualJoin = "EqualJoin";
	
	//collect results during processing
	private Map<String, List<Expression>> conditionMap;
	
	//cache one column, one table
	private Column col;
	private Table tab;

	private List<EqualJoin> joins;
	
	//-1:not set state
	//0: both values
	//1:at least one is column and if they are both columns, 
	//they are from same table
	//2:both columns but from different table
	private int sameTableState;	
	
	public ConditionReorganizer(Collection<String> tableNames){

		sameTableState = -1;
		joins = new ArrayList<EqualJoin>();
		
		setConditionMap(new HashMap<String, List<Expression>>());
		for(String tName : tableNames)
			getConditionMap().put(tName, new LinkedList<Expression>());
		
		getConditionMap().put(MultiTable, new LinkedList<Expression>());
		getConditionMap().put(EqualJoin, new LinkedList<Expression>());
	}
	
	private void addMultiTableExpression(Expression expr){
		getConditionMap().get(MultiTable).add(expr);
	}
	
	
	private Column getColumn(){
		Column result = col;
		col = null;
		return result;
	}
	
	private Table getTable(){
		Table result = tab;
		tab = null;
		return result;
	}
	
	private void setTable(Table t){
		tab = t;
	}
	
	
	private void setState(int state){
		//keep the highest value, can't change it back
		//to lower value unless after using getState() 
		//to retrieve it.
		//the sameTableState means the highest number 
		//of different table of column that have 
		//scanned since the last getState()
		if(sameTableState>state)
			return;
		else
			sameTableState = state;
	}
	
	private int getState(){
		int result = sameTableState;
		sameTableState = -1;
		return result;
	}
	
	public Expression getExprByTName(String tName){
		List<Expression> exprs = conditionMap.get(tName);
		if(exprs==null||exprs.size()==0)
			return null;
			
		Expression exp = exprs.get(0);
		
		for(int i=1; i<exprs.size(); i++){
			exp = new AndExpression(exp, exprs.get(1));
		}
		
		return exp;
	}
	
	public static boolean isSameTable(Table left, Table right){
		if(left!=null&&right!=null){
			if(left.getAlias()!=null&&right.getAlias()!=null){
				if(left.getAlias().equalsIgnoreCase(right.getAlias()))
					return true;
				else
					return false;
			}else if(left.getName().equalsIgnoreCase(right.getName()))
				return true;
			else
				return false;
		}else{
			throw new UnsupportedOperationException("Unexpected......"); 
		}
	}
	
	private int fromSameTable(Column left, Column right){
		if(left!=null&&right!=null){
			//both are column
			Table leftT = left.getTable();
			Table rightT = right.getTable();
			
			//alias are both not null
			if(leftT.getAlias()!=null&&rightT.getAlias()!=null){
				//if from the same table
				if(leftT.getAlias().equalsIgnoreCase(rightT.getAlias())){
					setTable(left.getTable());
					return 1;
				}else	//not from the same table
					return 2;
			}else if(leftT.getName().equalsIgnoreCase(rightT.getName())){
				setTable(left.getTable());
				return 1;
			}else	//not from the same table
				return 2;	 
		}else{
			if(left==null&&right==null)
				return 0;//both are values
			else if(left!=null){
				//left column, right value
				setTable(left.getTable());
				return 1;
			}else{
				//right column, left value
				setTable(right.getTable());
				return 1;
			}
		}
	}
	

	private void distributeExpression(Table t, Expression expr){
		String alias = t.getAlias();
		if(alias!=null){
			if(getConditionMap().containsKey(alias)){
				getConditionMap().get(alias).add(expr);
			}else
				throw new UnsupportedOperationException("Unexpected......"); 
		}else if(t.getName()!=null){
			if(getConditionMap().containsKey(t.getName())){
				getConditionMap().get(t.getName()).add(expr);
			}else
				throw new UnsupportedOperationException("Unexpected......"); 
		}else{
			//both alias and table name are null
			//then it is a non-null table with all null value
			//it means there is only one table
			//there should not have such case
			throw new UnsupportedOperationException("Unexpected......"); 
		}
	}
	
	private boolean isEqualJoin(Column left, Column right){
		if(left!=null&&right!=null){
			if(left.getColumnName().equalsIgnoreCase(right.getColumnName())){
				String leftTName = left.getTable().getName();
				String rightTName = right.getTable().getName();
				if(!leftTName.equalsIgnoreCase(rightTName)){
					//if they are in the same select
					if(getConditionMap().containsKey(leftTName)&&getConditionMap().containsKey(rightTName))
						return true;
				}
			}
		}
		return false;
	}
	
	public void printResults(){
		if(Config.DebugMode){
			int i=1;
			for(Entry<String, List<Expression>> entry : getConditionMap().entrySet()){
				String tName = entry.getKey();
				System.out.print(i+")."+tName+": ");
				for(Expression exp:entry.getValue()){
					System.out.print(exp.toString()+", ");
				}
				i++;
				System.out.println();
			}
			System.out.println();
		}	
	}
	
	public List<EqualJoin> getJoinList(){
		return joins;
	}
	
	@Override
	public void visit(NullValue arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
		
	}

	@Override
	public void visit(Function arg0) {
		
	}

	@Override
	public void visit(InverseExpression arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
		
	}

	@Override
	public void visit(JdbcParameter arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
		
	}

	@Override
	public void visit(DoubleValue arg0) {
		
	}

	@Override
	public void visit(LongValue arg0) {
		
	}

	@Override
	public void visit(DateValue arg0) {
		
	}

	@Override
	public void visit(TimeValue arg0) {
		
	}

	@Override
	public void visit(TimestampValue arg0) {
		
	}

	@Override
	public void visit(Parenthesis arg0) {
		arg0.getExpression().accept(this);
		int state = getState();
		if(state==1){
			Table t = getTable();
			if(t!=null)
				distributeExpression(t,arg0);
			else
				throw new UnsupportedOperationException("Unexpected......"); 
		}else
			addMultiTableExpression(arg0);
			
	}

	@Override
	public void visit(StringValue arg0) {
		
	}

	@Override
	public void visit(Addition arg0) {
		arg0.getLeftExpression().accept(this);
		Column left = getColumn();		
		arg0.getRightExpression().accept(this);
		Column right = getColumn();

		setState(fromSameTable(left, right));
	}

	

	@Override
	public void visit(Division arg0) {
		arg0.getLeftExpression().accept(this);
		Column left = getColumn();		
		arg0.getRightExpression().accept(this);
		Column right = getColumn();

		setState(fromSameTable(left, right));
	}

	@Override
	public void visit(Multiplication arg0) {
		arg0.getLeftExpression().accept(this);
		Column left = getColumn();		
		arg0.getRightExpression().accept(this);
		Column right = getColumn();

		setState(fromSameTable(left, right));
	}

	@Override
	public void visit(Subtraction arg0) {
		arg0.getLeftExpression().accept(this);
		Column left = getColumn();		
		arg0.getRightExpression().accept(this);
		Column right = getColumn();
		
		setState(fromSameTable(left, right));
	}

	@Override
	public void visit(AndExpression arg0) {
		arg0.getLeftExpression().accept(this);
		
		arg0.getRightExpression().accept(this);
	}

	@Override
	public void visit(OrExpression arg0) {

	}

	@Override
	public void visit(Between arg0) {
		arg0.getLeftExpression().accept(this);
		Column col = getColumn();
		if(col!=null)
			distributeExpression(col.getTable(), arg0);
		else
			throw new UnsupportedOperationException("Unexpected......"); 
	}

	
	@Override
	public void visit(EqualsTo arg0) {
		//check left
		arg0.getLeftExpression().accept(this);
		int stateL = getState();
		if(stateL==2){
			addMultiTableExpression(arg0);
			return;
		}
		Column left = getColumn();
		
		//check right
		arg0.getRightExpression().accept(this);
		int stateR = getState();
		if(stateR==2){
			addMultiTableExpression(arg0);
			return;
		}
		Column right = getColumn();
		
		//check combined left and right
		int state = fromSameTable(left,right);
		
		if(state==2){
			//check if is equal join
			if(isEqualJoin(left,right)){
				joins.add(new EqualJoin(left,right));
				getConditionMap().get(EqualJoin).add(arg0);
			}else
				addMultiTableExpression(arg0);
			return;
		}else if(state==1){
			Table t = getTable();
			distributeExpression(t, arg0);
		}else{
			addMultiTableExpression(arg0);
		}
	}
	

	@Override
	public void visit(GreaterThan arg0) {
		//check left
		arg0.getLeftExpression().accept(this);
		int stateL = getState();
		if(stateL==2){
			addMultiTableExpression(arg0);
			return;
		}
		Column left = getColumn();
		
		//check right
		arg0.getRightExpression().accept(this);
		int stateR = getState();
		if(stateR==2){
			addMultiTableExpression(arg0);
			return;
		}
		Column right = getColumn();
		
		//check combined left and right
		int state = fromSameTable(left,right);
		if(state==2){
			addMultiTableExpression(arg0);
			return;
		}else if(state==1){
			Table t = getTable();
			distributeExpression(t, arg0);
		}else{
			addMultiTableExpression(arg0);
		}
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		//check left
		arg0.getLeftExpression().accept(this);
		int stateL = getState();
		if(stateL==2){
			addMultiTableExpression(arg0);
			return;
		}
		Column left = getColumn();
		
		//check right
		arg0.getRightExpression().accept(this);
		int stateR = getState();
		if(stateR==2){
			addMultiTableExpression(arg0);
			return;
		}
		Column right = getColumn();
		
		//check combined left and right
		int state = fromSameTable(left,right);
		if(state==2){
			addMultiTableExpression(arg0);
			return;
		}else if(state==1){
			Table t = getTable();
			distributeExpression(t, arg0);
		}else{
			addMultiTableExpression(arg0);
		}
	}

	@Override
	public void visit(InExpression arg0) {
		ItemsList itmlist = arg0.getItemsList();
		if(itmlist instanceof SubSelect){
			setState(2);
		}else{
			ExpressionList explist = (ExpressionList)itmlist;
			for(Object obj :explist.getExpressions()){
				Expression exp = (Expression)obj;
				exp.accept(this);
			}
		}
		
		int state = getState();
		if(state==2)
			addMultiTableExpression(arg0);
		else if(state==0){
			
			arg0.getLeftExpression().accept(this);
			//the right expression does not need 
			//his parent sql's information
			Column col = getColumn();
			if(col!=null)
				distributeExpression(col.getTable(), arg0);
			else
				throw new UnsupportedOperationException("Unexpected......"); 
		}else{
			throw new UnsupportedOperationException("Unhandled situation: state==-1 or state==-1."); 
		}

	}

	@Override
	public void visit(IsNullExpression arg0) {
		arg0.getLeftExpression().accept(this);
		Column col = getColumn();
		if(col!=null)
			distributeExpression(col.getTable(), arg0);
		else
			throw new UnsupportedOperationException("Unexpected......"); 
	}

	@Override
	public void visit(LikeExpression arg0) {
		arg0.getLeftExpression().accept(this);
		Column col = getColumn();
		if(col!=null)
			distributeExpression(col.getTable(), arg0);
		else
			throw new UnsupportedOperationException("Unexpected......"); 
	}

	@Override
	public void visit(MinorThan arg0) {
		//check left
		arg0.getLeftExpression().accept(this);
		int stateL = getState();
		if(stateL==2){
			addMultiTableExpression(arg0);
			return;
		}
		Column left = getColumn();
		
		//check right
		arg0.getRightExpression().accept(this);
		int stateR = getState();
		if(stateR==2){
			addMultiTableExpression(arg0);
			return;
		}
		Column right = getColumn();
		
		//check combined left and right
		int state = fromSameTable(left,right);
		if(state==2){
			addMultiTableExpression(arg0);
			return;
		}else if(state==1){
			Table t = getTable();
			distributeExpression(t, arg0);
		}else{
			addMultiTableExpression(arg0);
		}
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		//check left
		arg0.getLeftExpression().accept(this);
		int stateL = getState();
		if(stateL==2){
			addMultiTableExpression(arg0);
			return;
		}
		Column left = getColumn();
		
		//check right
		arg0.getRightExpression().accept(this);
		int stateR = getState();
		if(stateR==2){
			addMultiTableExpression(arg0);
			return;
		}
		Column right = getColumn();
		
		//check combined left and right
		int state = fromSameTable(left,right);
		if(state==2){
			addMultiTableExpression(arg0);
			return;
		}else if(state==1){
			Table t = getTable();
			distributeExpression(t, arg0);
		}else{
			addMultiTableExpression(arg0);
		}
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		//check left
		arg0.getLeftExpression().accept(this);
		int stateL = getState();
		if(stateL==2){
			addMultiTableExpression(arg0);
			return;
		}
		Column left = getColumn();
		
		//check right
		arg0.getRightExpression().accept(this);
		int stateR = getState();
		if(stateR==2){
			addMultiTableExpression(arg0);
			return;
		}
		Column right = getColumn();
		
		//check combined left and right
		int state = fromSameTable(left,right);
		if(state==2){
			addMultiTableExpression(arg0);
			return;
		}else if(state==1){
			Table t = getTable();
			distributeExpression(t, arg0);
		}else{
			addMultiTableExpression(arg0);
		}
	}

	@Override
	public void visit(Column arg0) {
		col = arg0;
	}

	@Override
	public void visit(SubSelect arg0) {
		setState(2);
	}

	@Override
	public void visit(CaseExpression arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
		
	}

	@Override
	public void visit(WhenClause arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
		
	}

	@Override
	public void visit(ExistsExpression arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
		
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
		
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
		
	}

	@Override
	public void visit(Concat arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
		
	}

	@Override
	public void visit(Matches arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
		
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
		
	}

	@Override
	public void visit(BitwiseOr arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
		
	}

	@Override
	public void visit(BitwiseXor arg0) {
		throw new UnsupportedOperationException("Not supported yet."); 
		
	}

	public Map<String, List<Expression>> getConditionMap() {
		return conditionMap;
	}

	public void setConditionMap(Map<String, List<Expression>> conditionMap) {
		this.conditionMap = conditionMap;
	}

}
