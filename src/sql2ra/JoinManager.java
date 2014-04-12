package sql2ra;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import common.TimeCalc;
import ra.EqualJoin;
import ra.Operator;
import ra.OperatorHashJoin;
import ra.OperatorHashJoin_Block;
import ra.OperatorHashJoin_HalfMem;
import ra.OperatorHashJoin_Mem;

/**
 * Decide which join to use,
 * what is the order to join
 * @author Asia
 *
 */
public class JoinManager {
	private Set<String> joinedHistory;
	private List<EqualJoin> joinList;
	private Map<String, Operator> operMap;
	private File swapDir;
	
	public JoinManager(List<EqualJoin> joins, Map<String, Operator> operMapIn, File swapDirIn){
		joinedHistory = new HashSet<String>();
		joinList = joins;
		operMap = operMapIn;
		swapDir = swapDirIn;
		
		//find and set size
		for(EqualJoin ej : joins){
			ej.setLeftSize(operMap.get(ej.getLeftTableName()).getLength());
			ej.setRightSize(operMap.get(ej.getRightTableName()).getLength());
		}
		
		//move the smallest first, and
		//make each of them has an overlap, 
		//so that easy to pipeline
		reorder(joins);
	}
	

	public Operator executeJoin(){
		/*********************    EqualJoin    ********************/	
		Operator join = null;
		//for each join, decide which join to use
		for(EqualJoin ej : joinList){
			TimeCalc.begin(6);
			File resultFile = new File(swapDir.getPath()+"/"+ej.getLeftTableName()+"^"+ej.getRightTableName());
			Operator leftOper = operMap.get(ej.getLeftTableName());
			Operator rightOper = operMap.get(ej.getRightTableName());
			if(join==null)
				join = createHashJoin(ej.getColName(), leftOper, rightOper, resultFile);
			else{
				Operator neverJoinedOper;
				if(joinedHistory.contains(ej.getLeftTableName()))
					neverJoinedOper = rightOper;
				else
					neverJoinedOper = leftOper;
				
				join = createHashJoin(ej.getColName(), join, neverJoinedOper, resultFile);
			}
			
			joinedHistory.add(ej.getLeftTableName());
			joinedHistory.add(ej.getRightTableName());
			TimeCalc.end(6,"finish one equal join, size: "+ join.getLength() +" ---------------------------------------------------------");
		}//end for
		
		return join;
	}
	
	private OperatorHashJoin createHashJoin(String attr, Operator leftOper, Operator rightOper, File resultFile){
		OperatorHashJoin join;
		long left = leftOper.getLength();
		long right = rightOper.getLength();
		
		if(left<Config.FileThreshold_MB && right<Config.FileThreshold_MB){
			if(left>right)
				join = new OperatorHashJoin_Mem(
						attr,
						rightOper,
						leftOper);
			else
				join = new OperatorHashJoin_Mem(
						attr,
						leftOper,
						rightOper);
		}else if(left>Config.FileThreshold_MB && right<Config.FileThreshold_MB){
			join = new OperatorHashJoin_HalfMem(
					attr,
					rightOper,
					leftOper,
					resultFile);
		}else if(left<Config.FileThreshold_MB && right>Config.FileThreshold_MB){
			join = new OperatorHashJoin_HalfMem(
					attr,
					leftOper,
					rightOper,
					resultFile);
		}else{
			//both large
			Operator small;
			Operator large;
			
			if(left>right){
				small = rightOper;
				large = leftOper;
			}else{
				small = leftOper;
				large = rightOper;
			}
				
			join = new OperatorHashJoin_Block(
					attr,
					small,
					large,
					resultFile);
		}
		
		return join;
	}

	private void reorder(List<EqualJoin> joins){
		//find the total min
		int n  = joins.size();
		int minIndex = 0;
		long min = Integer.MAX_VALUE;
		for(int i=0; i<n; i++){
			if(joins.get(i).getTotalSize()<min)
				minIndex = i;
		}
		//swap the min value to the first
		swap(joins, 0, minIndex);
		
		
		//based on min, the first join in the list, 
		//reorder it by each join should have one
		//table in the previous join, in order to pipeline
		Set<String> previous = new HashSet<String>(n+2);
		previous.add(joins.get(0).getLeftTableName());
		previous.add(joins.get(0).getRightTableName());
		for(int i=1; i<n; i++){
			EqualJoin current = joins.get(i);
			boolean left = previous.contains(current.getLeftTableName());
			boolean right = previous.contains(current.getRightTableName());
			if(!left&&!right){
				//both not contains, then switch
				for(int j=i+1; j<n; j++){
					EqualJoin after = joins.get(j);
					boolean leftAfter = previous.contains(after.getLeftTableName());
					boolean rightAfter = previous.contains(after.getRightTableName());
					if(leftAfter||rightAfter){
						swap(joins, i, j);
						previous.add(after.getLeftTableName());
						previous.add(after.getRightTableName());
						break;
					}
				}//end for j
			}else{
				previous.add(current.getLeftTableName());
				previous.add(current.getRightTableName());
			}
		}//end for i
	}
	
	private void swap(List<EqualJoin> list, int i, int j){
		EqualJoin temp = list.get(i);
		list.set(i, list.get(j));
		list.set(j, temp);
	}
	

}
