package common;

public class TimeCalc {
	
	private static int[] ids = new int[10];
	private static long[] startTimes = new long[10];
	private static int size = 0;
	
	public static void begin(int id){
		if(exist(id)){
			System.out.println("Error! There is no such id!");
		}else if(size>=10){
			System.out.println("Error! There is no room for another timer!");
		}else{
			ids[size] = id;
			startTimes[size] = System.currentTimeMillis();
			size++;
		}
	}
	
	public static void end(int id){
		int index = findIndex(id);
		if(index<0)
			System.out.println("Error! There is no such id!");
		else{
			long span_ms = System.currentTimeMillis() - startTimes[index];
			String msg;
			if(span_ms>1000){
				float span_s = (float)span_ms/1000;
				msg = id + " running time : " + span_s + "s.";
			}else
				msg = id + " running time : " + span_ms + "ms.";
				
			System.out.println(msg);
			
			ids[index] = -1;
			size--;
		}
	}
	
	private static boolean exist(int idIn){
		for(int id : ids){
			if(id==idIn) return true;
		}
		return false;
	}
	
	private static int findIndex(int idIn){
		for(int i=0; i<ids.length; i++){
			if(ids[i]==idIn) return i;
		}
		return -1;
	}
}
