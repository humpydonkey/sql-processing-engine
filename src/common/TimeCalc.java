package common;

public class TimeCalc {
	
	private static int[] ids = new int[10];
	private static long[] startTimes = new long[10];
	private static int size = 0;
	private static StringBuilder errMsg;
	
	static{
		for(int i=0; i<ids.length; i++)
			ids[i] = -1;
	}
	
	public static void begin(int id){
		 errMsg = new StringBuilder("\n");
		if(exist(id)){
			errMsg.append("TimeCalc.begin() : Error! There is alread exist such id: " + id + " \n");
		}else if(size>=10){
			errMsg.append("TimeCalc.begin() : Error! There is no room for another timer! \n");
		}else{
			ids[size] = id;
			startTimes[size] = System.currentTimeMillis();
			size++;
		}
	}
	
	public static int end(int id){
		int index = findIndex(id);
		if(index<0){
			errMsg.append("TimeCalc.end() : Error! There is no such id! \n");
			System.out.print(errMsg.toString());
			return 0;
		}
		else{
			long span_ms = System.currentTimeMillis() - startTimes[index];
			String msg;
			if(span_ms>1000){
				float span_s = (float)span_ms/1000;
				msg = id + " running time : " + span_s + "s.";
			}else
				msg = id + " running time : " + span_ms + "ms.";
				
			System.out.println(msg + errMsg.toString());
			
			ids[index] = -1;	
			size--;
			return (int)span_ms;
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
