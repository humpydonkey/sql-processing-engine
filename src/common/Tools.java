package common;

import java.util.ArrayList;
import java.util.List;

public class Tools {

	public static boolean arrayContains(Long[] a, Long target) {
		for (int i = 0; i < a.length; i++) {
			if (a.equals(target))
				return true;
		}
		return false;
	}

	public static <T extends Comparable<? super T>> void quickSort_Desc(T[] a,
			int left, int right) {
		if (left >= right) {
			return;
		}

		T pivot = a[left];
		int center = left;
		for (int j = left + 1; j <= right; j++) {
			if (a[j].compareTo(pivot) > 0) {
				center++;
				if (center != j)
					swap(a, center, j);
			}
		}
		swap(a, center, left); // swap pivot to center

		quickSort_Desc(a, left, center - 1); // recursion left part,
												// i=pivot,exclude pivot

		quickSort_Desc(a, center + 1, right); // recursion right part
	}
	
	public static <T extends Comparable<? super T>> void quickSort_Asce(T[] a,
			int left, int right) {
		if (left >= right) {
			return;
		}

		T pivot = a[left];
		int center = left;
		for (int j = left + 1; j <= right; j++) {
			if (a[j].compareTo(pivot) < 0) {
				center++;
				if (center != j)
					swap(a, center, j);
			}
		}
		swap(a, center, left); // swap pivot to center

		quickSort_Desc(a, left, center - 1); // recursion left part,
												// i=pivot,exclude pivot

		quickSort_Desc(a, center + 1, right); // recursion right part
	}
	public static <T extends Comparable<? super T>> void quickSort_Desc(
			List<T> list, int left, int right) {
		if (left >= right) {
			return;
		}

		T pivot = list.get(left);
		int center = left;
		for (int j = left + 1; j <= right; j++) {
			if (list.get(j).compareTo(pivot) > 0) {
				center++;
				if (center != j)
					swap(list, center, j);
			}
		}
		swap(list, center, left); // swap pivot to center

		quickSort_Desc(list, left, center - 1); // recursion left part,
												// i=pivot,exclude pivot

		quickSort_Desc(list, center + 1, right); // recursion right part
	}

	public static <T extends Comparable<? super T>> void quickSort_Asce(
			List<T> list, int left, int right) {
		if (left >= right) {
			return;
		}

		T pivot = list.get(left);
		int center = left;
		for (int j = left + 1; j <= right; j++) {
			if (list.get(j).compareTo(pivot) < 0) {
				center++;
				if (center != j)
					swap(list, center, j);
			}
		}
		swap(list, center, left); // swap pivot to center

		quickSort_Asce(list, left, center - 1); // recursion left part,
												// i=pivot,exclude pivot

		quickSort_Asce(list, center + 1, right); // recursion right part
	}

	public static <T> void swap(T[] a, int i, int j) {
		T temp = a[i];
		a[i] = a[j];
		a[j] = temp;
	}

	public static <T> void swap(List<T> a, int i, int j) {
		T temp = a.get(i);
		a.set(i, a.get(j));
		a.set(j, temp);
	}

	public static <T> void printArray(T[] a) {
		System.out.println();
		for (int i = 0; i < a.length; i++) {
			System.out.print(a[i] + " ");
		}
	}

	public static <T> void printArray(List<T> a) {
		System.out.println();
		for (int i = 0; i < a.size(); i++) {
			System.out.print(a.get(i) + " ");
		}
	}
	
	public static void main(String[] args){
		String[] data = new String[]{"E","F","A","C","B"};
		//int[] data = new int[]{5,2,3,4,1};
		List<String> list = new ArrayList<String>(data.length);
		for(int i=0; i<data.length; i++)
			list.add(data[i]);
		
		quickSort_Asce(list, 0, data.length-1);
		for(String obj : list){
			System.out.println(obj.toString());
		}
	}

}
