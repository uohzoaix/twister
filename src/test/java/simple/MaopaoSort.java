package simple;

public class MaopaoSort {
	// 主方法
	public static void main(String[] args) {
		int[] arr = { 4, 3, 5, 7, 1, 8, 11, 9 };
		println(arr);

		// maopaoSort(arr);
		// insertSort(arr);
		quickSort(arr, 0, arr.length - 1);
		println(arr);
	}

	// 排序方法
	public static void maopaoSort(int[] arr) {
		int tmp = 0;
		for (int i = 0; i < arr.length; i++) {
			for (int j = 0; j < arr.length - i - 1; j++) {
				if (arr[j] > arr[j + 1]) {
					tmp = arr[j];
					arr[j] = arr[j + 1];
					arr[j + 1] = tmp;
				}
			}
		}
		println(arr);
	}

	public static void insertSort(int[] arr) {
		int len = arr.length;
		int j = 0;
		int tmp = 0;
		for (int i = 1; i < len; i++) {
			j = 0;
			tmp = arr[i];
			for (j = i; j > 0; j--) {
				if (arr[j - 1] > tmp) {
					arr[j] = arr[j - 1];
				} else {
					break;
				}
			}
			arr[j] = tmp;
		}
		println(arr);
	}

	// 打印方法
	public static void println(int[] before) {
		for (int i = 0; i < before.length; i++) {
			System.out.print(before[i] + " ");
		}
		System.out.println();

	}

	/**
	 * 
	 * @param arr
	 * @param low
	 * @param hig
	 */
	public static void quickSort(int[] arr, int low, int hig) {
		if (low < hig) {
			int pivot = partition(arr, low, hig);
			quickSort(arr, low, pivot - 1);
			quickSort(arr, pivot + 1, hig);
		}
	}

	public static int partition(int[] arr, int l, int r) {
		int pivot = arr[r];
		// 将小于等于pivot的元素移到左边区域
		// 将大于pivot的元素移到右边区域
		int index = l;
		for (int i = index; i < r; i++) {
			if (arr[i] - pivot <= 0) {
				swap(arr, index++, i);
			}
		}
		swap(arr, index, r);
		return index;
	}

	public static void swap(int[] a, int i, int j) {
		int temp;
		temp = a[i];
		a[i] = a[j];
		a[j] = temp;
	}
}
