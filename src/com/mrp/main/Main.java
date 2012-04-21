package com.mrp.main;

import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Scanner;

import com.mrp.prm.PRM;
import com.mrp.sgm.SGM;
import com.mrp.tjsgm.TJSGM;

public class Main {
	final String[] QUERY = { "Q2.1", "Q2.2", "Q2.3", "Q3.1", "Q3.2", "Q3.3", "Q3.4", "Q4.1", "Q4.2", "Q4.3" };
	private Scanner scanner = new Scanner(System.in);

	public static void main(String[] args) {
		new Main();
	}

	Main() {

		int choose = chooseExperiment();
		if (choose < 0) {
			ERROR("Experiment choose error.");
			return;
		} else {
			runExperiment(choose);
		}

	}

	private int chooseExperiment() {
		int choose = -1;
		println("Which experiment do you want to run?");
		println("1. Base query(All method)(EQ)");
		println("2. Block size(EQ & NEQ)");
		println("3. Selectivity (80/20)(EQ)");
		println("4. Theta-join with different reducers(NEQ)");
		println("5. Dymatic reducers(SGM, TJSGM-)(EQ)");
		println("6. TJSGM vs TJSGM-(partition function(EQ)");
		try {
			choose = scanner.nextInt();
		} catch (InputMismatchException e) {
			return -1;
		}
		return choose;
	}

	private Object[] chooseQuery() {
		List<Integer> choose = new ArrayList<Integer>();
		scanner.nextLine();
		println("Query = {Q2.1, Q2.2, Q2.3, Q3.1, Q3.2, Q3.3, Q3.4, Q4.1, Q4.2, Q4.3}");
		print("Choose query(ex: 1,2,3) 0 = all query, 11=1,4,8:");
		try {
			String tmp = scanner.nextLine();
			// choose multiple query
			for (String value : tmp.split(",")) {
				int tmpValue = Integer.parseInt(value);
				switch (tmpValue) {
				// add all query
				case 0:
					for (int i = 0; i < QUERY.length; i++) {
						if (!choose.contains(i)) {
							choose.add(i);
						}
					}
					break;
				// add query 1,4,8
				case 11:
					if (!choose.contains(0)) {
						choose.add(0);
					}
					if (!choose.contains(3)) {
						choose.add(3);
					}
					if (!choose.contains(7)) {
						choose.add(7);
					}
					break;
				// add selected query
				default:
					if (tmpValue > 0 && tmpValue < 11) {
						if (!choose.contains(tmpValue - 1)) {
							choose.add(tmpValue - 1);
						}
					}
				}
			}
			return choose.toArray();
			// input non-integer
		} catch (Exception e) {
			return null;
		}
	}

	private void runExperiment(int runType) {
		int[] query = convertObjectArrayToIntegerArray(chooseQuery());
		if (query == null || query.length == 0) {
			ERROR("Query choose error.");
			return;
		}
		switch (runType) {
		case 0:
			debug();
			break;
		case 1:
			runExperiment1(query);
			break;
		case 2:
			break;
		case 3:
			break;
		case 4:
			break;
		case 5:
			// runExperiment5(round);
			break;
		case 6:
			// runExperiment6(round);
			break;
		default:
			System.out.println("Bye.");
		}
	}

	private void runExperiment1(int[] query) {
		System.out.print("How many rounds you want? ");
		int round = scanner.nextInt();
		SGM sgm = new SGM();
		TJSGM tjsgm = new TJSGM();
		PRM prm = new PRM();
		long[][][] result = new long[3][round][query.length];
		for (int j = 0; j < round; j++) {
			for (int i = 0; i < query.length; i++) {
				result[0][j][i] = sgm.run(QUERY[query[i]]);
			}
			for (int i = 0; i < query.length; i++) {
				result[1][j][i] = tjsgm.run(QUERY[query[i]]);
			}
			for (int i = 0; i < query.length; i++) {
				result[2][j][i] = prm.run(QUERY[query[i]]);
			}
		}

		// print
		String[] Q = { "SGM", "TJSGM", "PRM" };
		for (int i = 0; i < result.length; i++) {
			System.out.println("Algorithm:" + Q[i]);
			for (int j = 0; j < result[i].length; j++) {
				for(int k = 0; k < result[i][j].length; k++){
					System.out.print(QUERY[query[k]]+"\t");
				}
				System.out.println();
				for (long q : result[i][j]) {
					System.out.print(q + "\t");
				}
				System.out.println();
			}
		}
	}

	private void debug() {
		// new SGM().run("Q3.4");
//		 new TJSGM().run("THETA");
		new PRM().run("THETA");
	}

	private int[] convertObjectArrayToIntegerArray(Object[] objectArray) {
		try {
			int[] intArray = new int[objectArray.length];
			for (int i = 0; i < objectArray.length; i++) {
				intArray[i] = (Integer) objectArray[i];
			}
			return intArray;
		} catch (Exception e) {
			return null;
		}
	}

	// ===trash code===
	private void print(Object[] var) {
		for (Object v : var) {
			System.out.print(v.toString() + "\t");
		}
		System.out.println();
	}

	private void print(int[] var) {
		for (int v : var) {
			System.out.print(v + "\t");
		}
		System.out.println();
	}

	private void println(Object var) {
		System.out.println(var.toString());
	}

	private void print(Object var) {
		System.out.print(var.toString());
	}

	// ===trash code===

	private void ERROR(String msg) {
		print("Execution Error: " + msg);
	}

}
