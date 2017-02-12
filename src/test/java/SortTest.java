/**
 * Created by heytitle on 12/21/16.
 */

import junit.framework.TestCase;
import org.apache.commons.collections.BufferOverflowException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.IndexedSorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.evaluation.utils.SorterFactory;
import org.evaluation.utils.Validator;
import org.evaluation.utils.generator.RandomTuple2LongInt;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Random;

public class SortTest extends TestCase {




	QuickSort qs = new QuickSort();

	@Test
	public  void testSort() throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
		int[] noRecords = new int[]{1000, 1000000};

		String[] sorters = new String[]{
			"org.evaluation.sorter.individual.optimization.DividedByConstant"
		};

		for ( String sorterName: sorters ) {
			System.out.println("Testing  " + sorterName );
			for( int i : noRecords ) {
				int seed = new Random().nextInt();
				InMemorySorter sorter = SorterFactory.getSorter(sorterName);

				fillRandomData(sorter, seed, i );

				if( sorterName.equals("org.example.sorter.individual.optimization.EmbedQuickSortInside")
					|| sorterName.equals("org.example.sorter.OptimizedSorter")
				) {
					((IndexedSorter)sorter).sort(sorter);
				} else {
					qs.sort(sorter);
				}

				boolean isSorted = Validator.isSorted(sorter.getIterator());

				assertTrue("Data is sorted property: seed " + seed + " , no. records " + i, isSorted);

			}
		}



	}

	@Test
	public  void testFlink3722() throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
		int[] noRecords = new int[]{1000, 1000000};

		String[] sorters = new String[]{
			"org.flink3722.NormalizedKeySorter",
		};

		org.flink3722.QuickSort qqs = new org.flink3722.QuickSort();

		for ( String sorterName: sorters ) {
			System.out.println("Testing  " + sorterName );
			for( int i : noRecords ) {
				int seed = new Random().nextInt();
				InMemorySorter sorter = SorterFactory.getSorter(sorterName);

				fillRandomData(sorter, seed, i );

				qqs.sort(sorter);

				boolean isSorted = Validator.isSorted(sorter.getIterator());

				assertTrue("Data is sorted property: seed " + seed + " , no. records " + i, isSorted);
			}
		}



	}

	public static void fillRandomData(InMemorySorter sorter, int seed, int noRecords) throws IOException {
		RandomTuple2LongInt generator = new RandomTuple2LongInt(seed);

		Tuple2<Integer,Integer> record = new Tuple2<Integer,Integer>();
		int num = 0;

		while (num < noRecords){
			generator.next(record);
			boolean succeed = sorter.write(record);
			if( !succeed ) {
				throw new BufferOverflowException(" We have enough space for " + num  + "records only.");
			}
			num++;
		}
	}

}
