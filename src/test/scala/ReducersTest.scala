import mapreduce.MeanMedianMaxReducer
import org.apache.hadoop.io.IntWritable
import org.scalatest.FunSuite

import scala.collection.mutable.ListBuffer

class ReducersTest extends FunSuite {

  //Test of method to calculate  median of list of elements
  test("Test median calculation for list with even number of elements"){
    val elementList = ListBuffer(new IntWritable(1), new IntWritable(2), new IntWritable(3), new IntWritable(4), new IntWritable(5), new IntWritable(6), new IntWritable(7), new IntWritable(8))
    val median = new MeanMedianMaxReducer().calculateMedian(elementList);
    assert(median == 4.5, "Median of even number of elements failed")
  }

  test("Test median calculation for list with odd number of elements"){
    val elementList = ListBuffer(new IntWritable(9), new IntWritable(10), new IntWritable(11), new IntWritable(12), new IntWritable(13), new IntWritable(14), new IntWritable(15))
    val median = new MeanMedianMaxReducer().calculateMedian(elementList);
    assert(median == 12.0, "Median of odd number of elements failed")
  }

  test("Test median calculation for list with a single element"){
    val elementList = ListBuffer(new IntWritable(9))
    val median = new MeanMedianMaxReducer().calculateMedian(elementList);
    assert(median == 9.0, "Median of single element failed")
  }

}
