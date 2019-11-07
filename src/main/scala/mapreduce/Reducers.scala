package mapreduce

import java.lang
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


//Reducer class to perform word count operation
class Reducers extends Reducer[Text, IntWritable, Text, IntWritable] {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    logger.info("Executing reducer to display total count of input from mapper completed")
    //Calculate the sum of each value for a key
    val sum = values.foldLeft(0)(_ + _.get)
    context.write(key, new IntWritable(sum))
    logger.info("Reducer execution completed")
  }
}

//Reducer to calculate authorship score
class AuthorshipScoreReducer extends Reducer[Text, DoubleWritable, Text, DoubleWritable]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  override def reduce(key: Text, values: lang.Iterable[DoubleWritable], context: Reducer[Text, DoubleWritable, Text, DoubleWritable]#Context): Unit = {
    logger.info("Executing reducer to give summation of authorship score of every author")
    //Sum of authorship score of the author in each article/book/publication
    val sum = values.foldLeft(0.0)(_ + _.get)
    context.write(key, new DoubleWritable(sum))
    logger.info("Reducer execution completed")
  }
}

//Reducer to calculate the number of unique collaborations for an author
class AuthorCollaborationReducer extends Reducer[Text, Text, Text, IntWritable]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  override def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, IntWritable]#Context): Unit = {
    logger.info("Executing author collaboration")
    logger.info("author" + key)
    val coAuthorlist = new ListBuffer[String]

    //For the given author iterate through the list of co-authors
    //Check if the given author already belongs to the list coAuthorLiat
    //If not add the author to the list
    //This ensures that duplicate values are not inserted and we have count of unique collaborations
    //Output length of thelist
    for(author <- values){
      if(!coAuthorlist.contains(author.toString))
        coAuthorlist.add(author.toString)
    }
    context.write(key, new IntWritable(coAuthorlist.length))
    logger.info("Reducer execution completed")
  }
}

//Reducer class to calculate mean, median and max of the number of co-authors for a given author across DBLP dataset
class MeanMedianMaxReducer extends Reducer[Text, IntWritable, Text, Text]{

  //method to calculate median of a given list of elements
  def calculateMedian(authorCount : ListBuffer[IntWritable]): Double ={
    if(authorCount.length == 1) authorCount.max.get() * 1.0 //case when there is only one element
    else if(authorCount.length % 2 == 1) authorCount.sorted.get(authorCount.length/2).get() * 1.0 //case when there are odd number of elements in the list
    else (authorCount.sorted.get(authorCount.length/2).get() + authorCount.sorted.get(authorCount.length/2 - 1).get()) / 2.0 //case when there are even number of elements in the list
  }

  override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    logger.info("Reducer received this value: " + values)
    val authorCount = new ListBuffer[IntWritable]
    //Write items from Iterable values to iterate through the list multiple times
    for(value <- values){
      authorCount.add(new IntWritable(value.get()))
    }
    //calculating mean
    val sum = authorCount.foldLeft(0.0)(_+_.get())
    val mean = sum /  authorCount.length

    //calculating max
    val max = authorCount.max

    //calculating median
    val median = calculateMedian(authorCount)

    context.write(key, new Text(mean + "," + median + "," + max))
    logger.info("Reducer execution completed")
  }
}
