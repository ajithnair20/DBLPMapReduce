package mapreduce

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{DoubleWritable, IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.slf4j.{Logger, LoggerFactory}

import scala.xml.XML


// Includes mapper functions to find author count, authorship score, etc...


//Class mapper includes generic function to process XML string to extract list of authors, years and other data from the XML
class Mappers {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def getElementFromXML(xml:String, element:String) : List[String] ={
    logger.info("extracting values from XML")
    logger.info(xml + ": " + element)
    val parent = XML.loadString(xml)
    val elementList  =  (parent \\ element).map(el => el.text.toLowerCase.trim).toList
    elementList
  }
}

//Mapper to find number of publications of individual authors
class AuthorCountMapper extends Mapper[LongWritable, Text, Text, IntWritable] {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val configuration = new Configuration
  val conf = ConfigFactory.load("Configuration")
  val one = new IntWritable(1)
  val dblpdtd = getClass.getClassLoader.getResource("dblp.dtd").toURI

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    logger.info("Executing mapper to get count of publications of each author")
    val xmlString =
        s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dblpdtd">
              <dblp>""" + value.toString + "</dblp>"
    val authors = new Mappers().getElementFromXML(xmlString,"author") //extract list of authors from the XML string
    for (author <- authors) context.write(new Text(author), one)
    logger.info("Mapper execution completed")
  }
}

//Mapper to find number of publications published each year
class YearCountMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val configuration = new Configuration
  val conf = ConfigFactory.load("Configuration")
  val one = new IntWritable(1)
  val dblpdtd = getClass.getClassLoader.getResource("dblp.dtd").toURI

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    logger.info("Executing mapper to get count of publications published each year")
    val xmlString =
        s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dblpdtd">
              <dblp>""" + value.toString + "</dblp>"
    val years = new Mappers().getElementFromXML(xmlString,"year").sorted //extract list of years from the XML string
    for (year <- years) context.write(new Text(year), one)
    logger.info("Mapper execution completed")
  }
}

//Mapper to calculate histogram where each bin shows the range of the numbers of co-authors
class CoAuthorRangeMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val configuration = new Configuration
  val conf = ConfigFactory.load("Configuration")
  val one = new IntWritable(1)
  val dblpdtd = getClass.getClassLoader.getResource("dblp.dtd").toURI
  //bins based on range of co-authors
  val range1 = (1 to 1).toList
  val range2 = (2 to 3).toList
  val range3 = (4 to 10).toList
  val range4 = (11 to 30).toList
  val range5 = (31 to 300).toList
  val range6 = 301

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    logger.info("Executing mapper to generate range of co-authors in papers")
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dblpdtd">
              <dblp>""" + value.toString + "</dblp>"
    val authors = new Mappers().getElementFromXML(xmlString,"author")

    //Check number of co-authors and assign it to corresponding bins
    if(range1.contains(authors.length))
      context.write(new Text("(1 Author)"), one)
    else if(range2.contains(authors.length))
      context.write(new Text("(2-3 Authors)"), one)
    else if(range3.contains(authors.length))
      context.write(new Text("(4-10 Authors)"), one)
    else if(range4.contains(authors.length))
      context.write(new Text("(11-30 Authors)"), one)
    else if(range5.contains(authors.length))
      context.write(new Text("(31-300 Authors)"), one)
    else if(authors.length >= range6)
      context.write(new Text("(More than 300 Authors)"), one)
    logger.info("Mapper execution completed")
  }
}

//Mapper to calculate authorship score of authors
class AuthorshipScoreMapper extends Mapper[LongWritable, Text, Text, DoubleWritable] {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val configuration = new Configuration
  val conf = ConfigFactory.load("Configuration")
  val one = new IntWritable(1)
  val dblpdtd = getClass.getClassLoader.getResource("dblp.dtd").toURI

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, DoubleWritable]#Context): Unit = {
    logger.info("Executing mapper to calculate authorship score of authors")
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dblpdtd">
              <dblp>""" + value.toString + "</dblp>"
    val authors = new Mappers().getElementFromXML(xmlString,"author").reverse //Extract list of authors from XML and arrange it in reverse order
    var prev = 0.0
    for(author <- authors){
      //Check if the current author is the first or primary author of the Article
      if(author == authors.last){
        context.write(new Text(author), new DoubleWritable(prev + 1.0/authors.length) )
      }
      else{
        //Calculating score for other authors
        context.write(new Text(author), new DoubleWritable((prev + 1.0/authors.length)*3.0/4) )
        prev = (prev + 1.0/authors.length)*1.0/4
      }

    }
    logger.info("Mapper execution completed")
  }
}

//Mapper to find the number of authors every individual authors have collaborated with
class AuthorCollaborationMapper extends Mapper[LongWritable, Text, Text, Text] {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val configuration = new Configuration
  val conf = ConfigFactory.load("Configuration")
  val one = new IntWritable(1)
  val dblpdtd = getClass.getClassLoader.getResource("dblp.dtd").toURI

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    logger.info("Executing mapper to calculate the number of authors every individual authors have collaborated with")
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dblpdtd">
              <dblp>""" + value.toString + "</dblp>"
    val authors = new Mappers().getElementFromXML(xmlString,"author") //Extracting list of authors from the XML

    //Generate combination for every author with every other author and write it to context
    for(author <- authors){
        for(coAuthor <- authors)
          if(author != coAuthor)
            context.write(new Text(author), new Text(coAuthor) )
    }
    logger.info("Mapper execution completed")
  }
}

class PublicationCountMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val configuration = new Configuration
  val conf = ConfigFactory.load("Configuration")
  val one = new IntWritable(1)
  val dblpdtd = getClass.getClassLoader.getResource("dblp.dtd").toURI

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dblpdtd">
              <dblp>""" + value.toString + "</dblp>"
    val article = new Mappers().getElementFromXML(xmlString,"article")

    //Check the parent element and write the corresponding element to context
    if(article.length > 0)
      context.write(new Text("article"), one )

    val inproceedings = new Mappers().getElementFromXML(xmlString,"inproceedings")
    if(inproceedings.length > 0)
      context.write(new Text("inproceedings"), one )

    val proceedings = new Mappers().getElementFromXML(xmlString,"proceedings")
    if(proceedings.length > 0)
      context.write(new Text("proceedings"), one )

    val book = new Mappers().getElementFromXML(xmlString,"book")
    if(book.length > 0)
      context.write(new Text("book"), one )

    val incollection = new Mappers().getElementFromXML(xmlString,"incollection")
    if(incollection.length > 0)
      context.write(new Text("incollection"), one )

    val phdthesis = new Mappers().getElementFromXML(xmlString,"phdthesis")
    if(phdthesis.length > 0)
      context.write(new Text("phdthesis"), one )

    val mastersthesis = new Mappers().getElementFromXML(xmlString,"mastersthesis")
    if(mastersthesis.length > 0)
      context.write(new Text("mastersthesis"), one )

    val www = new Mappers().getElementFromXML(xmlString,"www")
    if(www.length > 0)
      context.write(new Text("www"),one )

    val person = new Mappers().getElementFromXML(xmlString,"person")
    if(person.length > 0)
      context.write(new Text("person"), one )

    val data = new Mappers().getElementFromXML(xmlString,"data")
    if(data.length > 0)
      context.write(new Text("data"), one )

    logger.info("Mapper execution completed")
  }
}

//Mapper to calculate Mean, Median and Max of the number of co-authors of each individual authors
class MinMedianMaxMapper extends Mapper[LongWritable, Text, Text, IntWritable] {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val configuration = new Configuration
  val conf = ConfigFactory.load("Configuration")
  val one = new IntWritable(1)
  val dblpdtd = getClass.getClassLoader.getResource("dblp.dtd").toURI

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {
    logger.info("Executing Mapper to calculate Mean, Median and Max of the number of co-authors of each individual authors")
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dblpdtd">
              <dblp>""" + value.toString + "</dblp>"
    val authors = new Mappers().getElementFromXML(xmlString,"author")

    //For every author, we calculate the number of co-authors that he/she has worked with in a particular paper
    logger.info("Reducer sent this value: " + authors + " length: " + authors.length)
    for (author <- authors) context.write(new Text(author), new IntWritable(authors.length-1))
    logger.info("Mapper execution completed")
  }
}

