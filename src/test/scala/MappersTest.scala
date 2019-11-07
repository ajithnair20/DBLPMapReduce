import mapreduce.Mappers
import org.apache.hadoop.io.{DoubleWritable, Text}
import org.scalatest.FunSuite
import scala.collection.mutable.HashMap


//test cases to check functionality of mapper classes
class MappersTest extends FunSuite {

  //Test to check getElementFromXML method of Mappers class
  test("Test getElementFromXML method in Mappers class"){

    val dblpdtd = getClass.getClassLoader.getResource("dblp.dtd").toURI
    val input = "<inproceedings mdate=\"2017-05-24\" key=\"conf/icst/GrechanikHB13\">\n<author>Mark Grechanik</author>\n<author>B. M. Mainul Hossain</author>\n<author>Ugo Buy</author>\n<title>Testing Database-Centric Applications for Causes of Database Deadlocks.</title>\n<pages>174-183</pages>\n<year>2013</year>\n<booktitle>ICST</booktitle>\n<ee>https://doi.org/10.1109/ICST.2013.19</ee>\n<ee>http://doi.ieeecomputersociety.org/10.1109/ICST.2013.19</ee>\n<crossref>conf/icst/2013</crossref>\n<url>db/conf/icst/icst2013.html#GrechanikHB13</url>\n</inproceedings>"
    val xmlString =
      s"""<?xml version="1.0" encoding="ISO-8859-1"?>
              <!DOCTYPE dblp SYSTEM "$dblpdtd">
              <dblp>""" + input + "</dblp>"

    val authors = new Mappers().getElementFromXML(xmlString,"author")

    assert(authors.length == 3)
  }

  test("Check Authorship score"){

    val authors = List("Mark Grechanik","Ugo Buy").reverse
    val authorScore = new HashMap[String, Double]
    var prev = 0.0
    for(author <- authors){
      //Check if the current author is the first or primary author of the Article
      if(author == authors.last){
        authorScore(author) = prev + 1.0/authors.length
      }
      else{
        //Calculating score for other authors
        authorScore(author) = (prev + 1.0/authors.length)*3.0/4
        prev = (prev + 1.0/authors.length)*1.0/4
      }
    }
    assert(authorScore("Mark Grechanik") == 0.625)
    assert(authorScore("Ugo Buy") == 0.375)
  }
}

