package mapreduce

import com.typesafe.config.ConfigFactory
import mapreduceinputformat.XmlInputFormatWithMultipleTags
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.{Logger, LoggerFactory}

object MapReduceDBLP {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val configuration = new Configuration
    val conf = ConfigFactory.load("Configuration")
    configuration.set("mapred.textoutputformat.separator", ",")
    //loading start tags
    configuration.set("xmlinput.start", conf.getString("START_TAGS"))

    //loading end_tags
    configuration.set("xmlinput.end", conf.getString("END_TAGS"))
    configuration.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");


    //Job to find count of publications of each author
    logger.info("Starting first job to calculate count of authors");
    {
      val job1 = Job.getInstance(configuration, "Author Count")
      job1.setJarByClass(this.getClass)
      //Setting mapper
      job1.setMapperClass(classOf[AuthorCountMapper])
      job1.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
      job1.setCombinerClass(classOf[Reducers])

      //setting reducer
      job1.setReducerClass(classOf[Reducers])
      job1.setOutputKeyClass(classOf[Text])
      job1.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(job1, new Path(args(0)))
      FileOutputFormat.setOutputPath(job1, new Path(args(1) + conf.getString("AuthorCountOutputPath")))
      job1.waitForCompletion(true)
    }

    //Job to find count of publications published each year
    logger.info("Starting job to calculate count of publications/articles/ thesis published each year");
    {
      val job2 = Job.getInstance(configuration, "Year Count")
      job2.setJarByClass(this.getClass)
      //Setting mapper
      job2.setMapperClass(classOf[YearCountMapper])
      job2.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
      job2.setCombinerClass(classOf[Reducers])

      //setting reducer
      job2.setReducerClass(classOf[Reducers])
      job2.setOutputKeyClass(classOf[Text])
      job2.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(job2, new Path(args(0)))
      FileOutputFormat.setOutputPath(job2, new Path(args(1) + conf.getString("YearCountOutputPath")))
      job2.waitForCompletion(true)
    }

    logger.info("Starting job to find range of papers based on the number of co-oauthors");
    //Job to populate histogram of co-author range
    {
      val job3 = Job.getInstance(configuration, "Co-author range")
      job3.setJarByClass(this.getClass)
      //Setting mapper
      job3.setMapperClass(classOf[CoAuthorRangeMapper])
      job3.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
      job3.setCombinerClass(classOf[Reducers])

      //setting reducer
      job3.setReducerClass(classOf[Reducers])
      job3.setOutputKeyClass(classOf[Text])
      job3.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(job3, new Path(args(0)))
      FileOutputFormat.setOutputPath(job3, new Path(args(1) + conf.getString("CoAuthorRangeOutputPath")))
      job3.waitForCompletion(true)
    }

    logger.info("Starting job to calculate authorship score");
    //Job to calculate authorship score
    {
      val job4 = Job.getInstance(configuration, "Authorship course")
      job4.setJarByClass(this.getClass)
      //Setting mapper
      job4.setMapperClass(classOf[AuthorshipScoreMapper])
      job4.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
      //job3.setCombinerClass(classOf[AuthorshipScoreReducer])

      //setting reducer
      job4.setReducerClass(classOf[AuthorshipScoreReducer])
      job4.setOutputKeyClass(classOf[Text])
      job4.setOutputValueClass(classOf[DoubleWritable])
      FileInputFormat.addInputPath(job4, new Path(args(0)))
      FileOutputFormat.setOutputPath(job4, new Path(args(1) + conf.getString("AuthorshipScoreOutputPath")))
      job4.waitForCompletion(true)
    }

    logger.info("Starting job to find number of unique collaborations of each author with other authors");
    //Job to calculate count of co-authors/collaboration of authors
    {
      val job5 = Job.getInstance(configuration, "Author Collaboration")
      job5.setJarByClass(this.getClass)
      //Setting mapper
      job5.setMapperClass(classOf[AuthorCollaborationMapper])
      job5.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
      job5.setMapOutputKeyClass(classOf[Text])
      job5.setMapOutputValueClass(classOf[Text])
      //setting reducer
      job5.setReducerClass(classOf[AuthorCollaborationReducer])
      job5.setOutputKeyClass(classOf[Text])
      job5.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(job5, new Path(args(0)))
      FileOutputFormat.setOutputPath(job5, new Path(args(1) + conf.getString("AuthorCollaborationOutputPath")))
      job5.waitForCompletion(true)
    }

    logger.info("Starting job to find the count of different publications");
    //Job to count of type of publication
    {
      val job6 = Job.getInstance(configuration, "Type of Publications Count")
      job6.setJarByClass(this.getClass)
      //Setting mapper
      job6.setMapperClass(classOf[PublicationCountMapper])
      job6.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])
      //setting reducer
      job6.setReducerClass(classOf[Reducers])
      job6.setOutputKeyClass(classOf[Text])
      job6.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(job6, new Path(args(0)))
      FileOutputFormat.setOutputPath(job6, new Path(args(1) + conf.getString("PublicationTypeCountPath")))
      job6.waitForCompletion(true)
    }


    logger.info("Starting job to calculate mean median and max of the count of co-authors in a paper for every author");
    //Job to count the number of mean, max and median of count co-authors per paper for each author
    {
      val job7 = Job.getInstance(configuration, "Mean Median max")
      job7.setJarByClass(this.getClass)
      //Setting mapper
      job7.setMapperClass(classOf[MinMedianMaxMapper])
      job7.setInputFormatClass(classOf[XmlInputFormatWithMultipleTags])

      //setting reducer
      job7.setReducerClass(classOf[MeanMedianMaxReducer])
      job7.setOutputKeyClass(classOf[Text])
      job7.setOutputValueClass(classOf[IntWritable])
      FileInputFormat.addInputPath(job7, new Path(args(0)))
      FileOutputFormat.setOutputPath(job7, new Path(args(1) + conf.getString("MeanMedianMaxPath")))
      job7.waitForCompletion(true)
    }
  }
}
