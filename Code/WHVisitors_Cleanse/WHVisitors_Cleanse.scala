
/**
 * Big Data Architecture - SEIS736
 * Project
 * Wade Lykkehoy
 * Student ID: 101217575
 * lykk3260@stthomas.edu
 */


import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.storage.StorageLevel


object WHVisitors_Cleanse {
  

  // A helper function for cleaning up dates.  Does the following:
  // - Sometimes we have a date and time; drop the "time" part
  // - Make sure dates have all 3 parts; mm/dd/yyyy
  // - Make all months two characters by prepending "0" if needed
  // - Make all days two characters by prepending "0" if needed
  // - Make all years four characters by prepending "20" if needed (lucky,
  //     all of Obama's presidency was year 2000 and beyond)
  def sanatizeDate(dateTimeStamp: String) = {
    val dateOnly =  dateTimeStamp.split(" ")(0)
    val dateParts = dateOnly.split("/")
    var month = "00"
    var day = "00"
    var year = "0000"
    if (dateParts.length == 3) {             // Have something in format x/y/z; a good start...
      dateParts(0).length match {
        case 1 => month = "0" + dateParts(0)
        case 2 => month = dateParts(0)
        case _ => ;
      }
      dateParts(1).length match {
        case 1 => day = "0" + dateParts(1)
        case 2 => day = dateParts(1)
        case _ => ;
       }
      dateParts(2).length match {
        case 2 => year = "20" + dateParts(2)
        case 4 => year = dateParts(2)
        case _ => ;
      }
    }
    month + "/" + day + "/" + year  
  }


  def main(args: Array[String]) {
     
    if (args.length != 3) {
      printf("\nUsage: WHVisitors_GenStats <cleansed log dir> <output dir>\n")
      System.exit(1)
    }
    val rawLogDir = args(0)
    val cleansedLogDir = args(1)
    val outputDir = args(2)
    printf("\nExecution args:\n")
    printf("  Raw log directory     : %s\n", rawLogDir)
    printf("  Cleansed log directory: %s\n", cleansedLogDir)
    printf("  Output directory      : %s\n\n", outputDir)
    

    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)


    //
    // Part 1 - Loading and cleansing the data
    //    
    // Load the raw log data, split, drop incomplete records, drop header lines,
    // drop fields with either NAMELAST or NAMEFIRST blank, and yank out
    // only the fields that we need
    //
    // Dump a before and after count and a few records
    //
    
    val rawData = sc.textFile(rawLogDir)
    val rawRecordCount = rawData.count

    val cleansedData = rawData.map(line => line.toLowerCase.split(",").map(_.trim))
        .filter(rec => rec.length == 28)
        .filter(rec => rec(0) != "namelast")
        .filter(rec => (rec(0).length != 0))
        .filter(rec => (rec(1).length != 0))
        .map(rec => Array(rec(0), rec(1), rec(2), sanatizeDate(rec(11)), rec(19), rec(20), rec(26)))
    val cleansedRecordCount = cleansedData.count

    printf("\nRaw record count      : %d\n", rawRecordCount)
    printf("Cleansed record count : %d\n", cleansedRecordCount)
    printf("\nA few records:\n\n")
    cleansedData.take(20).foreach(rec => println(rec.mkString(",")))
    

    
    //
    // Part 2 - Generate some frequencies to help assess if the data makes sense
    //  

    // Produce a frequency of the length of the NAMELAST value and write out to both stdout and a file

    val saveDir_flnl = outputDir + "/freq_len_NAMELAST"
    val freq_lnl = cleansedData.map(rec => (rec(0).length, 1))
      .reduceByKey((x,y) => x + y, 1)
      .sortByKey()
    printf("\nFrequency by NAMELAST length (also saved in dir %s):\n\n", saveDir_flnl)
    printf("length\tcount\n")
    freq_lnl.foreach(rec => println(rec._1 + "\t" + rec._2))
    freq_lnl.saveAsTextFile(saveDir_flnl)


    // Produce a frequency of the length of the NAMEFIRST value and write out to both stdout and a file

    val saveDir_flnf = outputDir + "/freq_len_NAMEFIRST"
    val freq_lnf = cleansedData.map(rec => (rec(1).length, 1))
      .reduceByKey((x,y) => x + y, 1)
      .sortByKey()
    printf("\nFrequency by NAMEFIRST length (also saved in dir %s):\n\n", saveDir_flnf)
    printf("length\tcount\n")
    freq_lnf.foreach(rec => println(rec._1 + "\t" + rec._2))
    freq_lnf.saveAsTextFile(saveDir_flnf)


    // Produce a frequency of the length of the visitee_namelast value and write out to both stdout and a file

    val saveDir_flvnl = outputDir + "/freq_len_visitee_namelast"
    val freq_lvnl = cleansedData.map(rec => (rec(4).length, 1))
      .reduceByKey((x,y) => x + y, 1)
      .sortByKey()
    printf("\nFrequency by visitee_namelast length (also saved in dir %s):\n\n", saveDir_flvnl)
    printf("length\tcount\n")
    freq_lvnl.foreach(rec => println(rec._1 + "\t" + rec._2))
    freq_lvnl.saveAsTextFile(saveDir_flvnl)


    // Produce a frequency of the length of the visitee_namefirst value and write out to both stdout and a file

    val saveDir_flvnf = outputDir + "/freq_len_visitee_namefirst"
    val freq_lvnf = cleansedData.map(rec => (rec(5).length, 1))
      .reduceByKey((x,y) => x + y, 1)
      .sortByKey()
    printf("\nFrequency by visitee_namefirst length (also saved in dir %s):\n\n", saveDir_flvnf)
    printf("length\tcount\n")
    freq_lvnf.foreach(rec => println(rec._1 + "\t" + rec._2))
    freq_lvnf.saveAsTextFile(saveDir_flvnf)


    // Produce a frequency of the length of the Description value and write out to both stdout and a file

    val saveDir_fld = outputDir + "/freq_len_Description"
    val freq_ld = cleansedData.map(rec => (rec(6).length, 1))
      .reduceByKey((x,y) => x + y, 1)
      .sortByKey()
    printf("\nFrequency by Description length (also saved in dir %s):\n\n", saveDir_fld)
    printf("length\tcount\n")
    freq_ld.foreach(rec => println(rec._1 + "\t" + rec._2))
    freq_ld.saveAsTextFile(saveDir_fld)


    //
    // Part 3 - Save the cleansed data as a comma delimited file
    //
    // Note: When I used just a comma separator, I ran into issues when I re-loaded the data
    // and split it.  Trailing empty fields were ignored (although the comma separator was
    // present), resulting in records missing trailing fields.  This broke indexing the fields
    // within the record (e.g. rec(0), .., rec(6)).  I found using a comma and space (", ") 
    // corrected this issue although when re-loading I do have to trim the fields.
    //

    cleansedData.map(rec => rec.mkString(", ")).saveAsTextFile(cleansedLogDir)

    
    // Let the world know we have completed!
    printf("\nCleanse process completed!\n\n")

    
    System.exit(0)
  }    
    
}