
/**
 * Big Data Architecture - SEIS736
 * Project
 * Wade Lykkehoy
 * Student ID: 101217575
 * lykk3260@stthomas.edu
 */


import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.storage.StorageLevel



object WHVisitors_XrefContacts {

  def main(args: Array[String]) {
   
    if (args.length != 3) {
      printf("\nUsage: WHVisitors_XrefContacts <cleansed log dir> <contacts dir> <output dir>\n")
      System.exit(1)
    }
    val cleansedLogDir = args(0)
    val contactsDir    = args(1)
    val outputDir      = args(2)
    printf("\nExecution args:\n")
    printf("  Cleansed log directory: %s\n", cleansedLogDir)
    printf("  Contacts directory    : %s\n", contactsDir)
    printf("  Output directory      : %s\n", outputDir)
 
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)

    
    //
    // Load my contacts data, do a bit of cleansing, build a key/value RDD with elements of
    // the form "[lastname, firstname]",and dump a count plus a few records just to
    // confirm we have what we expect.  
    //
    // Cleansing:
    // - The number of fields in a record can vary based on contact data recorded; make 
    //   sure we have the first 3 (first name, mi, last name); drop those that do not.
    // - Drop the header record
    // - Drop records with no first name
    // - Drop records with no last name
    //

    val cleansedContacts = sc.textFile(contactsDir)
      .map(line => line.toLowerCase.split(",").map(_.trim))
      .filter(rec => rec.length > 3)
      .filter(rec => rec(0) != "first name")
      .filter(rec => rec(0).length != 0)
      .filter(rec => rec(2).length != 0)
      .map(rec => ("[" + rec(2) + ", " + rec(0) + "]"))

    printf("\nCleansed contact record count : %d\n", cleansedContacts.count)
    printf("\nA few records:\n\n")
    cleansedContacts.take(20).foreach(rec => println(rec))


    //
    // Load the data which was saved from the cleanse part, build a key/value RDD with elements of
    // the form "[lastname, firstname]" (same format as for the contacts), and dump a count plus 
    // a few records just to confirm we have what we expect.
    //

    val cleansedVisitors = sc.textFile(cleansedLogDir)
      .map(line => line.split(",").map(_.trim))
      .map(rec => ("[" + rec(0) + ", " + rec(1) + "]"))

    printf("\nCleansed visitor log record count: %d\n", cleansedVisitors.count())
    printf("\nA few records:\n\n")
    cleansedVisitors.take(20).foreach(rec => println(rec))
 

    // 
    // Now, let's intersect the two to see if there are any names in common, sort them, dump to stdout and
    // save to a file.
    //
    
    val matches = cleansedContacts.intersection(cleansedVisitors).sortBy(rec => rec, true, 1)

    printf("\nNumber of matches between my contacts and the visitor logs : %d\n", matches.count)
    
    if (matches.count > 0) {
      printf("\nAnd they are:\n\n")
      matches.foreach(rec => println(rec))
    }

    printf("\nList of matches saved in dir %s", outputDir)
    if (matches.count > 0) {
      matches.saveAsTextFile(outputDir)
    } else {
      sc.parallelize(Array(("No contacts were visitors to the WhiteHouse; bummer!!!")), 1).saveAsTextFile(outputDir)
    }

    
    // Let the world know we have completed!
    printf("\nXrefContacts process completed!\n\n")

    System.exit(0)
    
  } 
  
}