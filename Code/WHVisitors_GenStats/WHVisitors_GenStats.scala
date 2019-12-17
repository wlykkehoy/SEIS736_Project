
/**
 * Big Data Architecture - SEIS736
 * Project
 * Wade Lykkehoy
 * Student ID: 101217575
 * lykk3260@stthomas.edu
 */


import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.storage.StorageLevel


object WHVisitors_GenStats {


  def main(args: Array[String]) {
   
    if (args.length != 2) {
      printf("\nUsage: WHVisitors_GenStats <cleansed log dir> <output dir>\n")
      System.exit(1)
    }
    val cleansedLogDir = args(0)
    val outputDir = args(1)
    printf("\nExecution args:\n")
    printf("  Cleansed log directory: %s\n", cleansedLogDir)
    printf("  Output directory      : %s\n\n", outputDir)


    val sparkConf = new SparkConf()
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)

    
    //
    // Load the data which was saved from the cleanse part, persist
    // it as we will be using it quite a lot.  Dump a record count 
    // and a few records just to confirm we have what we expect
    //  

    val theData = sc.textFile(cleansedLogDir)
      .map(line => line.split(",").map(_.trim))
 
    theData.persist(StorageLevel.MEMORY_AND_DISK)  
          
    printf("\nRecord count: %d\n", theData.count())
    printf("\nA few records:\n\n")
    theData.take(20).foreach(rec => println(rec.mkString(",")))
    
    printf("\ntheData.paritions.size: %d\n", theData.partitions.size)
    
    //
    // Produce a frequency for the reasons for visiting (in the Description field) and write to a file
    //        
    // Note the sort is sorting descending on the value and ascending on the key
    //

    val saveDir_fd = outputDir + "/freq_Description"
    theData.map(rec => (rec(6), 1))
      .reduceByKey((x,y) => x + y, 1)
      .sortBy(kvpair => ((0 - kvpair._2), kvpair._1))
      .saveAsTextFile(saveDir_fd)
    printf("\nFrequency of Description saved in dir %s\n", saveDir_fd)

    //
    // Produce a frequency for the individuals visited and write to a file
    //    
    // Note I am formatting the key as "[ln, fn]" to make it easier to visually see which
    // is the first name vs last name, and when either is blank
    //
    // Note the sort is sorting descending on the value and ascending on the key
    //

    val saveDir_fv = outputDir + "/freq_visitee"
    theData.map(rec => ("[" + rec(4) + ", " + rec(5) + "]", 1))
      .reduceByKey((x,y) => x + y, 1)
      .sortBy(kvpair => ((0 - kvpair._2), kvpair._1))
      .saveAsTextFile(saveDir_fv)
    printf("\nFrequency of Description saved in dir %s\n", saveDir_fv)


    //
    // Get a count of number of visitors visiting the President and write to a file
    //
    // Note we need to loook for "potus" in either the visitee first name or visitee last name
    //

    val saveDir_cp = outputDir + "/count_POTUS"
    val count_cp = theData.filter(rec => (rec(4) == "potus") || (rec(5) == "potus")).count()
    sc.parallelize(Array(("POTUS", count_cp)), 1).saveAsTextFile(saveDir_cp)
    printf("\nCount of POTUS visitors saved in dir %s\n", saveDir_cp)


    //
    // Produce a frequency for the individuals visiting the President and write to a file
    //
    // Note I am formatting the key as "[ln, fn]" to make it easier to visually see which
    // is the first name vs last name, and when either is blank
    //
    // Note the sort is sorting descending on the value and ascending on the key
    //

    val saveDir_fpv = outputDir + "/freq_POTUS_visitors"
    theData.filter(rec => (rec(4) == "potus") || (rec(5) == "potus"))
      .map(rec => ("[" + rec(0) + ", " + rec(1) + "]", 1))
      .reduceByKey((x,y) => x + y, 1)
      .sortBy(kvpair => ((0 - kvpair._2), kvpair._1))
      .saveAsTextFile(saveDir_fpv)
    printf("\nFrequency of POTUS visitors saved in dir %s\n", saveDir_fpv)


    //
    // Get a count of number of visitors visiting the First Lady and write to a file
    //
    // Note we need to loook for "flotus" in either the visitee first name or visitee last name
    //

    val saveDir_cf = outputDir + "/count_FLOTUS"
    val count_cf = theData.filter(rec => (rec(4) == "flotus") || (rec(5) == "flotus")).count()
    sc.parallelize(Array(("FLOTUS", count_cf)), 1).saveAsTextFile(saveDir_cf)
    printf("\nCount of FLOTUS visitors saved in dir %s\n", saveDir_cf)


    //
    // Produce a frequency for the individuals visiting the First Lady and write to a file
    //
    // Note I am formatting the key as "[ln, fn]" to make it easier to visually see which
    // is the first name vs last name, and when either is blank
    //
    // Note the sort is sorting descending on the value and ascending on the key
    //

    val saveDir_ffv = outputDir + "/freq_FLOTUS_visitors"
    theData.filter(rec => (rec(4) == "flotus") || (rec(5) == "flotus"))
      .map(rec => ("[" + rec(0) + ", " + rec(1) + "]", 1))
      .reduceByKey((x,y) => x + y, 1)
      .sortBy(kvpair => ((0 - kvpair._2), kvpair._1))
      .saveAsTextFile(saveDir_ffv)
    printf("\nFrequency of FLOTUS visitors saved in dir %s\n", saveDir_ffv)

    
    //
    // The description field typically contains the reason for the visit.  Let's
    // extract records with "tour" somewhere in the description for further analysis.
    // We'll persist it as we will be using it in several following steps.
    //

    val tourVisitors = theData.filter(rec => rec(6).contains("tour"))

    tourVisitors.persist(StorageLevel.MEMORY_AND_DISK)

    //
    // Now determine percent of tour visitors and write to a file
    //

    val saveDir_pvot = outputDir + "/pct_visitors_on_tour"
    val pvot = (tourVisitors.count.toFloat / theData.count.toFloat) * 100
    sc.parallelize(Array(("Percent of visitors on a tour", pvot)), 1).saveAsTextFile(saveDir_pvot)
    printf("\nPercent of visitors on tour saved in dir %s\n", saveDir_pvot)

    
    //
    // Produce a frequency for individuals on a tour to see if it looks like there are
    // repeat visitors.
    //
    // Note I am formatting the key as "[ln, fn]" to make it easier to visually see which
    // is the first name vs last name, and when either is blank
    //
    // Note the sort is sorting descending on the value and ascending on the key
    //

    val saveDir_fvot = outputDir + "/freq_visitors_on_tour"
    tourVisitors.map(rec => ("[" + rec(0) + ", " + rec(1) + "]", 1))
      .reduceByKey((x,y) => x + y, 1)
      .sortBy(kvpair => ((0 - kvpair._2), kvpair._1))
      .saveAsTextFile(saveDir_fvot)
    printf("\nFrequency of visitors on a tour saved in dir %s\n", saveDir_fvot)


    //
    // Produce a frequency on tour visitors by year and month and write to a file
    //
    // Note I am formatting the key as "YYYY_MM".  During the data cleansing, we made 
    // sure there is something that looks like a date in the form mm/dd/yyyy; 
    // 0/0/000 if no date was present
    //

    val saveDir_fvotbym = outputDir + "/freq_visitors_on_tour_by_year_month"
    tourVisitors.map(rec => (rec(3).split("/")(2) + "_" + rec(3).split("/")(0), 1))
      .reduceByKey((x,y) => x + y, 1)
      .sortByKey()
      .saveAsTextFile(saveDir_fvotbym)
    printf("\nFrequency of visitors on a tour saved in dir %s\n", saveDir_fvotbym)



    //
    // Produce a frequency by season; Spring, Summer, Fall, Winter and write to a file
    //

    // A helper function to determine the season based on the month
    def season(visitDate: String) = {
      // Note during data cleansing, we made sure there is something that looks like a date in 
      // the form mm/dd/yyyy; 0/0/000 if no date was present
      var season = "<unknown>"
      val month = visitDate.split("/")(0).toInt
      if ((month == 3) || (month == 4) || (month == 5)) {
        season = "Spring"
      } else if ((month == 6) || (month == 7) || (month == 8)) {
        season = "Summer"
      } else if ((month == 9) || (month == 10) || (month == 11)) {
        season = "Fall"
      } else if ((month == 12) || (month == 1) || (month == 2)) {
        season = "Winter"
      } 
      season
    }

    val saveDir_fvotbs = outputDir + "/freq_visitors_on_tour_by_season"
    tourVisitors.map(rec => (season(rec(3)), 1))
      .reduceByKey((x,y) => x + y, 1)
      .sortByKey()
      .saveAsTextFile(saveDir_fvotbs)
    printf("\nFrequency of visitors on a tour by season saved in dir %s\n", saveDir_fvotbs)

    
    // Not sure if this is really necessary, but always seems like a good 
    // idea to clean up after ourselves
    theData.unpersist()
    tourVisitors.unpersist()
    
    // Let the world know we have completed!
    printf("\nGenStats process completed!\n\n")

    System.exit(0)
  }
}
