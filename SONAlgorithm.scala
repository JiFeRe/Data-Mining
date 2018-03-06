import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SONAlgorithm {


  def main(args: Array[String]): Unit = {
    val caseID = Integer.parseInt(args(0))
    val fileName = args(1)
    val support = Integer.parseInt(args(2))

    var outSupport = ""
    var outFileName = ""
    if (fileName.contains("small1.csv")){
      outFileName = "Jingfeng_Ren_SON_" + "Small1.case" + caseID + ".txt"
    }else if (fileName.contains("small2.csv")){
      outFileName = "Jingfeng_Ren_SON_" + "Small2.case" + caseID + ".txt"
    }else if (fileName.contains("beauty.csv")){
      outFileName = "Jingfeng_Ren_SON_" + "Beauty.case" + caseID +"-"+support+ ".txt"

    }else if (fileName.contains("books.csv")){
      outFileName = "Jingfeng_Ren_SON_" + "Books.case" + caseID +"-"+support+ ".txt"
    }else{
      outFileName = "test.txt"
    }



    val SONconf = new SparkConf()
      .setAppName("DatasetSON")
      .setMaster("local[2]")
    val scSON = new SparkContext(SONconf)

    val rddSON = scSON.textFile(fileName,2)
    val header = rddSON.first()
    val allBasckets = rddSON
      .filter(_ != header)
      .map(x => (x.split(",") {
        caseID - 1
      }, Set(x.split(",") {
        -(caseID - 2)
      })))
      .reduceByKey((a, b) => a ++ b)
      .map(x => x._2)


    val chunks = allBasckets.randomSplit(Array(1))


    var basketList: List[RDD[Set[String]]] = List()

    for (baskets <- chunks) {
      var freqtItems = baskets
        .flatMap(x => x)
        .map(x => (x, 1))
        .reduceByKey((a, b) => a + b)
        .filter(x => x._2 >= support)
        .map(x => Set(x._1))
      //val forReuducer = freqtItems.map(x=>(x,1))

      val freqItemsetsArray = freqtItems.collect()
      if (freqItemsetsArray.length > 0) {


        var possible_ItemSets = construct(freqItemsetsArray)
        while (possible_ItemSets.size > 0) {
          def freqFilter(line: Set[String]): Set[Set[String]] = {
            var ret: Set[Set[String]] = Set()
            for (x <- possible_ItemSets) {
              if (x.subsetOf(line)) {
                ret += x
              }
            }
            return ret
          }

          var next_ItemSets = baskets
            .flatMap(freqFilter)
            .map(x => (x, 1))
            .reduceByKey((a, b) => a + b)
            .filter(x => x._2 >= support)
            .map(x => x._1)

          freqtItems = freqtItems.union(next_ItemSets)

          val next_ItemSetsArray = next_ItemSets.collect()
          possible_ItemSets = construct(next_ItemSetsArray)

        }

        basketList ::= freqtItems
      }

    }


    var initCandidates = basketList(0)
    var i = 1
    while(i<basketList.size){
      initCandidates = initCandidates
        .union(basketList(i))
      i += 1
    }
    val allCandidates = initCandidates.distinct().collect()

    //    var allCandidates = basketList(0)
    //      .union(basketList(1))
    //      .distinct()
    //      .collect()


    def freqFilter(line: Set[String]): Set[Set[String]] = {
      var ret: Set[Set[String]] = Set()
      for (x <- allCandidates) {
        if (x.subsetOf(line)) {
          ret += x
        }
      }
      return ret
    }

    val trueItemSets = allBasckets
      .flatMap(freqFilter)
      .map(x => (x, 1))
      .reduceByKey((a, b) => a + b)
      .filter(x => x._2 >= support)
      .map(x => (x._1.size, x._1.toList.sorted))
      .sortBy(x => (x._1))

    val itemlengths = trueItemSets.keys.max()


    val output = new PrintWriter(new File(outFileName))
    for (eachKey <- 1 to itemlengths) {

      val eachItemSets = trueItemSets
        .filter(x => x._1 == eachKey)
        .values
        .map(x => x.mkString("(\'", "\', \'", "\')"))
        .sortBy(x => x)
        .collect()

      output.write(eachItemSets.mkString(", ") + "\n\n")

    }

    //      val orderedItemSets = trueItemSets
    //      .values
    //      .map(x=>x.mkString("(",",",")"))
    //      .collect()


    //output.write(orderedItemSets.mkString(","))
    output.close()


  }

  //construct possible itemSets base on previous frequent itemSets
  def construct(itemsets: Array[Set[String]]): Set[Set[String]] = {
    val set_len = itemsets(0).size
    var ret: Set[Set[String]] = Set()
    for (x <- itemsets) {
      for (y <- itemsets) {

        val possible_set = x ++ y
        if (possible_set.size == set_len + 1) {
          var count = 0
          for (subset <- itemsets) {
            if (subset.subsetOf(possible_set)) {
              count += 1
            }
          }
          if (count == set_len + 1) {
            ret += possible_set
          }
        }

      }

    }
    return ret
  }

}
