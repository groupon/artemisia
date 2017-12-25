package com.groupon.artemisia.task.hadoop.hive

import com.groupon.artemisia.TestSpec
import org.apache.hadoop.io.IOUtils.NullOutputStream

/**
  * Created by chlr on 12/14/17.
  */
class   BeelineParsersSpec extends TestSpec {

  "BeelineExecuteParser" must "parse the log file for HQLExecute" in {
    val data =
      """
        |INFO  : Partition chlr_db.parquet_test_1 {event_date=2017-04-18, country=AE} stats: [numFiles=4, numRows=172932, totalSize=70740496, rawDataSize=70567564]
        |INFO  : Partition chlr_db.parquet_test_1 {event_date=2017-04-18, country=AU} stats: [numFiles=4, numRows=522732, totalSize=219728484, rawDataSize=219205752]
        |INFO  : Partition chlr_db.parquet_test_1 {event_date=2017-04-18, country=BE} stats: [numFiles=4, numRows=434092, totalSize=178470980, rawDataSize=178036888]
        |INFO  : Partition chlr_db.parquet_test_1 {event_date=2017-04-18, country=CA} stats: [numFiles=4, numRows=174144, totalSize=73048064, rawDataSize=72873920]
        |INFO  : Partition chlr_db.parquet_test_1 {event_date=2017-04-18, country=DE} stats: [numFiles=4, numRows=1590544, totalSize=647702220, rawDataSize=646111676]
        |INFO  : Partition chlr_db.parquet_test_1 {event_date=2017-04-18, country=ES} stats: [numFiles=4, numRows=1015272, totalSize=421611796, rawDataSize=420596524]
        |INFO  : Partition chlr_db.parquet_test_1 {event_date=2017-04-18, country=FR} stats: [numFiles=8, numRows=2572136, totalSize=1069221476, rawDataSize=1066649340]
        |INFO  : Partition chlr_db.parquet_test_1 {event_date=2017-04-18, country=IE} stats: [numFiles=4, numRows=211792, totalSize=89958488, rawDataSize=89746696]
        |INFO  : Partition chlr_db.parquet_test_1 {event_date=2017-04-18, country=IT} stats: [numFiles=8, numRows=2508388, totalSize=1045665944, rawDataSize=1043157556]
        |INFO  : Partition chlr_db.parquet_test_1 {event_date=2017-04-18, country=JP} stats: [numFiles=4, numRows=361076, totalSize=149681496, rawDataSize=149320420]
        |INFO  : Partition chlr_db.parquet_test_1 {event_date=2017-04-18, country=NL} stats: [numFiles=4, numRows=523584, totalSize=216439612, rawDataSize=215916028]
        |INFO  : Partition chlr_db.parquet_test_1 {event_date=2017-04-18, country=NZ} stats: [numFiles=4, numRows=47292, totalSize=20121880, rawDataSize=20074588]
        |INFO  : Partition chlr_db.parquet_test_1 {event_date=2017-04-18, country=PL} stats: [numFiles=4, numRows=356864, totalSize=141994632, rawDataSize=141637768]
      """.stripMargin
    val parser = new BeeLineExecuteParser(new NullOutputStream())
    parser.write(data.getBytes)
    parser.flush()
    parser.getData.getInt("chlr_db.parquet_test_1") must be (10490848)
  }

  "BeelineReadParser" must "parse the log file for HQLRead" in {
    val data =
      """
        |+-----------+--+
        ||    cnt    |
        |+-----------+--+
        || 30636492  |
        |+-----------+--+
      """.stripMargin
    val parser = new BeeLineReadParser(new NullOutputStream())
    parser.write(data.getBytes)
    parser.getData.getInt("cnt") must be (30636492)
  }



}
