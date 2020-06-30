package incepetzscalaOne.scalaprograms

import org.apache.spark._;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders._;
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._;

object hackathon1 {

  case class Incidents(incidentnum: String, category: String,
                       description: String, dayofweek: String, date: String,
                       time: String, pddistrict: String, resolution: String,
                       address: String, x: String, y: String, pdid: String);
  // String works with df.as[<case class>] and Decimal works only with Encoder.
  def main(args: Array[String]) {
    val ss = SparkSession.builder().appName("Hackathon").master("local[*]").getOrCreate();
    val sc = ss.sparkContext;
    val sqlc = ss.sqlContext;
    sc.setLogLevel("ERROR");
    
    import ss.implicits._;
    println("BEGIN - Hackathon1");
    val sfpd = ss.read.format("csv").load("hdfs://localhost:54310/user/hduser/sfpd.csv"); //.as[Incidents];
    println("File to DF");
    val sfpdDF = sfpd.toDF("incidentnum", "category", "description", "dayofweek", "date", 
                           "time", "pddistrict", "resolution", "address", "X", "Y", "pdid");
    
    val sfpdDS = sfpdDF.as[Incidents];
    sfpdDS.createOrReplaceTempView("sfpd_table");
    
    val sql = new SQLContext(sc);
    println("Top 5 Districts having highest no.of Incedents")
    val highIncidents = ss.sql("select pddistrict as District, count(1) No_of_Incidents from sfpd_table group by pddistrict order by 2 desc limit 5");
    highIncidents.show();
    println("Top 10 Resolution having highest no.of Incedents")
    val highResolution = ss.sql("select resolution , count(1) No_of_Incidents from sfpd_table group by resolution order by 2 desc limit 10");
    highResolution.show();
        
    val highCategory = ss.sql("select Category , count(1) No_of_Incidents from sfpd_table group by Category order by 2 desc limit 3");
    highCategory.show();
    
    highCategory.write.mode("overwrite").json("hdfs://localhost:54310/user/hduser/Sfpd_Json/Top_3_Category_Incident.Json");
    
    val warrants = ss.sql("Select * from sfpd_table where category = 'WARRANTS'");
    warrants.write.mode("overwrite").parquet("hdfs://localhost:54310/user/hduser/sfpd_parquet/Warants.parquet");
    
    ss.udf.register("Trunc_Year", (ip_dt:String)=> {ip_dt.substring(ip_dt.lastIndexOf{'/'}+1)});
    
    val IncidentYear = ss.sql("SELECT Trunc_Year(date) as Year, count(1) as No_of_Incidents from sfpd_table GROUP BY Trunc_Year(date) ORDER BY No_of_Incidents DESC")
    IncidentYear.show();
    
    val IncidentYear_2014 = ss.sql("SELECT category, address, resolution from sfpd_table where Trunc_Year(date) = '2014'");
    IncidentYear_2014.show();
    
    val IncidentYear_2015 = ss.sql("SELECT  address, resolution from sfpd_table where Trunc_Year(date) = '2015' and category = 'VANDALISM'");
    IncidentYear_2015.show();
  }
}
