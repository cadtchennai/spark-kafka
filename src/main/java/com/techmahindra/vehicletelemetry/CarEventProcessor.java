package com.techmahindra.vehicletelemetry;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.gson.Gson;
import com.techmahindra.vehicletelemetry.service.MaintenanceAnalyzerService;
import com.techmahindra.vehicletelemetry.vo.CarEvent;

import scala.Tuple2;

public class CarEventProcessor {
	
	private Logger logger = Logger.getLogger("CarEventProcessor");
	
	public CarEventProcessor(String topic) throws IOException, AnalysisException {
		// Get properties
		Properties props = new Properties();
		props.load(CarEventProcessor.class.getClassLoader().getResourceAsStream("vt.properties"));
		
		// Initialize
		int numThreads = Integer.parseInt(props.getProperty("KAFKA_THREAD_COUNT"));
		int batchDuration = Integer.parseInt(props.getProperty("KAFKA_BATCH_DURATION"));
		String zkQuorum = props.getProperty("ZK_QUORUM");
		String kafkaGroup = props.getProperty("KAFKA_GROUP");
		
		// Instantiate streaming context & spark session
		SparkConf sparkConf = new SparkConf().setAppName("CarEventsProcessor");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(batchDuration));
		jssc.checkpoint(".\\checkpoint");
		SparkSession spark = JavaSparkSessionSingleton.getInstance(sparkConf);
		Properties connProps = new Properties();
		connProps.put("user", props.getProperty("JDBC_USER"));
		connProps.put("password", props.getProperty("JDBC_PASSWORD"));
		connProps.put("driver", props.getProperty("JDBC_DRIVER"));
		
		// Prepare topic Map
		Map<String, Integer> topicMap = new HashMap<>();
		topicMap.put(topic, numThreads);
		
		// Get events history and register as temp view
		Dataset<Row> dbDF = spark.read().jdbc(props.getProperty("JDBC_CONN_STRING"), 
											props.getProperty("JDBC_TABLE"), connProps);
		dbDF.createTempView("events_history");
		
		// Process streaming events
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, zkQuorum, kafkaGroup, topicMap);
		JavaDStream<CarEvent> events = messages.map(new Function<Tuple2<String, String>, CarEvent>() {
			private static final long serialVersionUID = 1L;
			@Override
		    public CarEvent call(Tuple2<String, String> tuple2) {
				Gson gson = new Gson();
				return gson.fromJson(tuple2._2, CarEvent.class);
	    	}
	    });
		
		events.foreachRDD(new VoidFunction<JavaRDD<CarEvent>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void call(JavaRDD<CarEvent> eventRDD) throws Exception {
				if(eventRDD.count() > 0) {
					Dataset<Row> streamDF = spark.createDataFrame(eventRDD, CarEvent.class);
					if(streamDF != null && streamDF.count() > 0) {
						streamDF.createOrReplaceTempView("events_stream");
					}
					String sql = "SELECT history.vin as vin,history.city as city,history.model as model,avg(history.outsideTemp) as avg_outtemp,avg(history.engineTemp) as avg_enginetemp "
							+ "FROM events_history as history,events_stream as stream "
							+ "WHERE history.vin = stream.vin GROUP BY history.vin,history.city,history.model";
					Dataset<Row> avgTempData = spark.sql(sql);
					MaintenanceAnalyzerService mas = new MaintenanceAnalyzerService();
					mas.process(avgTempData);
					logger.log(Level.INFO, "Processed " + eventRDD.count() + " events.");
				}
				else	logger.log(Level.INFO, "No events to process!!!");
			}
		});
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			jssc.stop();
			Thread.currentThread().interrupt();
		}
	}
	
	public static void main(String[] args) throws IOException, AnalysisException {
		if (args.length < 1) {
	      System.exit(1);
	    }
		new CarEventProcessor(args[0]);
	}
}

/** Lazily instantiated singleton instance of SparkSession */
class JavaSparkSessionSingleton implements Serializable {
  private static final long serialVersionUID = 1L;
  private static transient SparkSession instance = null;
  private JavaSparkSessionSingleton(){}
  public static SparkSession getInstance(SparkConf sparkConf) {
    if (instance == null) {
      instance = SparkSession
        .builder()
        .config(sparkConf)
        .getOrCreate();
    }
    return instance;
  }
}