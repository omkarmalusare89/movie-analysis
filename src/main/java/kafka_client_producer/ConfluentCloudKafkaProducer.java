package kafka_client_producer;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class ConfluentCloudKafkaProducer {

	public static void main(String[] args) throws Exception {
		Properties properties = new Properties();
		try (FileInputStream fis = new FileInputStream("src/main/resources/client.properties")) {
			properties.load(fis);
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
		properties.put("basic.auth.credentials.source", "USER_INFO"); 		
		KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(properties);

		String topic = "netflix.activity.movies";        
		String csvFile = "confluent-dataset/vodclickstream_uk_movies_03.csv";
		String AVRO_SCHEMA_FILE = "src/main/resources/clickstream_schema.avsc";

		Schema.Parser parser = new Schema.Parser();
		Schema schema = parser.parse(new java.io.File(AVRO_SCHEMA_FILE));
		GenericRecord record = new GenericData.Record(schema);

		try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
			String[] nextLine;
			reader.skip(1);  // skip the header of csv file

			while ((nextLine = reader.readNext()) != null) {           	
				String datetimeString = nextLine[1];  
				long datetimestampMillis = parseDateTimestamp(datetimeString);
				String release_dateString = nextLine[5]; 
				int release_date = parseDate(release_dateString);
				record.put("row_id", Long.parseLong(nextLine[0]));
				record.put("datetime",  datetimestampMillis);
				record.put("duration", Double.parseDouble(nextLine[2]));
				record.put("title", nextLine[3]);
				record.put("genres", nextLine[4]);
				record.put("release_date", release_date);
				record.put("movie_id", nextLine[6]);
				record.put("user_id", nextLine[7]);

				ProducerRecord<String,GenericRecord> producerRecord = new ProducerRecord<>(topic, String.valueOf(record.get("movie_id")), record);

				producer.send(producerRecord, new Callback() {
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if (exception != null) {
							exception.printStackTrace();
						} else {
							System.out.println("Sent record to partition " + metadata.partition() +
									" with offset " + metadata.offset());
						}
					}
				});
			}
		} catch (IOException | CsvValidationException e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}

	private static long parseDateTimestamp(String timestampString) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date;
		try {
			date = sdf.parse(timestampString);
		} catch (ParseException e) {
			e.printStackTrace();
			return 0;
		}
		return date.getTime();
	}

	private static int parseDate(String timestampString)  {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");       
		Date date;
		try {
			date = sdf.parse(timestampString);
		} catch (ParseException e) {
			e.printStackTrace();
			return 0;
		}
		return date.getDate();
	}
}
