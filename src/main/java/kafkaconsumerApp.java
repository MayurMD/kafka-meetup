
import java.util.ArrayList;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
//import io.confluent.kafka.serializers.


public class kafkaconsumerApp

{


    public static void main(String[] args) {


        Properties props=new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.deserializer","io.confluent.kafka.serializers.KafkaAvroDeserializer");
        // props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,"http://localhost:8081");
        props.put("group.id","demo-consumer-group");


        String venue_name;
        double latit,longit;

        KafkaConsumer consumer=new KafkaConsumer(props);



       // KafkaConsumer consumer=new KafkaConsumer(props);

        ArrayList<String> topiclist=new ArrayList<>();
        topiclist.add("venue");
        //topiclist.add("demo-new");
        consumer.subscribe(topiclist);


        try
        {

            while(true)
            {

                //ConsumerRecords<String,JsonNode> records=consumer.poll(10);
                ConsumerRecords<String, GenericData.Record> records=consumer.poll(10);

                for (ConsumerRecord record:records)
                {

                    System.out.println(record.value().getClass());
                    GenericData.Record output= (GenericData.Record)record.value();
                    venue_name=output.get("venue_name").toString();
                    latit=Double.parseDouble(output.get("venue_lat").toString());
                    longit=Double.parseDouble(output.get("venue_long").toString());


                    System.out.println("++++++++++++++++++++++++++++++++++++");
                    System.out.printf("venue name is %s venue lat is %f venue long is % f ",venue_name,latit,longit);
                    System.out.println(output.toString());
                    System.out.println("++++++++++++++++++++++++++++++++++++");
//                    System.out.println(record.value().getClass());
//                    JsonNode node=(JsonNode)record.value();
//                   // JsonNode venue=node.get("venue");
//                    System.out.println(node.get("venue").get("venue_name").toString());
                   // System.out.println(record.value());

                }

            }
        }catch(Exception e )
        {
            System.out.println(e.getMessage());
        } finally {
            consumer.close();
        }

    }
}
