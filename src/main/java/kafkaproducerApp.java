import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;





public class kafkaproducerApp {





    public static void main(String[] args) {



        Properties properties=new Properties();
        //properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
       properties.put("bootstrap.servers","localhost:9092");
       //properties.put("key.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("key.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");
       properties.put("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");
       properties.put("schema.registry.url", "http://localhost:8081");



       // properties.put("value.serializer","org.apache.kafka.connect.json.JsonSerializer");
        //properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, GenericRecord> producer=new KafkaProducer<>(properties);


        //define the avro schema to be used

        String rsvp_schema="{\"type\": \"record\"," +
                "\"name\": \"rsvp\"," +
                "\"fields\": [" +
                "{\"name\": \"venue_name\", \"type\": \"string\"}," +
                "{\"name\": \"venue_lat\",\"type\": \"double\"}," +
                "{\"name\": \"venue_long\",\"type\": \"double\"},"+
                "{\"name\": \"rsvp\",\"type\": \"boolean\"},"+
                "{\"name\": \"event_time\",\"type\": \"long\"},"+
                "{\"name\": \"guests\",\"type\": \"int\"},"+
                "{\"name\": \"mtime\",\"type\": \"long\"},"+
                "{\"name\": \"group_city\",\"type\": \"string\"},"+
                "{\"name\": \"group_country\",\"type\": \"string\"},"+
                "{\"name\": \"group_state\",\"type\": \"string\"},"+
                "{\"name\": \"group_name\",\"type\": \"string\"}"+
                    "]}";



        Schema.Parser parser=new Schema.Parser();
        Schema schema=parser.parse(rsvp_schema);
        GenericData.Record record=new GenericData.Record(schema);

        String venue_name,group_city,group_country,group_state,group_name;
        double latit,longit;
        boolean rsvp;
        long event_time,mtime;
        int guests;



        String key = "key1";


        OkHttpClient client =new OkHttpClient();
        try {

            Request request=new Request.Builder().url("https://stream.meetup.com/2/rsvps").build();
            Response response=client.newCall(request).execute();
            InputStream is ;
            if (response.isSuccessful()) {
                is = response.body().byteStream();
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
                String line;
                int count=0;
                String visibility;
                while ((line=bufferedReader.readLine())!=null)
                {
//                    System.out.println("Sending record to consumer");
                    System.out.println(line);
                    ObjectMapper mapper =new ObjectMapper();
                    JsonNode node=mapper.readTree(line);
                    if (node.get("venue")!=null) {
                        venue_name = node.get("venue").get("venue_name").textValue();
                        latit = node.get("venue").get("lat").asDouble();
                        longit = node.get("venue").get("lon").asDouble();
                    }
                    else
                    {
                        venue_name="Online";
                        latit=0.00;
                        longit=0.00;
                    }

                    group_city=node.get("group").get("group_city").textValue();
                    //System.out.println(group_city);
                    group_state= node.get("group").get("group_state")==null?"NA":node.get("group").get("group_state").textValue();
                    //System.out.println(group_state);
                    group_country=node.get("group").get("group_country").textValue();
                    group_name=node.get("group").get("group_name").textValue();
                    event_time=node.get("event").get("time").asLong();
                    mtime=node.get("mtime").asLong();
                    System.out.println(node.get("response").textValue());
                    rsvp= (node.get("response").textValue()).equals("yes")?true:false;
                    guests=node.get("guests").asInt();





                    ///System.out.println("input values are %d");
                    record.put("venue_name",venue_name);
                    record.put("venue_lat",latit);
                    record.put("venue_long",longit);
                    record.put("event_time",event_time);
                    record.put("mtime",mtime);
                    record.put("guests",guests);
                    record.put("rsvp",rsvp);
                    record.put("group_city",group_city);
                    record.put("group_state",group_state);
                    record.put("group_country",group_country);
                    record.put("group_name",group_name);


                    System.out.println("++++++++++++++++++++++++++++++++++");

                    System.out.println(record.toString());

                    System.out.println("++++++++++++++++++++++++++++++++++");

//                    record.put("lat",lat);
//                    record.put("longi",longi);



                    //ProducerRecord<String,JsonNode> record=new ProducerRecord<String, JsonNode>("venue",node);
                    ProducerRecord<String,GenericRecord> kafkarecord=new ProducerRecord<>("rsvp",key,record);
                    producer.send(kafkarecord);
                }

                is.close();

            }

        }catch(Exception e )
        {
            System.out.println(e);
        }finally {
            producer.close();
        }






    }
}
