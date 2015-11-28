package com.futuristGDF.kafkaexamples;
 

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.utils.VerifiableProperties;
import scala.collection.Seq;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class VHM_KafkaProducer {
    final static String TOPIC = "IB_TEST1";
    
    public static void main(String[] argv){
    	
        Properties properties = new Properties();
        //properties.put("metadata.broker.list","d2mimphgda004.edc.nam.gm.com:9092");
        properties.put("metadata.broker.list","192.168.0.121:6667");
        
        properties.put("partitioner.class", "com.futuristGDF.kafkaexamples.RoundRobinPartitioner");
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      
        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
        
        String key = "0";
        
        for(int i=1; i<100000; i++){
        	
        	 SimpleDateFormat sdf = new SimpleDateFormat();
             KeyedMessage<String, String> message =new KeyedMessage<String, String>(TOPIC, key,"6e 6f 77 20 69 73 20 74 68 65 20 74 69 6d 65 20 66 6f 72 20 61 6c 6c 20 6d 65 6e 20 74 6f 20 63 6f 6d 65 20 74 6f 20 74 68 65 20 61 69 64 20 6f 66 20 74 68 65 69 72 20 63 6f 75 6e 74 72 79" + sdf.format(new Date()));
             producer.send(message);
           
       }
        
        producer.close();
        
    }
    
   
}
