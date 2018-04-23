package edu.unc.tier.rabbittrace;
import com.rabbitmq.client.*;
//import java.util.List;
//import java.util.Iterator;
import java.io.FileWriter;
import java.io.IOException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
//import java.util.ArrayList;
//import java.io.BufferedReader;
//import java.io.InputStreamReader;
//import javax.xml.parsers.DocumentBuilderFactory;
//import javax.xml.parsers.DocumentBuilder;
//import org.w3c.dom.Document;
//import org.w3c.dom.NodeList;
//import org.w3c.dom.Node;
//import org.w3c.dom.Element;
//import java.io.StringReader;
//import org.xml.sax.InputSource;


public class Trace {

  private static final String EXCHANGE_NAME = "firehose.topic";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    factory.setUsername("mysql");
    factory.setPassword("5ecr3t");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();
    boolean durable = true;
    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC, durable);
    channel.exchangeBind(EXCHANGE_NAME,"amq.rabbitmq.trace","#");
    String queueName = channel.queueDeclare().getQueue();
    //queueName = "firehose";

     if (argv.length < 1) {
      System.err.println("Usage: ReceiveLogsTopic [binding_key]...");
      System.exit(1);
    } 

    for (String bindingKey : argv) {
      channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
    }

    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
                                 AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        //System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'");
	System.out.println(envelope.toString());
        JSONParser jsonParser = new JSONParser();
        try
        {
           String filename= "/var/log/rabbitmq/trace.log";
           FileWriter fw = new FileWriter(filename,true);
           JSONObject jsonObject = (JSONObject) jsonParser.parse(message);
	   System.out.println(jsonObject.toString());
           fw.write(envelope.toString());
           fw.write(jsonObject.toString());
	   fw.write(System.lineSeparator());
           fw.close();
	}
        catch (Exception e)
        {
           System.err.println(e.getMessage());
        }
      }
    };
    channel.basicConsume(queueName, true, consumer);
  }
}

