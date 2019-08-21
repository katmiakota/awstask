
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.S3Object;
import org.json.JSONObject;

import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;


public class LambdaRequestHandler implements RequestHandler<S3Event, String> {

    private AWSCredentials credentials = new BasicAWSCredentials(
            "AAAA",
            "EEEEE");


    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        AmazonS3 s3client = AmazonS3ClientBuilder.standard().build();
        S3EventNotification.S3EventNotificationRecord record = s3Event.getRecords().get(0);

        String bkt = record.getS3().getBucket().getName();
        String key = record.getS3().getObject().getKey().replace('+', ' ');
        try {
            key = URLDecoder.decode(key, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        S3Object s3Object = s3client.getObject(bkt, key);


        InputStreamReader cmdStream =
                new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader(cmdStream);

        String data = null;
        try {
            data = getJSONInputStream(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }


        JSONObject obj = new JSONObject(data);
        AmazonDynamoDB dynamodb = AmazonDynamoDBClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
        DynamoDB dynamoDB = new DynamoDB(dynamodb);
        Table table = dynamoDB.getTable(obj.getString("tableName"));
        Item item = new Item()
                .withPrimaryKey("packageId", obj.getString("packageId"))
                .withBigInteger("originTimeStamp", obj.getBigInteger("originTimeStamp"));
        table.putItem(item);


        return null;
    }


    private String getJSONInputStream(BufferedReader reader) throws IOException {
        String data = "";
        String line;
        while ((line = reader.readLine()) != null) {
            data += line;
        }
        return data;
    }
}




