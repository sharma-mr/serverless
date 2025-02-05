package com.csye6225.neu.serverless.event;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.amazonaws.services.simpleemail.model.*;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.util.*;

public class EmailEvent {

    private DynamoDB amazonDynamoDB;

    //@Value("${tableName}")
   // private String tableName;


    public EmailEvent() {
        AmazonDynamoDBClient client = new AmazonDynamoDBClient();
        client.setRegion(Region.getRegion(Regions.US_EAST_1));
        this.amazonDynamoDB = new DynamoDB(client);
    }

    private static final String EMAIL_SUBJECT="Due Bills";

    long unixTime = Instant.now().getEpochSecond() + 60*60;

    private static final String SENDER_EMAIL = System.getenv("SenderEmail");

    private static final String EMAIL_TEXT = "The links for the due bills are :- ";

    public Object handleRequest(SNSEvent request, Context context){
        TableCollection<ListTablesResult> tables = amazonDynamoDB.listTables();
        Iterator<Table> iterator = tables.iterator();
        while (iterator.hasNext()) {
            Table table = iterator.next();
            context.getLogger().log("Dynamo db table name:- " + table.getTableName());
        }
        Table table = amazonDynamoDB.getTable("dynamoDBTable");
        if(table == null)
            context.getLogger().log("Table not present in dynamoDB");

        if (request.getRecords() == null) {
            context.getLogger().log("There are no records available");
            return null;
        }

        if(amazonDynamoDB == null) {
            context.getLogger().log("Dynamo db object is null");
        }
        String messageFromSQS =  request.getRecords().get(0).getSNS().getMessage();
        String emailRecipient = messageFromSQS.split(",")[0];
        Item item = amazonDynamoDB.getTable("dynamoDBTable").getItem("id", emailRecipient);
        if ((item != null && Long.parseLong(item.get("TTL").toString()) < Instant.now().getEpochSecond() || item == null)) {
            amazonDynamoDB.getTable("dynamoDBTable").putItem(new PutItemSpec()
                    .withItem(new Item().withString("id", emailRecipient).withLong("TTL", unixTime)));
            String[] billLinks = messageFromSQS.split(",");
            StringBuilder stringBuilder = new StringBuilder();
            for (int index = 1; index < billLinks.length; index++) {
                stringBuilder.append("\n");
                stringBuilder.append(billLinks[index]);
            }

            Content content = new Content().withData(stringBuilder.toString());
            Body body = new Body().withText(content);
            try {
                AmazonSimpleEmailService client =
                        AmazonSimpleEmailServiceClientBuilder.standard()
                                .withRegion(Regions.US_EAST_1).build();
                SendEmailRequest emailRequest = new SendEmailRequest()
                        .withDestination(
                                new Destination().withToAddresses(emailRecipient))
                        .withMessage(new Message()
                                .withBody(body)
                                .withSubject(new Content()
                                        .withCharset("UTF-8").withData(EMAIL_SUBJECT)))
                        .withSource(SENDER_EMAIL);
                client.sendEmail(emailRequest);
            } catch (Exception ex) {
            }
        }
        return null;
    }

}
