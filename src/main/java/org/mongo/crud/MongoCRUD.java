package org.mongo.crud;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.bson.Document;

/**
 *
 * @author miguelinux
 */
public class MongoCRUD {

    //By default it connects to localhost:27017

    private static final MongoClient client = new MongoClient();
    private static final MongoDatabase db = client.getDatabase("mongoTest");
    private final MongoCollection<Document> users = db.getCollection("users");

    public static void main(String[] args) {
        MongoCRUD mongo = new MongoCRUD();
        mongo.insertExample();
        mongo.findExample();
    }

    public void insertExample() {
        users.drop();
        Document migue = new Document("name", "Migue")
                .append("age", 29)
                .append("profession", "programmer");

        Document angelica = new Document("name", "Angelica")
                .append("age", 29)
                .append("profession", "programmer");

        Document migo = new Document("name", "Migo")
                .append("age", 29)
                .append("profession", "medic");

        System.out.println("\nInsert Example:");
        //Notice after the insert that the documents include the _id field
        System.out.println("migue = " + migue);
        users.insertOne(migue);
        users.insertMany(Arrays.asList(angelica, migo));
        System.out.println("migue = " + migue);
        System.out.println("--\n");
    }

    public void findExample() {
        System.out.println("Find Example:");
        users.drop();

        for (int i = 0; i < 5; i++) {
            users.insertOne(new Document("x", i));
        }

        System.out.println("Find one: ");
        Document first = users.find().first();
        System.out.println(first);

        System.out.println("\nFind all into List<>:");
        List<Document> all = users.find().into(new ArrayList<Document>());
        for (Document d : all) {
            System.out.println(d);
        }
        
        //use it when the collection is too large
        //the cursor is not left open when you exhaust the cursor, but if you 
        //get and exception or break before exhaust the cursor then its left open
        System.out.println("\nFind all with iteration (cursor):");
        try(MongoCursor<Document> cursor = users.find().iterator()){
            while(cursor.hasNext()){
                Document doc = cursor.next();
                System.out.println(doc);
            }
        }
        
        System.out.println("\nCount:");
        System.out.println(users.count());

        System.out.println("--\n");
    }

}
