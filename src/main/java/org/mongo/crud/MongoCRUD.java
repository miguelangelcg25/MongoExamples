package org.mongo.crud;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;

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
        mongo.findWithFilters();
        mongo.sort();
        mongo.updateAndReplace();
        mongo.delete();
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
        try (MongoCursor<Document> cursor = users.find().iterator()) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                System.out.println(doc);
            }
        }

        System.out.println("\nCount:");
        System.out.println(users.count());

        System.out.println("--\n");
    }

    public void findWithFilters() {
        System.out.println("Find with FILTERS:");
        users.drop();

        for (int i = 0; i < 5; i++) {
            users.insertOne(new Document("x", i).append("y", i * 10));
        }

        //db.users.find({ x:3, y:{"$gt:10, $lt:50}})
        Bson filter = new Document("x", 3).append("y", new Document("$gt", 10).append("$lt", 50));
        Document doc = users.find(filter).first();
        System.out.println(doc);

        filter = and(eq("x", 3), gt("y", 10), lt("y", 50));
        doc = users.find(filter).first();
        System.out.println(doc);

        //excludes x and includes y
        Bson projections = new Document("x", 0).append("y", 1);
        //exlcudes x and _id
        projections = Projections.exclude("x", "_id");
        //includes y, _id is included by default
        projections = Projections.include("y");
        //includes x, y and exlcudes _id
        projections = Projections.fields(Projections.include("x", "y"), Projections.exclude("_id"));
        List<Document> list = users.find(filter).projection(projections).into(new ArrayList<Document>());
    }

    public void sort() {
        System.out.println("\nSort, limit and skip:");
        users.drop();
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                users.insertOne(new Document("i", i).append("j", j));
            }
        }

        Bson projection = Projections.excludeId();
        //sorts i ascending and j descending
        Bson sort = new Document("i", 1).append("j", -1);
        sort = Sorts.ascending("i", "j");
        sort = Sorts.orderBy(Sorts.ascending("i"), Sorts.descending("i"));

        List<Document> list = users.find()
                .sort(sort)
                .projection(projection)
                .skip(5)
                .limit(10)
                .into(new ArrayList<Document>());

        for (Document d : list) {
            System.out.println(d);
        }
    }

    public void updateAndReplace() {
        System.out.println("\nUpdate and replace:");
        users.drop();

        for (int i = 0; i < 8; i++) {
            users.insertOne(new Document("_id", i).append("i", i));
        }

        //replace the WHOLE document!!!
        users.replaceOne(eq("_id", 5), new Document("i", 5).append("x", 20));
        //update a value
        users.updateOne(eq("_id", 6), new Document("$set", new Document("i", 66)));
        //upsert
        users.updateOne(eq("_id", 9), new Document("$set", new Document("i", 99)),
                new UpdateOptions().upsert(true));
        //update many documents
        users.updateMany(gte("_id", 5), new Document("$inc", new Document("i", 1)));

        List<Document> list = users.find().into(new ArrayList<Document>());

        for (Document d : list) {
            System.out.println(d);
        }

    }

    public void delete() {
        System.out.println("\nUpdate and replace:");
        users.drop();

        for (int i = 0; i < 8; i++) {
            users.insertOne(new Document("_id", i).append("i", i));
        }
        
        users.deleteMany(gte("_id", 5));
        users.deleteOne(eq("_id", 3));
        
        List<Document> list = users.find().into(new ArrayList<Document>());

        for (Document d : list) {
            System.out.println(d);
        }
    }
}
