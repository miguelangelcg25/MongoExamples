package org.mongo.course.hw3;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;

/**
 *
 * @author miguelinux
 */
public class HomeWork3_1 {
    
    private static final MongoClient client = new MongoClient();
    private static final MongoDatabase db = client.getDatabase("school");
    private final MongoCollection<Document> students = db.getCollection("students");
    
    /**Deletes the lowest homework score
     * 
     * {
	"_id" : 0,
	"name" : "aimee Zank",
	"scores" : [
		{
			"type" : "exam",
			"score" : 1.463179736705023
		},
		{
			"score" : 11.78273309957772,
			"type" : "quiz"
		},
		{
			"type" : "homework",
			"score" : 6.676176060654615
		},
		{
			"type" : "homework",
			"score" : 35.8740349954354
		}
	]
    }
     */
    public void deleteLowestGrade(){
        List<Document> list = students.find().into(new ArrayList<Document>());
        
        for(Document student: list){
            List<Document> scores = (List)student.get("scores");
            Document min = new Document("score", 99999d);
            
            for(Document s: scores){
                if(s.get("type").equals("homework") && 
                        ((Double)s.get("score" )) < ((Double)min.get("score")) ){
                    min = s;
                }
            }
            System.out.println(min);
            Document update = new Document("scores", min);
            students.updateOne(student, new Document("$pull", update));
        }
    }
    
    public static void main(String[] args) {
        HomeWork3_1 hw = new HomeWork3_1();
        hw.deleteLowestGrade();
    }
    
}
