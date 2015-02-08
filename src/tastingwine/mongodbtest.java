package tastingwine;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoDatabase;

public class mongodbtest {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		System.out.println("Going to open mongoDB Connection....");
		MongoClient mongoClient = new MongoClient("localhost", 27017);
		DB db = mongoClient.getDB("mongodbtest");
		DBCollection wishlistcollection = db.getCollection("wishlist");
		DBObject document = new BasicDBObject();
		document.put("personID", "person1");
		document.put("wineID", "wine1");
		wishlistcollection.insert(document);
	}
}
