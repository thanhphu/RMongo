package rmongo

import java.util
import java.util.logging.{Level, Logger}

import com.mongodb._
import com.mongodb.util.JSON

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class RMongo(dbName: String, hosts: String, replica: Boolean, username: String, pwd: String) {
	val mongoLogger = Logger.getLogger("com.mongodb")
	mongoLogger.setLevel(Level.SEVERE)
	val servers = hosts.split(",").map(_.trim.split(":")).map { a =>
		if (a.length < 2) new ServerAddress(a(0), 27017) else new ServerAddress(a(0), a(1).toInt)
	}.toList
	val this.replica = replica
	val credential = mCreateCredential(username, dbName, pwd)
	val m = if (!this.replica || username.isEmpty || pwd.isEmpty) {
		new MongoClient(servers.head)
	} else {
		new MongoClient(servers, List(credential))
	}
	val db = m.getDB(dbName)
	// val newDB = m.getDatabase(dbName)
	var writeConcern = WriteConcern.ACKNOWLEDGED

	def this(dbName: String, host: String, replica: Boolean) = this(dbName, host + ":27017", replica, "", "")

	def this(dbName: String, host: String, port: Int) = this(dbName, host + ":" + port, false, "", "")

	def this(dbName: String) = this(dbName, "127.0.0.1:27017", false, "", "")

	def mCreateCredential(userName: String, database: String, password: String): MongoCredential = {
		MongoCredential.createCredential(username, database, password.toCharArray)
	}

	def dbSetWriteConcern(w: Int, wtimeout: Int, fsync: Boolean, j: Boolean) {
		writeConcern = new WriteConcern(w, wtimeout)
	}

	def dbShowCollections(): Array[String] = {
		db.getCollectionNames.map(name => name).toArray
	}

	def dbInsertDocument(collectionName: String, jsonDoc: String): String = {
		dbModificationAction(collectionName, jsonDoc, _.insert(_, writeConcern))
	}

	def dbRemoveQuery(collectionName: String, query: String): String = {
		dbModificationAction(collectionName, query, _.remove(_, writeConcern))
	}

	def dbModificationAction(collectionName: String,
	                         query: String,
	                         action: (DBCollection, DBObject) => WriteResult): String = {
		val dbCollection = db.getCollection(collectionName)

		val queryObject = JSON.parse(query).asInstanceOf[DBObject]
		try {
			val results = action(dbCollection, queryObject)

			if (results != null) "ok" else "error"
		} catch  {
			case e: Exception => e.getMessage
		}
	}

	def dbGetQuery(collectionName: String, query: String): String = {
		// newDB.getCollection(collectionName).find(BsonDocument.parse(query)).head.toJson
		dbGetQuery(collectionName, query, 0, 1000)
	}

	def dbGetQuery(collectionName: String, query: String,
	               skipNum: Double, limitNum: Double): String = {
		dbGetQuery(collectionName, query, skipNum.toInt, limitNum.toInt)
	}

	def dbGetQuery(collectionName: String, query: String,
	               skipNum: Int, limitNum: Int): String = {
		dbGetQuery(collectionName, query, "{}", skipNum, limitNum)
	}

	def dbGetQuery(collectionName: String, query: String, keys: String): String = {
		dbGetQuery(collectionName, query, keys, 0, 1000)
	}

	def dbGetQuery(collectionName: String, query: String, keys: String,
	               skipNum: Double, limitNum: Double): String = {
		dbGetQuery(collectionName, query, keys, skipNum.toInt, limitNum.toInt)
	}

	def dbGetQuery(collectionName: String, query: String, keys: String,
	               skipNum: Int, limitNum: Int): String = {
		val dbCollection = db.getCollection(collectionName)

		val queryObject = JSON.parse(query).asInstanceOf[DBObject]
		val keysObject = JSON.parse(keys).asInstanceOf[DBObject]
		val cursor = dbCollection.find(queryObject, keysObject).skip(skipNum).limit(limitNum)

		val results = RMongo.toCsvOutput(cursor)

		results
	}

	def dbGetDistinct(collectionName: String, key: String): String = {
		dbGetDistinct(collectionName, key, "")
	}

	def dbGetDistinct(collectionName: String, key: String, query: String): String = {
		val dbCollection = db.getCollection(collectionName)

		val queryObject = JSON.parse(query).asInstanceOf[DBObject]
		val distinctResults = dbCollection.distinct(key, queryObject).iterator
		val results = ListBuffer[String]()

		while (distinctResults.hasNext) {
			val item = distinctResults.next
			results.append("\"" + item.toString.replaceAll("\n", "\\n") + "\"")
		}

		results.mkString("\n")
	}

	//def dbAggregate(collectionName: String, queries: Array[String]): String = {
	def dbAggregate(collectionName: String, queries: Array[String]): Array[String] = {
		val dbCollection = db.getCollection(collectionName)
		val queryArray = new Array[DBObject](queries.length)
		for (i <- queries.indices) {
			val query = queries(i)
			queryArray(i) = JSON.parse(query).asInstanceOf[DBObject]
		}
		val aggregateIterator = dbCollection.aggregate(queryArray(0), util.Arrays.copyOfRange(queryArray, 1, queryArray.length): _*).results.iterator

		val results = ListBuffer[String]()
		while (aggregateIterator.hasNext) {
			val item = aggregateIterator.next
			// results.append("\"" + item.toString.replaceAll("\n", "\\n") + "\"")
			results.append(item.toString)
		}
		//results.mkString("\n")
		results.toArray
	}

	def close() {
		m.close()
	}

	def main() {

	}
}

object RMongo {
	val SEPARATOR = ""

	def toJsonOutput(cursor: DBCursor): String = {
		val results = ListBuffer[String]()
		while (cursor.hasNext) {
			val item = cursor.next
			results.append(item.toString)
		}

		results.mkString("[", SEPARATOR, "]")
	}

	def toCsvOutput(cursor: DBCursor): String = {

		if (!cursor.hasNext) return ""

		val results = ListBuffer[String]()

		if (cursor.getKeysWanted != null && cursor.getKeysWanted.keySet.size != 0) {
			/* If fields were specified (save time) */
			val keysWanted = cursor.getKeysWanted
			keysWanted.put("_id", 1)
			val keys = keysWanted.keySet.toArray(new Array[String](keysWanted.keySet.size))
			results.append(keys.mkString(SEPARATOR))
			while (cursor.hasNext) {
				results.append(csvRowFromDBObject(keys, cursor.next))
			}
		} else {
			/* Try to find set of desired keys by scanning cursor */
			val keysWanted = new util.HashSet[String]()
			val ccursor: DBCursor = cursor.copy
			while (ccursor.hasNext) {
				keysWanted.addAll(ccursor.next.keySet)
			}
			val keys = keysWanted.toArray(new Array[String](keysWanted.size))
			results.append(keys.mkString(SEPARATOR))
			while (cursor.hasNext) {
				results.append(csvRowFromDBObject(keys, cursor.next))
			}
		}

		results.mkString("\n")
	}

	def csvRowFromDBObject(keys: Array[String], item: DBObject): String = {

		keys.map { k =>
			val value = item.get(k)

			if (value != null)
				value.toString.replaceAll("\"", "\\\"")
			else
				""
		}.mkString(SEPARATOR)
	}
}
