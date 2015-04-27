import org.scalaquery.ql.basic.{BasicTable => Table}
import org.scalaquery.ql.extended.SQLiteDriver.Implicit._
import org.scalaquery.ql.TypeMapper._
import org.scalaquery.ql._
import org.scalaquery.session._
import org.scalaquery.session.Database.threadLocalSession
import org.scalaquery.meta.MTable
import org.scalaquery.ql.Ordering.Desc
import org.scalaquery.ql.Ordering$Asc$
import sun.org.mozilla.javascript.internal.ast.Yield
import java.sql.Connection
import java.sql.DriverManager
import scala.collection.mutable.ListBuffer
import com.sohu.kafka.offsetapp.OffsetDB
    
object OffsetDBTest {
  val database = Database.forURL("jdbc:sqlite:offsetapp.db", 
			   driver = "org.sqlite.JDBC")
			   
  val Offsets = new Table[(String, String, String, Long, Long, Option[String], Long)]("OFFSETS") {
    val group = column[String]("group")
	val topic = column[String]("topic")
	val partition = column[String]("partition")
	val offset = column[Long]("offset")
	val logSize = column[Long]("log_size")
	val owner = column[Option[String]]("owner")
	val timestamp = column[Long]("timestamp")
    
	def * = (group ~ topic ~ partition ~ offset ~ logSize ~ owner ~ timestamp)
  }
	
  def main(args: Array[String]) {
	 
	Class.forName("org.sqlite.JDBC")
	val conn = DriverManager.getConnection("jdbc:sqlite:offsetapp.db")
	try {
	  val timestamp = System.currentTimeMillis()
	  println(testQuery(timestamp)(conn))
	  println(testDelete(timestamp)(conn))
	  println(testQuery(timestamp)(conn))
	} finally {
		conn.close()
	}
  }
  
  def createTable() {
    database withSession {
      if (MTable.getTables("OFFSETS").list().isEmpty) {
        Offsets.ddl.create
      }
	}
  }
	
  def testQuery() {
	 database withSession {
	   val d = for {
	     c <- Offsets 
	     _ <- Query orderBy c.timestamp
	   } yield c.timestamp ~ c.partition ~ c.owner ~ c.offset ~ c.logSize
	   println(d.list().size)
	 }
  }

  def testQuery(timestamp: Long)(conn: Connection) : Int = {
    val queryStatement = conn.prepareStatement("select topic, offset from OFFSETS where timestamp < ? limit 10")
	try {
		queryStatement.setLong(1, timestamp)
		val rs = queryStatement.executeQuery()
		try {
			val b = new ListBuffer[(Int, String)]
			while(rs.next)
				b.append((rs.getInt(1), rs.getString(2)))
			b.toList.size
		} finally rs.close()
	} finally queryStatement.close()
  } 

  def testDelete(timestamp: Long)(conn: Connection) = {
	val deleteStatement = conn.prepareStatement("delete from OFFSETS where timestamp < ?")
	try {
		deleteStatement.setLong(1, timestamp)
		deleteStatement.executeUpdate()
	} finally deleteStatement.close()
  }
}
