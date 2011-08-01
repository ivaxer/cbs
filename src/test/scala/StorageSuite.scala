import java.io.File
import java.nio.ByteBuffer

import org.scalatest.Suite
import org.scalatest.BeforeAndAfterEach

import org.ivaxer.cbs.{Storage, ColumnSchema}
import org.ivaxer.cbs.OpenMode._

class StorageSuite extends Suite with BeforeAndAfterEach {
    val dbdir = "/tmp/test-storage"

    override def beforeEach() {
        val dbf = new File(dbdir)
        if (!dbf.exists)
            dbf.mkdir()
        create_db()
    }

    def create_db() {
        val db = new Storage(dbdir, WRITE)
        db.create("longs", new ColumnSchema(8))
        db.create("ints", new ColumnSchema(4))
        db.create("shorts", new ColumnSchema(2))
        db.create("bytes", new ColumnSchema(1))
    }

    override def afterEach() {
        val dbf = new File(dbdir)
        for (f <- dbf.listFiles)
            f.delete()
        dbf.delete()
    }

    def testExample() {
        val dbw = new Storage(dbdir, WRITE)
        val buf = ByteBuffer.allocate(24)
        buf.putLong(Long.MaxValue)
        buf.putLong(Int.MaxValue)
        buf.putLong(Short.MaxValue)
        buf.position(0)
        buf.limit(16)
        dbw.append("longs", buf)
        buf.position(16)
        buf.limit(24)
        dbw.append("longs", buf)
        val dbr = new Storage(dbdir, READ)
        var bb  = dbr.read("longs", 0, 2)
        buf.rewind()
        for (b <- bb)
            while (b.hasRemaining)
                assert(b.getLong() === buf.getLong())
        bb = dbr.read("longs", 1, 1)
        for (b <- bb)
            while (b.hasRemaining)
                assert(b.getLong() === Int.MaxValue)
        buf.rewind()
        bb = dbr.read("longs", 0, 3)
        for (b <- bb)
            while (b.hasRemaining)
                assert(b.getLong() === buf.getLong())
    }
}

