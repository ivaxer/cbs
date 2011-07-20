import java.io.{File, RandomAccessFile, IOException}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode._
import scala.io.Source
import scala.collection.mutable.HashMap

import Structures.Header


class Block(val header: Header, val data: Array[Byte]) {
}


object ColumnSchema {
    def parseFrom(file: File): ColumnSchema = {
        val lines = Source.fromFile(file).getLines
        val row_size = lines.next().toInt
        return new ColumnSchema(row_size)
    }
}


class ColumnSchema(val row_size: Int) {
}


class StorageError(msg: String) extends Exception


class Storage(basedir: String) {
    val schema_suffix = ".schema"

    var schemas: HashMap[String, ColumnSchema] = null
    //var iobuffers: HashMap[String, ByteBuffer] = null
    var filechannels: HashMap[String, FileChannel] = null

    if (!new File(basedir).exists)
        throw new IOException("Storage directory '%s' does not exists" format basedir)

    def open() = {
        schemas = new HashMap[String, ColumnSchema]
        filechannels = new HashMap[String, FileChannel]

        val db = new File(basedir)
        val fls = db.listFiles()
        val schema_fls = fls.filter(_.getName.endsWith(schema_suffix))

        for (f <- schema_fls) {
            val name = f.getName().dropRight(schema_suffix.length)
            val fname = f.getAbsolutePath().dropRight(schema_suffix.length)

            val channel = new RandomAccessFile(new File(fname), "rw").getChannel()
            filechannels += name -> channel

            schemas += name -> ColumnSchema.parseFrom(f)
        }
    }

    def close() = {
    }

    def read(start: Int, count: Int, column: String) = {
        is_column_exists(column)
        val channel = file
    }

    def append(block: Block, column: String) = {
        is_column_exists(column)
        val channel = filechannels(column)
        channel.position(channel.size)
        val header_data = block.header.toByteArray
        val header_size: Byte = header_data.length.toByte
        val size = 1 + header_size + block.header.getBlockSize().toInt
        val buf = ByteBuffer.allocate(size)
        buf.put(header_size).put(header_data).put(block.data)
        buf.flip()
        val appended = channel.write(buf)
        print("Appended %s of %s bytes to column '%s':\n%s" format (appended, size, column, block.header))
    }

    def repack() = {
    }

    protected def is_column_exists(column: String) = {
        if (!schemas.contains(column))
            throw new StorageError("Unknown column")
    }
}

object ColumnBasedStorage {
    def main(args: Array[String]) = {
        val storage = new Storage("/tmp/storage")
        storage.open()
        val header = Header.newBuilder().setNumRows(1).setBlockSize(8).build()
        val data: Array[Byte] = Array[Byte](1, 2, 3, 4, 5, 6, 7, 8)
        storage.append(new Block(header, data), "huj")
    }
}

