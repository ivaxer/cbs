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


class ColumnReader(channel: FileChannel, schema: ColumnSchema) {
    val buffer = channel.map(READ_ONLY, 0, channel.size)
    var header: Header = null
    var data_start_offset = 0
    var data_end_offset = 0
    var row_start = 0
    var row_end = 0

    next_block()

    def next_block(): Header = {
        seek_to_next_header();
        header = read_header()
        data_start_offset = buffer.position()
        data_end_offset = data_start_offset + schema.row_size * header.getNumRows().toInt
        row_start = row_end
        row_end = row_start + header.getNumRows().toInt
        return header
    }

    def read(start: Int, count: Int): ByteBuffer = {
        if (start < row_start)
            throw new Exception("Out of range: start < first row in block")
        if (start + count > row_end)
            throw new Exception("Out of range: start + count >= last row in block")
        val length = count * schema.row_size
        val offset = data_start_offset + (start - row_start) * schema.row_size
        buffer.position(offset)
        val result = buffer.slice()
        result.limit(length)
        return result
    }

    def reset() = {
        buffer.position(0)
        data_start_offset = 0
        data_end_offset = 0
        row_start = 0
        row_end = 0
        next_block()
    }

    override def toString(): String = {
        return "Row start: " + row_start +
        "\nRow end: " + row_end +
        "\nData start offset: " + data_start_offset +
        "\nData end offset: " + data_end_offset +
        "\nBacked buffer: " + buffer.toString +
        "\nHeader: " + header.toString
    }

    protected def seek_to_next_header() = {
        buffer.position(data_end_offset)
    }

    protected def read_header(): Header = {
        val header_size = buffer.get()
        val header_data = new Array[Byte](header_size)
        buffer.get(header_data)
        return Header.parseFrom(header_data)
    }
}



class ColumnSchema(val row_size: Int) {
}


class StorageError(msg: String) extends Exception


class Storage(basedir: String) {
    val schema_suffix = ".schema"

    var schemas: HashMap[String, ColumnSchema] = null
    var filechannels: HashMap[String, FileChannel] = null
    var column_readers: HashMap[String, ColumnReader] = null

    if (!new File(basedir).exists)
        throw new IOException("Storage directory '%s' does not exists" format basedir)

    def open() = {
        schemas = new HashMap[String, ColumnSchema]
        filechannels = new HashMap[String, FileChannel]
        column_readers = new HashMap[String, ColumnReader]

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

    def read(dst: ByteBuffer, start: Int, count: Int, column: String) = {
        is_column_exists(column)
        val reader = get_column_reader(column)
        // XXX: read from start of file each call
        reader.reset()
        println(reader)

        while (reader.row_end <= start) {
            reader.next_block()
            println(reader)
        }

        // XXX: just for test
        reader.read(dst, start, count)
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

    protected def get_column_reader(column: String): ColumnReader = {
        if (column_readers.contains(column)) {
            return column_readers(column)
        }
        else {
            val channel = filechannels(column)
            val schema = schemas(column)
            return new ColumnReader(channel, schema)
        }
    }
}

object ColumnBasedStorage {
    def main(args: Array[String]) = {
        val storage = new Storage("/tmp/storage")
        storage.open()
        val header = Header.newBuilder().setNumRows(1).setBlockSize(8).build()
        var data: Array[Byte] = Array[Byte](2, 2, 3, 4, 5, 6, 7, 8)
        //storage.append(new Block(header, data), "huj")
        val buf = ByteBuffer.allocate(8)
        storage.read(buf, 1, 1, "huj")
        println(buf.get())
    }
}

