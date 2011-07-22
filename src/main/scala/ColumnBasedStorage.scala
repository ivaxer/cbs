import java.io.{File, RandomAccessFile, IOException}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode._
import scala.io.Source
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.math.min

import Structures.Header


class Block(val header: Header, val data: ByteBuffer) {
}


object ColumnSchema {
    def parseFrom(file: File): ColumnSchema = {
        val lines = Source.fromFile(file).getLines
        val row_size = lines.next().toInt
        return new ColumnSchema(row_size)
    }
}


class ColumnSchema(val row_size: Int) {
    override def toString(): String = {
        return "Row size: " + row_size
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
        return "Schema: " + schema.toString +
        "\nRow start: " + row_start +
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


class ColumnWriter(channel: FileChannel, schema: ColumnSchema) {
    seek_to_endfile()

    def append(block: Block) = {
        check_block(block)
        val header = block.header
        val data = block.data
        write_header(header)
        channel.write(data)
    }

    override def toString(): String = {
        return "Schema: " + schema.toString +
        "\nFile channel: " + channel.toString
    }

    protected def seek_to_endfile() = {
        channel.position(channel.size)
    }

    protected def check_block(block: Block) = {
        val header = block.header
        if (header.getBlockSize != block.data.remaining)
            throw new Exception("Header's block size and actual block size differs")
    }

    protected def write_header(header: Header) = {
        val data = header.toByteArray
        val header_size = data.length.toByte
        val buf = ByteBuffer.allocate(header_size + 1)
        buf.put(header_size)
        buf.put(data)
        buf.flip()
        channel.write(buf)
    }
}


class StorageError(msg: String) extends Exception


class Storage(basedir: String) {
    val schema_suffix = ".schema"

    var schemas: HashMap[String, ColumnSchema] = null
    var filechannels: HashMap[String, FileChannel] = null
    var column_readers: HashMap[String, ColumnReader] = null
    var column_writers: HashMap[String, ColumnWriter] = null

    if (!new File(basedir).exists)
        throw new IOException("Storage directory '%s' does not exists" format basedir)

    def open() = {
        schemas = new HashMap[String, ColumnSchema]
        filechannels = new HashMap[String, FileChannel]
        column_readers = new HashMap[String, ColumnReader]
        column_writers = new HashMap[String, ColumnWriter]

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

    // XXX: return value is ListBuffer of ByteBuffers now.
    def read(start: Int, count: Int, column: String): ListBuffer[ByteBuffer] = {
        is_column_exists(column)
        val reader = get_column_reader(column)
        // XXX: read from start of file each call
        reader.reset()

        while (reader.row_end <= start) {
            reader.next_block()
        }

        val result = ListBuffer[ByteBuffer]()
        var pending_rows = count
        var cur_start = start

        while (pending_rows > 0) {
            val cur_count = min(pending_rows, reader.header.getNumRows().toInt)
            val buf = reader.read(cur_start, cur_count)
            result += buf
            pending_rows -= cur_count
            cur_start += cur_count
            if (pending_rows > 0)
                reader.next_block()
        }

        return result
    }

    def read(start: Int, column: String): ListBuffer[ByteBuffer] = read(start, 1, column)

    def append(block: Block, column: String) = {
        is_column_exists(column)
        val writer = get_column_writer(column)
        writer.append(block)
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

    protected def get_column_writer(column: String): ColumnWriter = {
        if (column_writers.contains(column)) {
            return column_writers(column)
        }
        else {
            val channel = filechannels(column)
            val schema = schemas(column)
            return new ColumnWriter(channel, schema)
        }
    }
}

object ColumnBasedStorage {
    def main(args: Array[String]) = {
        val storage = new Storage("/tmp/storage")
        storage.open()
        val header1 = Header.newBuilder().setNumRows(2).setBlockSize(16).build()
        val data1 = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16))
        storage.append(new Block(header1, data1), "huj")

        val header2 = Header.newBuilder().setNumRows(1).setBlockSize(8).build()
        val data2 = ByteBuffer.wrap(Array[Byte](17, 18, 19, 20, 21, 22, 23, 24))
        storage.append(new Block(header2, data2), "huj")

        var row = new Array[Byte](8)
        for (buf <- storage.read(0, 3, "huj")) {
            while (buf.hasRemaining()) {
                buf.get(row)
                println(row.mkString)
            }
        }
    }
}

