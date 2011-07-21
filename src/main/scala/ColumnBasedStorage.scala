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

    def read(dst: ByteBuffer, start: Int, count: Int) = {
        if (start < row_start)
            throw new Exception("Out of range: start < first row in block")
        if (start + count > row_end)
            throw new Exception("Out of range: start + count >= last row in block")
        val length = count * schema.row_size
        val offset = data_start_offset + (start - row_start) * schema.row_size
        buffer.position(offset)
        // XXX: memory allocation
        val tmp_array = new Array[Byte](length)
        buffer.get(tmp_array)
        dst.put(tmp_array)
        dst.flip()
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
    //var iobuffers: HashMap[String, ByteBuffer] = null
    var filechannels: HashMap[String, FileChannel] = null
    var mmaps: HashMap[String, ByteBuffer] = null

    if (!new File(basedir).exists)
        throw new IOException("Storage directory '%s' does not exists" format basedir)

    def open() = {
        schemas = new HashMap[String, ColumnSchema]
        filechannels = new HashMap[String, FileChannel]
        mmaps = new HashMap[String, ByteBuffer]

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

    def read(start: Int, count: Int, column: String): ByteBuffer = {
        is_column_exists(column)
        val channel = filechannels(column)
        val schema = schemas(column)
        var buf: ByteBuffer = null
        if (mmaps.contains(column)) {
            buf = mmaps(column)
        }
        else {
            buf = channel.map(READ_ONLY, 0, channel.size)
            mmaps += column -> buf
        }
        buf.position(0)
        var header = read_next_header(buf)
        var start_row = 0
        var last_row = header.getNumRows().toInt
        while (last_row <= start) {
            buf.position(buf.position() + header.getNumRows().toInt * schema.row_size)
            header = read_next_header(buf)
            start_row = last_row
            last_row = last_row + header.getNumRows().toInt
            println("Start row: " + start_row + " last row: " + last_row)
        }
        if (start + count > last_row)
            throw new Exception
        buf.position(buf.position() + (start - start_row) * schema.row_size)
        val size = count * schema.row_size
        val result: Array[Byte] = new Array[Byte](size)
        buf.get(result)
        return ByteBuffer.wrap(result)
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

    protected def read_next_header(buf: ByteBuffer): Header = {
        val header_size = buf.get()
        val header_data = new Array[Byte](header_size)
        buf.get(header_data)
        return Header.parseFrom(header_data)
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

