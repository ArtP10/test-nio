package files;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class FileProcessingState {
	
	final AsynchronousFileChannel inputChannel;
	final AsynchronousFileChannel outputChannel;
	
	final long inputFileSize;
	
	final String fileName;
	
	final AtomicLong currentReadPosition = new AtomicLong(0);
    final ConcurrentSkipListMap<Long, ByteBuffer> processedBuffers = new ConcurrentSkipListMap<>();
    final AtomicLong nextWritePosition = new AtomicLong(0);
    final AtomicLong totalBytesWritten = new AtomicLong(0);
    volatile boolean allReadsInitiated = false; 

    FileProcessingState(AsynchronousFileChannel inputChannel, AsynchronousFileChannel outputChannel, long inputFileSize, String fileName) {
        this.inputChannel = inputChannel;
        this.outputChannel = outputChannel;
        this.inputFileSize = inputFileSize;
        this.fileName = fileName;
    }

}
