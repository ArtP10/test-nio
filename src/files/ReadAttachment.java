package files;

import java.nio.ByteBuffer;

public class ReadAttachment {
	  private final ByteBuffer buffer;
      private final long startPosition;
      private final FileProcessingState fileState;

      public ReadAttachment(ByteBuffer buffer, long startPosition, FileProcessingState fileState) {
          this.buffer = buffer;
          this.startPosition = startPosition;
          this.fileState = fileState;
      }

      public ByteBuffer getBuffer() { return buffer; }
      public long getStartPosition() { return startPosition; }
      public FileProcessingState getFileState() { return fileState; }

}
