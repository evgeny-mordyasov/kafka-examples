package rgo.nativekafka.consumer.exception;

public class BatchProcessingException extends RuntimeException {

    public BatchProcessingException() {
    }

    public BatchProcessingException(String message) {
        super(message);
    }

    public BatchProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
