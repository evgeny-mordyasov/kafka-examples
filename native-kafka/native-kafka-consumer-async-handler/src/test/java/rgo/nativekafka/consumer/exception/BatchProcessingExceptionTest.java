package rgo.nativekafka.consumer.exception;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BatchProcessingExceptionTest {

    @Test
    void createInstance_withNoArgs_ShouldCreateExceptionWithoutMessageAndCause() {
        BatchProcessingException exception = new BatchProcessingException();

        assertThat(exception).hasMessage(null);
        assertThat(exception.getCause()).isNull();
    }

    @Test
    void createInstance_withMessage_ShouldCreateExceptionWithMessage() {
        BatchProcessingException exception = new BatchProcessingException("message");

        assertThat(exception).hasMessage("message");
        assertThat(exception.getCause()).isNull();
    }

    @Test
    void createInstance_withMessageAndCause_ShouldCreateExceptionWithMessageAndCause() {
        RuntimeException cause = new RuntimeException("cause");

        BatchProcessingException exception = new BatchProcessingException("message", cause);

        assertThat(exception).hasMessage("message");
        assertThat(exception).hasCause(cause);
    }
}
