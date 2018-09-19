package no.ssb.lds.core.validation;

public class LinkedDocumentValidationException extends RuntimeException {

    LinkedDocumentValidationException(String message) {
        super(message);
    }

    LinkedDocumentValidationException(String message, Throwable cause) {
        super(message, cause);
    }

}
