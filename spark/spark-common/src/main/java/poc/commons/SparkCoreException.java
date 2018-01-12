package poc.commons;


public class SparkCoreException extends RuntimeException {

    public SparkCoreException(String message) {
        super(message);
    }
	public SparkCoreException(String message, Throwable cause) {
		super(message, cause);
	}
}