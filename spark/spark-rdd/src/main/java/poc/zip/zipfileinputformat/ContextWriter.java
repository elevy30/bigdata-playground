package poc.zip.zipfileinputformat;

/**
 * Noddy method for writing Key/Value pairs out to non-Hadoop classes.
 */
public interface ContextWriter<K, V>
{
    public void write( K key, V value );
}
