package poc.sql.integrity.bitwise;

import java.io.Serializable;

/**
 * Created by elevy on 15-Feb-17.
 */
public class ColumnLocation implements Serializable{
    public int bitColumnId;
    public int bitLocation;

    public ColumnLocation(int bitColumnId, int bitLocation) {
        this.bitColumnId = bitColumnId;
        this.bitLocation = bitLocation;
    }

    @Override
    public String toString() {
        return "ColumnLocation{" +
                "bitColumnId=" + bitColumnId +
                ", bitLocation=" + bitLocation +
                "}\n";
    }
}
