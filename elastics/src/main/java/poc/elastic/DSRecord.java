package poc.elastic;

import lombok.*;

import java.util.Map;

/**
 * Created by eyallevy on 19/08/17 .
 */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class DSRecord {
    boolean effectedAnomaly;
    Map dsEntry;

    @Override
    public String toString() {
        return "DSRecord{" + "effectedAnomaly=" + effectedAnomaly + ", dsEntry=" + dsEntry.get("ds_row_id") + '}';
    }
}
