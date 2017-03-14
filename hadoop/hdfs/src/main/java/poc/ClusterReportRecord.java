package poc;


import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Created by dgilboa on 14/02/2017.
 */
@Data
@AllArgsConstructor
public class ClusterReportRecord implements Serializable{

    String cluster_identifier;
    String anomaly_identifier;
    Long tr_identifier;
}
