package poc.sql.dataloader;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * Created by haimcohen on 14/08/2016.
 */
@Accessors(chain = true)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataFieldItem implements Serializable {

    private String name;

    private String type;

    private String trType;

    private boolean loadable;

    private int position;
    
    private boolean pk;
    
    private boolean analysis;

    private String dateFormat;

}
