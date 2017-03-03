package poc.sql.integrity.internal.prop;

import lombok.Getter;

/**
 * Created by eyallevy on 27/02/17.
 */
@Getter
public class Properties_home extends Prop{

    private String rootPath = "file:///opt/Dropbox/dev/git-hub/poc/_resources/data/bitwise";

    public String invalidList = "MismatchList";

    public String id = "Id";
    public String testedColumn = "sc_status";
    public String dataSourceIdPath        = rootPath + "/proxy_fixed_bit.csv";
    public String bitwisePath           = rootPath + "/proxy_fixed_Bitwise.csv";
    public String columnLocationMapPath = rootPath + "/columnLocationMap.csv";
    public String dataSourceParquet     = rootPath + "/proxy_fixed";
    public String idsOnlyPath           = rootPath + "/ID.csv";
}
