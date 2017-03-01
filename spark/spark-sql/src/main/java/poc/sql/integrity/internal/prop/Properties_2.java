package poc.sql.integrity.internal.prop;

import lombok.Getter;

/**
 * Created by eyallevy on 27/02/17.
 */
@Getter
public class Properties_2 extends Prop{

    public String invalidList = "MismatchList";

    public String id = "Id";
    public String testedColumn = "sc_status";
    public String dataSourcePath = "file:///opt/Dropbox/dev/poc/_resources/data_bitwise/proxy_fixed_bit.csv";
    public String bitwisePath = "file:///opt/Dropbox/dev/poc/_resources/data_bitwise/proxy_fixed_Bitwise.csv";
    public String columnLocationMapPath = "file:///opt/Dropbox/dev/poc/_resources/data_bitwise/columnLocationMap.csv";
    public String dataSourceParquet = "file:///opt/Dropbox/dev/poc/_resources/data_bitwise/proxy_fixed";
    public String idsOnlyPath = "file:///opt/Dropbox/dev/poc/_resources/bigdata/ID.csv";
}
