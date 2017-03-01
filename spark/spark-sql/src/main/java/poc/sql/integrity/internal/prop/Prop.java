package poc.sql.integrity.internal.prop;

import lombok.Getter;

import java.io.Serializable;

/**
 * Created by eyallevy on 27/02/17.
 */
@Getter
public abstract class Prop implements Serializable{
     String invalidList = "MismatchList";

     String id = "Id";
     String testedColumn = "cs_username";
     String dataSourcePath = "file:///opt/Dropbox/dev/poc/_resources/data_bitwise/proxy_fixed.csv";
     String bitwisePath = "file:///opt/Dropbox/dev/poc/_resources/data_bitwise/proxy_fixed_Bitwise.csv";
     String columnLocationMapPath = "file:///opt/Dropbox/dev/poc/_resources/data_bitwise/columnLocationMap.csv";
     String dataSourceParquet = "file:///opt/Dropbox/dev/poc/_resources/data_bitwise/proxy_fixed";
     String idsOnlyPath = "file:///opt/Dropbox/dev/poc/_resources/bigdata/ID.csv";

}
