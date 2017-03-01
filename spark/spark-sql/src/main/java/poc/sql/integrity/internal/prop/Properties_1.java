package poc.sql.integrity.internal.prop;


import lombok.Getter;

/**
 * Created by eyallevy on 27/02/17.
 */
@Getter
public class Properties_1 extends Prop{

    public String invalidList = "MismatchList";

    public String id = "ID";
    public String testedColumn = "T1351";
    public String csvPath = "file:///opt/Dropbox/dev/poc/_resources/bigdata/QR_500K.csv";
    public String dataSourcePath = "file:///opt/Dropbox/dev/poc/_resources/bigdata/QR_500K_ID.csv";
    public String bitwisePath = "file:///opt/Dropbox/dev/poc/_resources/bigdata/QR_500K_Bitwise.csv";
    public String columnLocationMapPath = "file:///opt/Dropbox/dev/poc/_resources/bigdata/columnLocationMap.csv";
    public String dataSourceParquet = "file:///opt/Dropbox/dev/poc/_resources/bigdata/QR_500K_ID";
    public String idsOnlyPath = "file:///opt/Dropbox/dev/poc/_resources/bigdata/ID.csv";
}
