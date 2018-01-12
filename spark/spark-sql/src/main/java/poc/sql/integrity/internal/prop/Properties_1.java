package poc.sql.integrity.internal.prop;


import lombok.Getter;

/****
 * Created by eyallevy on 27/02/17.
 ****/
@Getter
public class Properties_1 extends Prop{

    public String invalidList  = "MismatchList";
    public String id           = "Id";
    public String testedColumn = "T1351";

    private String rootPath = "file:///opt/Dropbox/dev/git-hub/poc/_resources/bigdata";
    public String csvPath               = rootPath + "/QR_500K.csv";
    public String dataSourceIdPath      = rootPath + "/QR_500K_ID.csv";
    public String idsOnlyPath           = rootPath + "/ID.csv";
    public String bitwisePath           = rootPath + "/QR_500K_Bitwise.csv";
    public String columnLocationMapPath = rootPath + "/columnLocationMap.csv";

    public String dataSourceParquet     = rootPath + "/QR_500K_ID";
}
