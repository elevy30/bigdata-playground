package poc.model;

import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table("Person")
public class Person {
    @PrimaryKey
    private Integer pId;

    private String name;

    public Integer getpId() {
        return pId;
    }

    public void setpId(Integer pId) {
        this.pId = pId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Person [pId=" + pId + ", name=" + name + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((pId == null) ? 0 : pId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Person other = (Person) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (pId == null) {
            if (other.pId != null)
                return false;
        } else if (!pId.equals(other.pId))
            return false;
        return true;
    }


//    CREATE TABLE users (
//            userid text PRIMARY KEY,
//            first_name text,
//            last_name text,
//            emails set<text>,
//            top_scores list<int>,
//            todo map<timestamp, text>
//    );

}
