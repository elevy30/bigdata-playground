package poc.model;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.List;

/**
 * Created by eyallevy on 08/01/17.
 */
public  class Product implements Serializable {
    private Integer id;
    private String name;
    private List<Integer> parents;

    public Product() { }

    public Product(Integer id, String name, List<Integer> parents) {
        this.id = id;
        this.name = name;
        this.parents = parents;
    }

    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public List<Integer> getParents() { return parents; }
    public void setParents(List<Integer> parents) { this.parents = parents; }

    @Override
    public String toString() {
        return MessageFormat.format("Product'{'id={0}, name=''{1}'', parents={2}'}'", id, name, parents);
    }
}
