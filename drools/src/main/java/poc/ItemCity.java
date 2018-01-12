package poc;

import java.math.BigDecimal;

/**
 * Created by eyallevy on 22/01/17.
 */
public class ItemCity {

    public enum City {
        PUNE, NAGPUR
    }

    public enum Type {
        GROCERIES, MEDICINES, WATCHES, LUXURYGOODS
    }

    private City purchaseCity;
    private BigDecimal sellPrice;
    private Type typeofItem;
    private BigDecimal localTax;

    public City getPurchaseCity() {
        return purchaseCity;
    }

    public void setPurchaseCity(City purchaseCity) {
        this.purchaseCity = purchaseCity;
    }

    public BigDecimal getSellPrice() {
        return sellPrice;
    }

    public void setSellPrice(BigDecimal sellPrice) {
        this.sellPrice = sellPrice;
    }

    public Type getTypeofItem() {
        return typeofItem;
    }

    public void setTypeofItem(Type typeofItem) {
        this.typeofItem = typeofItem;
    }

    public BigDecimal getLocalTax() {
        return localTax;
    }

    public void setLocalTax(BigDecimal localTax) {
        this.localTax = localTax;
    }
}
