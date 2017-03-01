package poc.model;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.List;

/**
 * Created by eyallevy on 08/01/17.
 */
public  class Proxy extends EntityId implements Serializable {

    private String cs_username;
    private String date;
    private Timestamp time;
    private int sc_status;
    private double time_taken;
    private double sc_bytes;
    private double cs_bytes;
    private String cs_host;
    private String s_action;
    private String cs_method;

    public Proxy() {
        super(-1);
    }


    public Proxy(int id, String cs_username, String date, Timestamp time, int sc_status, double time_taken, double sc_bytes, double cs_bytes, String cs_host, String s_action, String cs_method) {
        super(id);
        this.cs_username = cs_username;
        this.date = date;
        this.time = time;
        this.sc_status = sc_status;
        this.time_taken = time_taken;
        this.sc_bytes = sc_bytes;
        this.cs_bytes = cs_bytes;
        this.cs_host = cs_host;
        this.s_action = s_action;
        this.cs_method = cs_method;
    }

    public String getCs_username() {
        return cs_username;
    }

    public void setCs_username(String cs_username) {
        this.cs_username = cs_username;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }

    public int getSc_status() {
        return sc_status;
    }

    public void setSc_status(int sc_status) {
        this.sc_status = sc_status;
    }

    public double getTime_taken() {
        return time_taken;
    }

    public void setTime_taken(double time_taken) {
        this.time_taken = time_taken;
    }

    public double getSc_bytes() {
        return sc_bytes;
    }

    public void setSc_bytes(double sc_bytes) {
        this.sc_bytes = sc_bytes;
    }

    public double getCs_bytes() {
        return cs_bytes;
    }

    public void setCs_bytes(double cs_bytes) {
        this.cs_bytes = cs_bytes;
    }

    public String getCs_host() {
        return cs_host;
    }

    public void setCs_host(String cs_host) {
        this.cs_host = cs_host;
    }

    public String getS_action() {
        return s_action;
    }

    public void setS_action(String s_action) {
        this.s_action = s_action;
    }

    public String getCs_method() {
        return cs_method;
    }

    public void setCs_method(String cs_method) {
        this.cs_method = cs_method;
    }

    @Override
    public String toString() {
        return MessageFormat.format("Product'{'id={0}, name=''{1}'', parents={2}'}'", super.getId(), cs_username, cs_host);
    }
}
