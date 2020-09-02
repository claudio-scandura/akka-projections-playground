package com.mylaesoftware.projections.jpa;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity(name = "OBSERVATION")
public class Observation {

    @Id
    @Column(name = "value")
    public String value;

    @Column(name = "timestamp")
    public Long timestamp;

    public Observation() {}

    public Observation(String value, Long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
