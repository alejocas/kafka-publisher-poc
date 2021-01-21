package io.confluent.examples.clients.cloud.model;

import io.polygon.kotlin.sdk.rest.reference.TickerNewsDTO;

import java.util.List;

public class TickNewsTransformationDTO {

    private String title;
    private String image;
    private String summary;
    private String url;
    private List<String> symbols;

    public TickNewsTransformationDTO() {
    }

    public TickNewsTransformationDTO(TickerNewsDTO tn) {
        this.title = tn.getTitle();
        this.image = tn.getImage();
        this.summary = tn.getSummary();
        this.url = tn.getUrl();
        this.symbols = tn.getSymbols();
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public List<String> getSymbols() {
        return symbols;
    }

    public void setSymbols(List<String> symbols) {
        this.symbols = symbols;
    }

    public String toString() {
        return new com.google.gson.Gson().toJson(this);
    }
}
