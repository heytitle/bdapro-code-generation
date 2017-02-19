package org.apache.flink.runtime.operators.sort;


public final class SorterTemplateModel<T> {

    String templateName;

    public SorterTemplateModel(){}

    public String createTemplateModel(){
        return "";
    }

    public String getTemplateModel(){
        return "";
    }

    public String getGeneratedFileName(){
        return "";
    }

    public String getTemplateVariable(){
        return "";
    }

    public InMemorySorter<T> getSorter(){
        InMemorySorter<T> sorterObject;
        return sorterObject;
    }
}
