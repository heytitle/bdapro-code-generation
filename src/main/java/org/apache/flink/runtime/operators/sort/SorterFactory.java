package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import java.util.List;
import org.codehaus.janino.SimpleCompiler;




public final class SorterFactory<T> {

    String templatePath;

    public SorterFactory(TypeSerializer<T> serializer, TypeComparator<T> comparator, List<MemorySegment> memory) {
        this.createSorter(serializer, comparator, memory);
    }


    public InMemorySorter<T> createSorter(TypeSerializer<T> serializer, TypeComparator<T> comparator, List<MemorySegment> memory) {
        InMemorySorter<T> sorterObject =  new NormalizedKeySorter<T>(serializer, comparator, memory);

        SorterTemplateModel sTModel = new SorterTemplateModel();
        String templateModel = sTModel.getTemplateModel();

        TemplateManager TManage = new TemplateManager();
        templatePath = TManage.getGeneratedCode(templateModel);

        try {
            //create compiler and cook
            SimpleCompiler sortCompiler = new SimpleCompiler();
            sortCompiler.cook(templatePath);

            //reference to the class
            Class<SorterTemplateModel> clazz = (Class<SorterTemplateModel>) Class.forName(sortCompiler.toString());

            //instantiate
            SorterTemplateModel instance = clazz.newInstance();
            sorterObject = instance.getSorter();


        }catch (Exception e) {
            e.printStackTrace();
        }

        return sorterObject;
    }


}
