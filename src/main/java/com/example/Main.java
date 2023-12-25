package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionList;

public class Main {
    public static void main(String[] args) {
        var pipeline = Pipeline.create();
        var countriesCollection = pipeline.apply(
            "Reading from list",
            Create.of("Colombia", "Francia", "Estados Unidos", "Bolivia")
        );
        
        var countriesBeginWithC = countriesCollection.apply(
            "Filtering By C", ParDo.of(
                new DoFn<String,String>() {
                    @ProcessElement
                    public void processElement(@Element String elem, ProcessContext c) {
                        if (elem.startsWith("C")) {
                            c.output(elem);
                        }
                    }
                }
            )
        );

        var countriesBeginWitthB = countriesCollection.apply(
            "Filtering By B", ParDo.of(
                new DoFn<String,String>() {
                    @ProcessElement
                    public void processElement(@Element String elem, ProcessContext c) {
                        if (elem.startsWith("B")) {
                            c.output(elem);
                        }
                    }
                }
            )
        );

        var mergedCollectionWithFlatten = 
        PCollectionList.of(countriesBeginWithC).and(countriesBeginWitthB)
            .apply(Flatten.pCollections());
        
        mergedCollectionWithFlatten.apply(
            TextIO.write().to("extractwords").withoutSharding()
        );

        pipeline.run();
    }
}