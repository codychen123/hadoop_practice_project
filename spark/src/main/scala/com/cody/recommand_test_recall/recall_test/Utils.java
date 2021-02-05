package com.cody.recommand_test_recall.recall_test;

import org.apache.spark.ml.linalg.Vector;

import java.util.ArrayList;
import java.util.List;

public class Utils {
    // 计算余弦相似度，两个向量之间的积
    public static double cosineSimilarity(Vector featuresLeft, Vector featuresRight) {
        double[] dataLeft = featuresLeft.toArray();
        List<Float> lista = new ArrayList<>();
        if (dataLeft.length > 0) {
            for (double d : dataLeft) {
                lista.add((float) d);
            }
        }

        double[] dataRight = featuresRight.toArray();
        List<Float> listb = new ArrayList<>();
        if (dataRight.length > 0) {
            for (double d : dataRight) {
                listb.add((float) d);
            }
        }

        float cross=0.0f;
        float t1 = 0.0f;
        float t2=0.0f;
        for (int i = 0; i <lista.size() ; i++) {
            cross+= lista.get(i) * listb.get(i) ;
            t1 +=Math.pow(lista.get(i),2);
            t2 +=Math.pow(listb.get(i),2);
        }

        return cross/(t1*t2);
    }
}
