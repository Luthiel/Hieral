package com.moving.dws.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

public class IKUtil {
    public static Set<String> split(String s) {
        Set<String> result = new HashSet<>();
        // 字符流 input
        StringReader reader = new StringReader(s);
        // 分词
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        try {
            Lexeme next = ikSegmenter.next();
            while (next != null) {
                String word = next.getLexemeText();
                result.add(word);
                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}
