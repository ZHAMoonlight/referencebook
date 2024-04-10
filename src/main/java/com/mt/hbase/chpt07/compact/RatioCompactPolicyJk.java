package com.mt.hbase.chpt07.compact;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @author pengxu
 * @Date 2018/12/9.
 */
public class RatioCompactPolicyJk {

    public static List<Integer> applyCompactionPolicy(List<Integer> candidates,
        boolean mayBeStuck) {
        int minFileToCompact = 3;
        int maxFileToCompact = 10;
        int minCompactSize = 100;
        double ratio = 1.2;
        int start = 0;
        // get store file sizes for incremental compacting selection.
        final int countOfFiles = candidates.size();
        long[] fileSizes = new long[countOfFiles];
        long[] sumSize = new long[countOfFiles];
        for (int i = countOfFiles - 1; i >= 0; --i) {
            fileSizes[i] = candidates.get(i);
            // calculate the sum of fileSizes[i,i+maxFilesToCompact-1) for algo
            int tooFar = i + maxFileToCompact - 1;
            sumSize[i] = fileSizes[i] + ((i + 1 < countOfFiles) ? sumSize[i + 1] : 0) - ((tooFar
                < countOfFiles) ? fileSizes[tooFar] : 0);
        }

        while (countOfFiles - start >= minFileToCompact &&
            fileSizes[start] > Math.max(minCompactSize, (sumSize[start + 1] * ratio))) {
            ++start;
        }
        if (start < countOfFiles) {
            System.out.println("Default compaction algorithm has selected " + (countOfFiles - start)
                + " files from " + countOfFiles + " candidates");
        } else if (mayBeStuck) {
            // We may be stuck. Compact the latest files if we can.
            int filesToLeave = candidates.size() - minFileToCompact;
            if (filesToLeave >= 0) {
                start = filesToLeave;
            }
        }
        System.out.println("sumSize=" + Arrays.toString(sumSize));
        System.out.println("start=" + start);
        candidates.subList(0, start).clear();
        System.out.println("fileToCompact =" + candidates);
        return candidates;
    }

    public static void main(String[] args) {
        int[] candidateIntArray = new int[]{8200, 4300, 160, 150, 80, 70, 30, 20, 5};
        List<Integer> candidateList = new LinkedList<Integer>();
        for (Integer candidate : candidateIntArray) {
            candidateList.add(candidate);
        }
        applyCompactionPolicy(candidateList, true);
    }

}
