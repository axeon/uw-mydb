package uw.mydb.util;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 一致性hash实现。
 *
 * @param <T>
 * @author axeon
 */
public class ConsistentHash<T> {
    /**
     * 虚拟节点数。
     */
    private final int numberOfReplicas;

    private final SortedMap<Integer, T> circle = new TreeMap();

    /**
     * hash算法，考虑效率使用murmur3_32.
     */
    private final HashFunction hash = Hashing.murmur3_32();

    public ConsistentHash(int numberOfReplicas,
                          Collection<T> nodes) {
        this.numberOfReplicas = numberOfReplicas;
        for (T node : nodes) {
            add(node);
        }
    }

    /**
     * 增加真实机器节点
     *
     * @param node
     */
    private void add(T node) {
        for (int i = 0; i < this.numberOfReplicas; i++) {
            circle.put(this.hash.hashUnencodedChars(node.toString() + i).asInt(), node);
        }
    }

    /**
     * 删除真实机器节点
     *
     * @param node
     */
    public void remove(T node) {
        for (int i = 0; i < this.numberOfReplicas; i++) {
            circle.remove(this.hash.hashUnencodedChars(node.toString() + i).asInt());
        }
    }

    /**
     * 取得真实机器节点
     *
     * @param key
     * @return
     */
    public T get(String key) {
        if (circle.isEmpty()) {
            return null;
        }
        int code = this.hash.hashUnencodedChars(key).asInt();
        if (!circle.containsKey(code)) {
            // 沿环的顺时针找到一个虚拟节点
            SortedMap<Integer, T> tailMap = circle.tailMap(code);
            code = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        // 返回该虚拟节点对应的真实机器节点的信息
        return circle.get(code);
    }

}