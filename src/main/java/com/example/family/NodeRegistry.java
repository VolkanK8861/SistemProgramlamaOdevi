package com.example.family;

import family.NodeInfo;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class NodeRegistry {

    // Buradaki 'newKeySet' bazen eski Java sürümlerinde kızarabilir.
    // Eğer kızarırsa: Collections.newSetFromMap(new ConcurrentHashMap<>()) yaparız.
    // Ama şimdilik böyle kalsın, çalışır.
    private final Set<NodeInfo> nodes = ConcurrentHashMap.newKeySet();

    public void add(NodeInfo node) {
        nodes.add(node);
    }

    public void addAll(Collection<NodeInfo> others) {
        nodes.addAll(others);
    }

    public List<NodeInfo> snapshot() {
        // List.copyOf Java 10 ve üstü ister.
        // Eğer JDK 8 kullanıyorsan burası hata verebilir.
        // Hata verirse: return new java.util.ArrayList<>(nodes); yap.
        return List.copyOf(nodes);
    }

    public void remove(NodeInfo node) {
        nodes.remove(node);
    }
}