package com.example.family;

import family.ChatMessage;
import family.Empty;
import family.FamilyServiceGrpc;
import family.FamilyView;
import family.NodeInfo;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class NodeMain {

    private static int TOLERANCE_LEVEL = 1;
    // Liderin Hafızası (Hangi ID kimde?)
    private static Map<Integer, List<NodeInfo>> messageLocations = new ConcurrentHashMap<>();

    // Şahsi Sayaç (Ben kaç tane yazdım?)
    public static AtomicLong LOCAL_STORAGE_COUNT = new AtomicLong(0);

    private static final Map<String, ManagedChannel> channelCache = new ConcurrentHashMap<>();
    private static final int START_PORT = 5555;

    // Rapor Sıklığı: 3 Saniye
    private static final int REPORT_INTERVAL_SECONDS = 3;

    public static void main(String[] args) throws Exception {
        System.out.println("⚙️ Sistem başlatılıyor...");
        try {
            Path path = Paths.get("tolerance.conf");
            if (Files.exists(path)) {
                List<String> lines = Files.readAllLines(path);
                for (String line : lines) {
                    if (line.startsWith("tolerance=")) {
                        TOLERANCE_LEVEL = Integer.parseInt(line.split("=")[1].trim());
                        System.out.println("✅ tolerance.conf okundu. Hedef: " + TOLERANCE_LEVEL);
                    }
                }
            } else { System.out.println("⚠️ Ayar dosyası yok, varsayılan (1)."); }
        } catch (Exception e) {}

        String host = "127.0.0.1";
        int port = findFreePort(START_PORT);

        NodeInfo self = NodeInfo.newBuilder().setHost(host).setPort(port).build();
        NodeRegistry registry = new NodeRegistry();
        FamilyServiceImpl service = new FamilyServiceImpl(registry, self);

        Server server = ServerBuilder.forPort(port).addService(service).build().start();
        System.out.printf("Node started on %s:%d%n", host, port);

        if (port == START_PORT) {
            startLeaderTextListener(registry, self);
            // LİDER RAPORU (FULL PAKET)
            startLeaderFullReport(registry);
        } else {
            // ÇOCUK RAPORU
            startMemberReport(port);
        }

        discoverExistingNodes(host, port, registry, self);
        startHealthChecker(registry, self);
        server.awaitTermination();
    }

    // --- LİDERİN KRAL RAPORU (GÜNCELLENDİ) ---
    private static void startLeaderFullReport(NodeRegistry registry) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                // 1. Hafızadaki Bilgi
                int totalGlobal = messageLocations.size();
                int activeMembers = registry.snapshot().size();

                // 2. Liderin Şahsi Emeği
                long myPersonalCount = LOCAL_STORAGE_COUNT.get();

                // 3. (YENİ) Gerçek Fiziksel Klasör Sayımı
                long totalPhysicalFiles = 0;
                Path sharedStorage = Paths.get("Storage");
                if (Files.exists(sharedStorage)) {
                    try (Stream<Path> files = Files.list(sharedStorage)) { totalPhysicalFiles = files.count(); }
                }

                System.out.println("\n================== [ LİDER RAPORU ] ==================");
                System.out.println("🌍 SİSTEM İNDEKSİ : " + totalGlobal + " Mesaj (RAM)");
                System.out.println("👥 AKTİF ÜYE      : " + activeMembers + " Adet");
                System.out.println("------------------------------------------------------");
                System.out.println("🏠 LİDERİN KASASI : " + myPersonalCount + " Dosya (Ben Yazdım)");
                System.out.println("📂 TÜM STORAGE    : " + totalPhysicalFiles + " Dosya (Herkesin Toplamı)");
                System.out.println("======================================================\n");

            } catch (Throwable t) {}
        }, 5, REPORT_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    // --- ÇOCUK RAPORU ---
    private static void startMemberReport(int myPort) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                long myCount = LOCAL_STORAGE_COUNT.get();
                System.out.println("💾 [RAPOR - ÜYE " + myPort + "] Ben toplam " + myCount + " mesaj kaydettim.");
            } catch (Throwable e) {}
        }, 5, REPORT_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }


    private static ManagedChannel getChannel(String host, int port) {
        String key = host + ":" + port;
        return channelCache.computeIfAbsent(key, k ->
                ManagedChannelBuilder.forAddress(host, port).usePlaintext().build()
        );
    }

    private static void startLeaderTextListener(NodeRegistry registry, NodeInfo self) {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(6666)) {
                System.out.printf("Leader listening for text on TCP %s:%d%n", self.getHost(), 6666);
                while (true) {
                    Socket client = serverSocket.accept();
                    new Thread(() -> handleClientTextConnection(client, registry, self)).start();
                }
            } catch (IOException e) { System.err.println("Listener Error: " + e.getMessage()); }
        }, "LeaderTextListener").start();
    }

    private static void handleClientTextConnection(Socket client, NodeRegistry registry, NodeInfo self) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
             PrintWriter outToClient = new PrintWriter(client.getOutputStream(), true)) {

            String line;
            while ((line = reader.readLine()) != null) {
                String text = line.trim();
                if (text.isEmpty()) continue;

                String[] parts = text.split("\\s+", 3);
                if (parts.length < 1) continue;
                String command = parts[0].toUpperCase();

                if (command.equals("SET")) {
                    if (parts.length >= 3) {
                        try {
                            int msgId = Integer.parseInt(parts[1]);
                            String data = parts[2];
                            long ts = System.currentTimeMillis();

                            System.out.println("➕ [SET] İstek -> ID: " + msgId);

                            List<NodeInfo> allMembers = registry.snapshot();
                            List<NodeInfo> targets = new ArrayList<>(allMembers);
                            Collections.shuffle(targets);
                            int count = 0;
                            List<NodeInfo> successfulNodes = new ArrayList<>();
                            ChatMessage msg = ChatMessage.newBuilder().setId(msgId).setText(data).setFromHost(self.getHost()).setFromPort(self.getPort()).setTimestamp(ts).build();

                            for (NodeInfo target : targets) {
                                if (count >= TOLERANCE_LEVEL) break;
                                if (sendMessageToNode(target, msg)) { successfulNodes.add(target); count++; }
                            }
                            messageLocations.put(msgId, successfulNodes);
                            outToClient.println("OK");
                        } catch (Exception e) { outToClient.println("ERROR"); }
                    } else { outToClient.println("ERROR"); }
                }
                else if (command.equals("GET")) {
                    if (parts.length >= 2) {
                        try {
                            int searchId = Integer.parseInt(parts[1].trim());
                            System.out.println("🔍 [GET] İstek -> ID: " + searchId);
                            boolean found = false;
                            List<NodeInfo> members = registry.snapshot();
                            for (NodeInfo member : members) {
                                try {
                                    System.out.println("   ❓ Soruluyor: " + member.getPort());
                                    ManagedChannel channel = getChannel(member.getHost(), member.getPort());
                                    FamilyServiceGrpc.FamilyServiceBlockingStub stub = FamilyServiceGrpc.newBlockingStub(channel);
                                    family.GetResponse response = stub.getMessage(family.GetRequest.newBuilder().setId(searchId).build());
                                    if (response.getFound()) {
                                        System.out.println("   ✅ [BULUNDU] Kaynak: " + member.getPort());
                                        outToClient.println("SUCCESS: " + response.getText());
                                        found = true; break;
                                    }
                                } catch (Exception e) {
                                    System.out.println("   ⚠️ Üye cevap vermedi: " + member.getPort());
                                }
                            }
                            if (!found) {
                                System.out.println("   ❌ Kimse bulamadı.");
                                outToClient.println("ERROR: Veri bulunamadi.");
                            }
                        } catch (Exception e) { outToClient.println("ERROR"); }
                    }
                }
            }
        } catch (IOException e) {}
    }

    private static boolean sendMessageToNode(NodeInfo target, ChatMessage msg) {
        try {
            ManagedChannel channel = getChannel(target.getHost(), target.getPort());
            FamilyServiceGrpc.FamilyServiceBlockingStub stub = FamilyServiceGrpc.newBlockingStub(channel);
            stub.receiveChat(msg);
            System.out.println("   -> Gönderildi: " + target.getPort());
            return true;
        } catch (Exception e) { return false; }
    }

    private static int findFreePort(int startPort) {
        int port = startPort;
        while (true) {
            try (ServerSocket ignored = new ServerSocket(port)) { return port; } catch (IOException e) { port++; }
        }
    }

    private static void discoverExistingNodes(String host, int selfPort, NodeRegistry registry, NodeInfo self) {
        for (int port = START_PORT; port < selfPort; port++) {
            ManagedChannel channel = null;
            try {
                channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
                FamilyServiceGrpc.FamilyServiceBlockingStub stub = FamilyServiceGrpc.newBlockingStub(channel);
                FamilyView view = stub.join(self);
                registry.addAll(view.getMembersList());
                System.out.printf("Joined through %s:%d%n", host, port);
            } catch (Exception ignored) {} finally { if (channel != null) channel.shutdownNow(); }
        }
    }

    private static void startFamilyPrinter(NodeRegistry registry, NodeInfo self) {}

    private static void startHealthChecker(NodeRegistry registry, NodeInfo self) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(() -> {
            List<NodeInfo> members = registry.snapshot();
            for (NodeInfo n : members) {
                if (n.getHost().equals(self.getHost()) && n.getPort() == self.getPort()) continue;
                try {
                    ManagedChannel channel = getChannel(n.getHost(), n.getPort());
                    FamilyServiceGrpc.FamilyServiceBlockingStub stub = FamilyServiceGrpc.newBlockingStub(channel);
                    stub.getFamily(Empty.newBuilder().build());
                } catch (Exception e) {
                    System.out.printf("Node %s:%d unreachable, removing%n", n.getHost(), n.getPort());
                    registry.remove(n);
                }
            }
        }, 5, 10, TimeUnit.SECONDS);
    }
}