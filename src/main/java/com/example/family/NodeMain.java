package com.example.family;

import family.Empty;
import family.FamilyServiceGrpc;
import family.FamilyView;
import family.NodeInfo;
import family.ChatMessage;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;         // EKLENDİ
import java.util.Collections;       // EKLENDİ
import java.util.List;              // EKLENDİ
import java.util.Map;


import java.io.IOException;
import java.net.ServerSocket;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.*;
import java.io.PrintWriter;


public class NodeMain {

    private static final int START_PORT = 5555;
    private static final int PRINT_INTERVAL_SECONDS = 10;
    private static int TOLERANCE_LEVEL = 1; // Varsayılan 1 olsun, dosyadan okuyamazsa patlamasın// Liderin Hafızası (Defteri)
    private static Map<Integer, List<NodeInfo>> messageLocations = new ConcurrentHashMap<Integer, List<NodeInfo>>();

    public static void main(String[] args) throws Exception {

        // A) ÖNCE AYAR DOSYASINI OKU
        System.out.println("⚙️ Sistem başlatılıyor...");
        try {
            java.nio.file.Path path = java.nio.file.Paths.get("tolerance.conf");
            if (java.nio.file.Files.exists(path)) {
                java.util.List<String> lines = java.nio.file.Files.readAllLines(path);
                for (String line : lines) {
                    if (line.startsWith("tolerance=")) {
                        String deger = line.split("=")[1].trim();
                        TOLERANCE_LEVEL = Integer.parseInt(deger);
                        System.out.println("✅ tolerance.conf okundu. Hedef: " + TOLERANCE_LEVEL + " üye.");
                    }
                }
            } else {
                System.out.println("⚠️ tolerance.conf bulunamadı! Varsayılan (1) ile devam ediliyor.");
            }
        } catch (Exception e) {
            System.out.println("⚠️ Dosya okuma hatası: " + e.getMessage());
        }

        // B) SONRA NORMAL BAŞLATMA İŞLEMLERİ (Senin eski kodun)
        String host = "127.0.0.1";
        int port = findFreePort(START_PORT);

        NodeInfo self = NodeInfo.newBuilder()
                .setHost(host)
                .setPort(port)
                .build();

        NodeRegistry registry = new NodeRegistry();
        FamilyServiceImpl service = new FamilyServiceImpl(registry, self);

        Server server = ServerBuilder
                .forPort(port)
                .addService(service)
                .build()
                .start();

        System.out.printf("Node started on %s:%d%n", host, port);

        // Eğer bu ilk node ise (port 5555), TCP 6666'da text dinlesin
        if (port == START_PORT) {
            startLeaderTextListener(registry, self);
        }

        discoverExistingNodes(host, port, registry, self);
        startFamilyPrinter(registry, self);
        startHealthChecker(registry, self);

        server.awaitTermination();
    }

    private static void startLeaderTextListener(NodeRegistry registry, NodeInfo self) {
    // Sadece lider (5555 portlu node) bu methodu çağırmalı
    new Thread(() -> {
        try (ServerSocket serverSocket = new ServerSocket(6666)) {
            System.out.printf("Leader listening for text on TCP %s:%d%n",
                    self.getHost(), 6666);

            while (true) {
                Socket client = serverSocket.accept();
                new Thread(() -> handleClientTextConnection(client, registry, self)).start();
            }

        } catch (IOException e) {
            System.err.println("Error in leader text listener: " + e.getMessage());
        }
    }, "LeaderTextListener").start();
}

private static void handleClientTextConnection(Socket client,
                                               NodeRegistry registry,
                                               NodeInfo self) {
    System.out.println("New TCP client connected: " + client.getRemoteSocketAddress());
    try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(client.getInputStream()))) {

        String line;
        while ((line = reader.readLine()) != null) {
            String text = line.trim();
            if (text.isEmpty()) continue;

            System.out.println("[RAW] Received: " + text);

            // Split into max 3 parts
            // Example: "SET 100 This is a message"
            // parts[0]="SET", parts[1]="100", parts[2]="This is a message"
            String[] parts = text.split(" ", 3);
            String command = parts[0].toUpperCase();

            if (command.equals("SET")) {
                // Format: SET <id> <message>
                if (parts.length >= 3) {
                    String id = parts[1];
                    String data = parts[2];
                    System.out.println(">> SET COMMAND DETECTED");
                    System.out.println("   ID  : " + id);
                    System.out.println("   DATA: " + data);

                    // --- DAĞITIM MANTIĞI EKLENDİ ---
                    try {
                        int messageId = Integer.parseInt(id);
                        long ts = System.currentTimeMillis();

                        ChatMessage msg = ChatMessage.newBuilder()
                                .setId(messageId)
                                .setText(data)
                                .setFromHost(self.getHost())
                                .setFromPort(self.getPort())
                                .setTimestamp(ts)
                                .build();

                        // Lider bu mesajı tüm üyelere (kendisi hariç) gönderir
                        broadcastToFamily(registry, self, msg);

                        // Liderin kendisi de diske yazsın istiyorsak buraya manuel ekleme yapabiliriz
                        // Ama şimdilik üyelerin yazması yeterli.

                    } catch (NumberFormatException e) {
                        System.out.println("!! HATA: ID bir sayı olmalı! (Örn: 100)");
                    } catch (Exception e) {
                        System.out.println("!! HATA: Mesaj gönderilemedi: " + e.getMessage());
                    }
                    // --------------------------------

                } else {
                    System.out.println("!! ERROR: Invalid SET format. Use: SET <id> <data>");
                }
            }

            else if (command.equals("GET")) {
                // Format: GET <id>
                if (parts.length >= 2) {
                    String idStr = parts[1].trim(); // trim() ekledik
                    System.out.println(">> GET COMMAND DETECTED");
                    System.out.println("   ID: " + idStr);

                    PrintWriter outToClient = new PrintWriter(client.getOutputStream(), true);

                    try {
                        int searchId = Integer.parseInt(idStr);
                        boolean found = false;

                        List<NodeInfo> members = registry.snapshot();

                        // --- DÖNGÜ BAŞLIYOR ---
                        for (NodeInfo member : members) {

                            // BURADAKİ "continue" ENGELİNİ KALDIRDIK!
                            // Artık Lider kendisine de gRPC isteği atıp soracak.

                            System.out.println("❓ Soruluyor: " + member.getPort());

                            ManagedChannel channel = null;
                            try {
                                channel = ManagedChannelBuilder
                                        .forAddress(member.getHost(), member.getPort())
                                        .usePlaintext()
                                        .build();

                                FamilyServiceGrpc.FamilyServiceBlockingStub stub =
                                        FamilyServiceGrpc.newBlockingStub(channel);

                                family.GetRequest request = family.GetRequest.newBuilder()
                                        .setId(searchId)
                                        .build();

                                family.GetResponse response = stub.getMessage(request);

                                if (response.getFound()) {
                                    System.out.println("✅ BULUNDU! Kaynak: " + member.getPort());
                                    System.out.println("📄 İÇERİK: " + response.getText());

                                    outToClient.println("SUCCESS: " + response.getText());
                                    found = true;
                                    break;
                                }

                            } catch (Exception e) {
                                System.out.println("⚠️ Üye cevap vermedi: " + member.getPort());
                            } finally {
                                if (channel != null) channel.shutdown();
                            }
                        }
                        // --- DÖNGÜ BİTTİ ---

                        if (!found) {
                            outToClient.println("ERROR: Veri bulunamadi.");
                            System.out.println("❌ Kimse bulamadı.");
                        }

                    } catch (Exception e) {
                        outToClient.println("ERROR: Hata: " + e.getMessage());
                    }

                } else {
                    System.out.println("!! ERROR: Invalid GET format. Use: GET <id>");
                }
            }
            else {
                System.out.println("!! UNKNOWN COMMAND: " + command);
            }
        }

    } catch (IOException e) {
        System.err.println("TCP client handler error: " + e.getMessage());
    } finally {
        try { client.close(); } catch (IOException ignored) {}
    }
}

    private static void broadcastToFamily(NodeRegistry registry,
                                          NodeInfo self,
                                          ChatMessage msg) {

        List<NodeInfo> members = registry.snapshot();

        for (NodeInfo n : members) {
            // ARTIK KENDİMİZİ ATLAMİYORUZ, HERKESE GÖNDERİYORUZ

            ManagedChannel channel = null;
            try {
                channel = ManagedChannelBuilder
                        .forAddress(n.getHost(), n.getPort())
                        .usePlaintext()
                        .build();

                FamilyServiceGrpc.FamilyServiceBlockingStub stub =
                        FamilyServiceGrpc.newBlockingStub(channel);

                stub.receiveChat(msg);

                System.out.printf("Broadcasted message to %s:%d%n", n.getHost(), n.getPort());

            } catch (Exception e) {
                System.err.printf("Failed to send to %s:%d (%s)%n",
                        n.getHost(), n.getPort(), e.getMessage());
            } finally {
                if (channel != null) channel.shutdownNow();
            }
        }
    }


    private static int findFreePort(int startPort) {
        int port = startPort;
        while (true) {
            try (ServerSocket ignored = new ServerSocket(port)) {
                return port;
            } catch (IOException e) {
                port++;
            }
        }
    }

    private static void discoverExistingNodes(String host,
                                              int selfPort,
                                              NodeRegistry registry,
                                              NodeInfo self) {

        for (int port = START_PORT; port < selfPort; port++) {
            ManagedChannel channel = null;
            try {
                channel = ManagedChannelBuilder
                        .forAddress(host, port)
                        .usePlaintext()
                        .build();

                FamilyServiceGrpc.FamilyServiceBlockingStub stub =
                        FamilyServiceGrpc.newBlockingStub(channel);

                FamilyView view = stub.join(self);
                registry.addAll(view.getMembersList());

                System.out.printf("Joined through %s:%d, family size now: %d%n",
                        host, port, registry.snapshot().size());

            } catch (Exception ignored) {
            } finally {
                if (channel != null) channel.shutdownNow();
            }
        }
    }

    private static void startFamilyPrinter(NodeRegistry registry, NodeInfo self) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        scheduler.scheduleAtFixedRate(() -> {
            List<NodeInfo> members = registry.snapshot();
            System.out.println("======================================");
            System.out.printf("Family at %s:%d (me)%n", self.getHost(), self.getPort());
            System.out.println("Time: " + LocalDateTime.now());
            System.out.println("Members:");

            for (NodeInfo n : members) {
                boolean isMe = n.getHost().equals(self.getHost()) && n.getPort() == self.getPort();
                System.out.printf(" - %s:%d%s%n",
                        n.getHost(),
                        n.getPort(),
                        isMe ? " (me)" : "");
            }
            System.out.println("======================================");
        }, 3, PRINT_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private static void startHealthChecker(NodeRegistry registry, NodeInfo self) {
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    scheduler.scheduleAtFixedRate(() -> {
        List<NodeInfo> members = registry.snapshot();

        for (NodeInfo n : members) {
            // Kendimizi kontrol etmeyelim
            if (n.getHost().equals(self.getHost()) && n.getPort() == self.getPort()) {
                continue;
            }

            ManagedChannel channel = null;
            try {
                channel = ManagedChannelBuilder
                        .forAddress(n.getHost(), n.getPort())
                        .usePlaintext()
                        .build();

                FamilyServiceGrpc.FamilyServiceBlockingStub stub =
                        FamilyServiceGrpc.newBlockingStub(channel);

                // Ping gibi kullanıyoruz: cevap bizi ilgilendirmiyor,
                // sadece RPC'nin hata fırlatmaması önemli.
                stub.getFamily(Empty.newBuilder().build());

            } catch (Exception e) {
                // Bağlantı yok / node ölmüş → listeden çıkar
                System.out.printf("Node %s:%d unreachable, removing from family%n",
                        n.getHost(), n.getPort());
                registry.remove(n);
            } finally {
                if (channel != null) {
                    channel.shutdownNow();
                }
            }
        }

    }, 5, 10, TimeUnit.SECONDS); // 5 sn sonra başla, 10 sn'de bir kontrol et
}

}
