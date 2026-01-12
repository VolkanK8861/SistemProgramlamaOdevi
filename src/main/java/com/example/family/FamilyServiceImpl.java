package com.example.family;

import family.ChatMessage;
import family.Empty;
import family.FamilyServiceGrpc;
import family.FamilyView;
import family.NodeInfo;
import family.GetRequest;
import family.GetResponse;
import io.grpc.stub.StreamObserver;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class FamilyServiceImpl extends FamilyServiceGrpc.FamilyServiceImplBase {

    private final NodeRegistry registry;
    private final NodeInfo self;

    public FamilyServiceImpl(NodeRegistry registry, NodeInfo self) {
        this.registry = registry;
        this.self = self;
        this.registry.add(self);
    }

    @Override
    public void join(NodeInfo request, StreamObserver<FamilyView> responseObserver) {
        registry.add(request);
        FamilyView view = FamilyView.newBuilder().addAllMembers(registry.snapshot()).build();
        responseObserver.onNext(view);
        responseObserver.onCompleted();
    }

    @Override
    public void getFamily(Empty request, StreamObserver<FamilyView> responseObserver) {
        FamilyView view = FamilyView.newBuilder().addAllMembers(registry.snapshot()).build();
        responseObserver.onNext(view);
        responseObserver.onCompleted();
    }

    // --- YAZMA (SET) ---
    @Override
    public void receiveChat(ChatMessage request, StreamObserver<Empty> responseObserver) {
        try {
            System.out.println("📥 [GELEN] ID: " + request.getId());

            Path folder = Paths.get("Storage");
            if (!Files.exists(folder)) { Files.createDirectories(folder); }

            String fileName = request.getId() + ".txt";
            Path filePath = folder.resolve(fileName);

            Files.write(filePath, request.getText().getBytes());
            System.out.println("   💾 Yazıldı.");

            // 1. HERKES (ÇOCUKLAR VE LİDER) KENDİ SAYISINI YAZAR
            long myCount = NodeMain.LOCAL_STORAGE_COUNT.incrementAndGet();
            System.out.println("   📊 [ŞAHSİ RAPOR] Şu ana kadar " + myCount + " dosya tutuyorum.");

            // 2. SADECE LİDER (5555) İSE, ALTINA TOPLAM DEPOYU DA YAZAR
            if (self.getPort() == 5555) {
                try (Stream<Path> files = Files.list(folder)) {
                    long totalCount = files.count();
                    System.out.println("   📂 [TÜM DEPO]    Klasörde toplam " + totalCount + " dosya var.");
                }
            }

        } catch (Exception e) { System.err.println("Hata: " + e.getMessage()); }

        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    // --- OKUMA (GET) ---
    @Override
    public void getMessage(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        int istenenId = request.getId();
        String okunanVeri = "";
        boolean bulundu = false;
        try {
            Path filePath = Paths.get("Storage", istenenId + ".txt");
            if (Files.exists(filePath)) {
                byte[] bytes = Files.readAllBytes(filePath);
                okunanVeri = new String(bytes);
                bulundu = true;
            }
        } catch (Exception e) {}

        GetResponse response = GetResponse.newBuilder()
                .setText(okunanVeri).setFound(bulundu).setFromNode(self.getHost() + ":" + self.getPort()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}