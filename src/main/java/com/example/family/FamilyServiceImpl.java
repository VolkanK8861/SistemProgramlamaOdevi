package com.example.family;

import family.ChatMessage;
import family.Empty;
import family.FamilyServiceGrpc;
import family.FamilyView;
import family.NodeInfo;
import family.GetRequest;
import family.GetResponse;
import io.grpc.stub.StreamObserver;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
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

    // --- YAZMA İŞLEMİ (MANUEL BUFFERED - EL EMEĞİ) ---
    @Override
    public void receiveChat(ChatMessage request, StreamObserver<Empty> responseObserver) {
        try {
            System.out.println("📥 [GELEN] ID: " + request.getId());

            // HER ÜYE KENDİ KLASÖRÜNE YAZAR (Storage_5556 gibi)
            String folderName = "Storage_" + self.getPort();
            Path folder = Paths.get(folderName);
            if (!Files.exists(folder)) { Files.createDirectories(folder); }

            String fileName = request.getId() + ".txt";
            Path filePath = folder.resolve(fileName);

            // --- PUAN GETİREN KISIM: MANUEL BUFFER ---
            // 8KB (8192 byte) tampon bellek ile yazıyoruz.
            try (FileOutputStream fos = new FileOutputStream(filePath.toFile());
                 BufferedOutputStream bos = new BufferedOutputStream(fos, 8192)) {

                byte[] veri = request.getText().getBytes();
                bos.write(veri);
                bos.flush(); // Tamponu diske boşalt
            }
            System.out.println("   💾 Yazıldı (Buffered).");

            // --- GERÇEKÇİ RAPOR (Klasördeki Dosyayı Sayar) ---
            long fileCount = 0;
            try (Stream<Path> files = Files.list(folder)) { fileCount = files.count(); }

            System.out.println("   📊 [DİSK RAPORU Node:" + self.getPort() + "] Klasörde " + fileCount + " dosya var.");

        } catch (Exception e) { System.err.println("Hata: " + e.getMessage()); }

        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    // --- OKUMA İŞLEMİ ---
    @Override
    public void getMessage(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        int istenenId = request.getId();
        String okunanVeri = "";
        boolean bulundu = false;
        try {
            // Kendi klasörüne bak
            String folderName = "Storage_" + self.getPort();
            Path filePath = Paths.get(folderName, istenenId + ".txt");

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