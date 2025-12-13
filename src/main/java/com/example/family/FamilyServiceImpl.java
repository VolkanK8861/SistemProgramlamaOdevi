package com.example.family;

import family.Empty;
import family.FamilyServiceGrpc;
import family.FamilyView;
import family.NodeInfo;
import family.ChatMessage;
import io.grpc.stub.StreamObserver;

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

        FamilyView view = FamilyView.newBuilder()
                .addAllMembers(registry.snapshot())
                .build();

        responseObserver.onNext(view);
        responseObserver.onCompleted();
    }

    @Override
    public void getFamily(Empty request, StreamObserver<FamilyView> responseObserver) {
        FamilyView view = FamilyView.newBuilder()
                .addAllMembers(registry.snapshot())
                .build();

        responseObserver.onNext(view);
        responseObserver.onCompleted();
    }

    // Diğer düğümlerden broadcast mesajı geldiğinde
    // Liderden veya başka bir üyeden mesaj geldiğinde çalışan fonksiyon burası
    @Override
    public void receiveChat(ChatMessage request, StreamObserver<Empty> responseObserver) {
        // 1. Önce yine ekrana basalım ki çalıştığını görelim (Loglama)
        System.out.println("💬 Incoming message (ID: " + request.getId() + "):");
        System.out.println("  From: " + request.getFromHost() + ":" + request.getFromPort());
        System.out.println("  Text: " + request.getText());
        System.out.println("--------------------------------------");

        // --- DİSKE YAZMA İŞLEMİ (YENİ EKLEDİĞİMİZ KISIM) ---
        // Neden ekledik? Veriler kalıcı olsun, program kapanınca silinmesin diye.
        try {
            // A. Klasör Ayarlaması
            // Herkesin verisi "Storage" klasöründe dursun.
            // Files.createDirectories: Klasör yoksa oluşturur, varsa hata vermez devam eder.
            java.nio.file.Path folder = java.nio.file.Paths.get("Storage");
            if (!java.nio.file.Files.exists(folder)) {
                java.nio.file.Files.createDirectories(folder);
            }

            // B. Dosya İsmi Belirleme
            // Mesajın ID'sini dosya adı yapıyoruz. (Örn: 100.txt)
            // request.getId() -> Proto dosyasına eklediğimiz yeni alan!
            // Eğer ID 0 gelirse (eski sistemden kalma mesajsa) rastgele bir sayı verelim.
            int msgId = request.getId();
            if (msgId == 0) msgId = (int) (System.currentTimeMillis() % 10000);

            String fileName = msgId + ".txt";
            java.nio.file.Path filePath = folder.resolve(fileName);

            // C. Yazma İşlemi
            // Files.write: İçeriği (byte olarak) dosyaya yazar. Dosya yoksa oluşturur.
            java.nio.file.Files.write(filePath, request.getText().getBytes());

            System.out.println("💾 DİSKE KAYDEDİLDİ: " + fileName);

        } catch (Exception e) {
            // Hata Toleransı: Diske yazamazsak (yer yoktur vs) program çökmesin, hatayı yazıp geçsin.
            System.err.println("❌ Dosya yazma hatası: " + e.getMessage());
            e.printStackTrace();
        }
        // --- DİSKE YAZMA BİTTİ ---

        // Lidere "Tamamdır kardeşim, ben mesajı aldım (ve yazdım)" cevabını dönüyoruz.
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    // --- YENİ EKLENEN OKUMA FONKSİYONU ---
    @Override
    public void getMessage(family.GetRequest request,
                           io.grpc.stub.StreamObserver<family.GetResponse> responseObserver) {

        int istenenId = request.getId();
        System.out.println("🔍 Lider veriyi sordu. ID: " + istenenId);

        String okunanVeri = "";
        boolean bulundu = false;

        try {
            // 1. Dosyanın yolunu bul (Storage/100.txt)
            String fileName = istenenId + ".txt";
            java.nio.file.Path filePath = java.nio.file.Paths.get("Storage", fileName);

            // 2. Dosya var mı kontrol et
            if (java.nio.file.Files.exists(filePath)) {
                // 3. Varsa hepsini oku
                byte[] bytes = java.nio.file.Files.readAllBytes(filePath);
                okunanVeri = new String(bytes);
                bulundu = true;
                System.out.println("✅ Dosya bulundu ve okundu: " + fileName);
            } else {
                System.out.println("❌ Dosya yok: " + fileName);
            }

        } catch (Exception e) {
            System.err.println("Okuma hatası: " + e.getMessage());
        }

        // 4. Cevabı hazırla ve Lidere gönder
        family.GetResponse response = family.GetResponse.newBuilder()
                .setText(okunanVeri)
                .setFound(bulundu)
                .setFromNode(self.getHost() + ":" + self.getPort())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
    // -------------------------------------
}
