---
translation_status: translated
---

# Artefakt depolaması { #artifact-storage }

GPUStack, büyük ve özel JSON artefaktlarını S3 uyumlu bir bucket'ta saklar. Benchmark
izleme arşivleri bu katmanı kullanır; tarayıcı istemcilerine hiçbir zaman depolama
kimlik bilgileri verilmez. PostgreSQL/MySQL meta verileri ile nesne depolaması
birlikte yedeklenmelidir.

## Modlar { #modes }

- `builtin`: paketle gelen Linux konteyneri, aynı düğümde **SeaweedFS 4.54**'ü başlatır.
  Paketli sunucularda varsayılan budur. `artifacts/builtin` dahil olmak üzere GPUStack
  veri biriminin tamamını (varsayılan olarak `/var/lib/gpustack`) kalıcı olarak saklayın.
- `external`: paylaşılan bir S3 hizmetini ve mevcut bir bucket'ı kullanır. GPUStack
  bucket'ı oluşturmaz; politikasını, şifrelemesini, yaşam döngüsünü veya genel erişimini değiştirmez.
- `disabled`: yeni artefakt yazma işlemlerini durdurur. Açıkça korunan okuma profilleri,
  okuyucular tarafından kullanılmaya devam eder. Kaynaktan veya wheel paketinden
  kurulumlarda ve çalışanlarda varsayılan budur.

Birden fazla sunucudan oluşan dağıtımlar `external` modunu seçmeli ve aynı bucket
ile ad alanını paylaşmalıdır. Paketle gelen tek düğümlü hizmet, depolama çoğaltması
veya yüksek kullanılabilirlik (HA) garantisi sunmaz. Kimlik bilgileri bir kez rastgele
üretilir, kalıcı birimde özel dosyalar olarak saklanır ve yeniden başlatmalardan sonra
tekrar kullanılır. S3 ve dahili uç noktaları loopback adresine bağlanır;
ek bir konteyner portunun dışarıya açılması gerekmez.

## Yapılandırma { #configuration }

Sunucuyu bir YAML dosyası, `GPUSTACK_` ortam değişkenleri veya CLI bayraklarıyla
(örneğin `--artifact-storage-mode external`) yapılandırın. YAML anahtarlarında alt çizgi,
bayraklarda kısa çizgi, ortam değişkeni adlarında ise büyük harf ve alt çizgi kullanılır.

```yaml
artifact_storage_mode: external
artifact_storage_profile_id: monitoring-eu-v1
artifact_storage_endpoint: https://s3.example.com # omit for AWS endpoint discovery
artifact_storage_bucket: gpustack-private
artifact_storage_prefix: installations/production
artifact_storage_region: eu-west-1
artifact_storage_addressing_style: path # auto, path, virtual
artifact_storage_tls_verify: true
# artifact_storage_ca_file: /etc/ssl/private/s3-ca.pem
```

Kimlik bilgileri açıkça belirtilmediğinde SDK'nın varsayılan AWS kimlik bilgisi
zinciri kullanılır. Bu zincir, ortamdan alınan kimlik bilgilerini, paylaşılan AWS
yapılandırmasını ve SDK'nın desteklediği örnek, konteyner veya web kimliği bilgilerini
destekler. Gerçek bulut kimliği zincirleri hedef dağıtımda doğrulanmalıdır. Açıkça
belirtilen kimlik bilgileri için `artifact_storage_access_key_id`,
`artifact_storage_secret_access_key` ve isteğe bağlı `artifact_storage_session_token`
kullanılır. Bunları korumalı yapılandırma veya gizli ortam değişkeni enjeksiyonu
aracılığıyla sağlayın. Bu değerler GPUStack Config dökümlerine ve nesnenin metin
gösterimine dahil edilmez. Kimlik bilgilerini uç nokta URL'sine yerleştirmeyin.

TLS doğrulaması, özel uç noktalar da dahil olmak üzere varsayılan olarak etkindir.
Özel bir CA belirtmek için `artifact_storage_ca_file` kullanın. Sertifika doğrulamasını
devre dışı bırakmak için açıkça `artifact_storage_tls_verify: false` ayarı veya
`--no-artifact-storage-tls-verify` bayrağı gerekir.

| Ayar | Varsayılan | Anlamı |
| --- | --- | --- |
| `artifact_storage_connect_timeout` | 5 saniye | SDK bağlantı zaman aşımı |
| `artifact_storage_read_timeout` | 30 saniye | SDK okuma zaman aşımı |
| `artifact_storage_max_attempts` | 3 | İlk deneme dahil toplam SDK denemesi sayısı |
| `artifact_storage_max_connections` | 8 | SDK bağlantı havuzu boyutu |
| `artifact_storage_max_concurrency` | 4 | Profil ve sunucu olay döngüsü başına devam eden çağrı sayısı |
| `artifact_storage_max_object_bytes` | 16777216 | Tek bir belgenin kodu çözüldükten sonraki en büyük bayt sayısı |
| `artifact_storage_max_total_bytes` | 0 (sınırsız) | Kurulumun tamamı için başlangıçtaki sıkıştırılmış nesne kapasitesi; paylaşılan veritabanı politikasını bir kez başlatır |
| `artifact_storage_builtin_port` | 18333 | Yerleşik modda loopback S3 portu |
| `benchmark_archive_retention_days` | ayarlanmamış | Benchmark saklama süresini izler; isteğe bağlı arşiv sona erme süresi, gün cinsinden |
| `benchmark_archive_sample_step_seconds` | 15 | Arşiv toplayıcısının örnekleme ızgarası |
| `benchmark_archive_flush_interval_seconds` | 60 | Arşivin depolamaya aktarılma sıklığı |
| `benchmark_archive_settle_seconds` | 30 | Kaynak metriklerin oturması için tanınan süre |
| `benchmark_archive_max_concurrency` | 2 | Arşiv işlerinin eşzamanlılık bütçesi |
| `benchmark_archive_max_run_bytes` | 268435456 | Yürütme başına en büyük arşiv bayt sayısı |

Saklama ve toplayıcı ayarlarını Benchmark arşiv alanı kullanır; S3 bağdaştırıcısı
boyutu sınırı aşan bir nesneyi hiçbir zaman sessizce kesmez. Bucket'ın kendi saklama
politikasını uygulamanın saklama politikasıyla uyumlu yapılandırın; böylece bucket
yaşam döngüsü, dizinde hâlâ kayıtlı olan nesneleri silmez.

## İzleme nesnelerinin adları { #monitoring-object-names }

Yeni izleme nesnelerinin adları, değişmez bir UUID'den önce kapsamlarını belirtir:

```text
resources-<uuid>.json.gz
runtime-aggregate-<uuid>.json.gz
runtime-prefill-instances-<uuid>.json.gz
phase-1024-1-runtime-decode-instances-<uuid>.json.gz
```

Dizin zaten kurulumu, sahibi, Benchmark'ı, yürütmeyi ve izleme biçimini belirttiği
için dosya adları bu alanları tekrarlamaz. Önek; çalışma zamanı veya kaynak verilerini,
belirli rolleri ve toplu ya da örnek bazındaki görünümleri belirtir. Kaydedilen
aşamalar, aşama kimliklerini ekler; çalışmanın tamamına ait dosyalarda `run` yer almaz.
Varsayılan `all` rolü ve kaynak kapsamının varsayılan toplu görünümü de yazılmaz.
Bir dosya birden fazla metrik içerir. Bileşenler, uzunluğu sınırlandırılmış ASCII
etiketlerine dönüştürülür; yetkili kapsam kaynağı veritabanı olmaya devam eder.
UUID, farklı veri yakalamalarının birbirinin üzerine yazmasını önler.

Yalnızca UUID içeren mevcut adlar değişmeden kalır ve okunmaya devam eder.
Okuyucular, taşıma ve temizleme işlemleri dizinde kayıtlı nesne anahtarının tamamını
kullanır; yolları hiçbir zaman UUID'den yeniden oluşturmazlar. Dosya adı, etkin
revizyonu belirtmez ve örneklerin eksiksiz olduğunu garanti etmez. Bu özellikler
paylaşılan arşiv dizininde tutulmaya devam eder.

## Okunabilir revizyon manifestleri { #readable-revision-manifests }

Bir arşiv `ready`, `partial` veya `no_data` durumunda mühürlendikten sonra, ayrı
bir arka plan döngüsü yayımlanan her revizyon için bir dizin oluşturur:

```text
installations/<installation>/owners/<owner>/benchmarks/<id>/runs/<run>/
  monitoring/v1/manifests/revision-000001.json
```

Bunlar girintili, sıkıştırılmamış gerçek UTF-8 JSON dosyalarıdır; dolayısıyla bir S3
veya Filer tarayıcısı bunları görüntüleyebilir. İzleme eğrileri bağımsız gzip nesneleri
olarak kalır. Eski mühürlenmiş arşivlerin manifestleri, Prometheus sorgulanmadan veya
izleme verileri değiştirilmeden, yayımlanmış veritabanı dizinlerinden geriye dönük
olarak oluşturulur. Devam eden veri yakalamaları mühürlenene kadar bekler; isteğe
bağlı dizin yazma işlemleri, sürmekte olan veri yakalama döngüsünü engellemez.

Her belge değişmez revizyonunu belirtir ve kapsamları, aşamaları, rolleri, görünümleri,
metrik adlarını ve birimlerini, örnek kapsamını, nesne anahtarlarını, boyutları ve
SHA-256 değerlerini listeler. Desteklenen metriklerin ayrıntılı meta verileri bulunur;
`unavailable_metrics`, desteklenmeyen ve uygulanamayan anahtarları nedenleriyle
birlikte kısa gruplar hâlinde toplar. Desteklendiği hâlde örneği bulunmayan metrikler
ayrı tutulur. Zaman değerleri Unix saniyelerini korur ve okunabilir UTC zaman
damgalarını da içerir. `revision_state`, geçerli iş durumunu değil, kaydedilen
revizyonun durumunu açıklar. Ham istemler, kimlik bilgileri, başlatma komutları,
ortam değişkenleri veya özel kaynak adresleri dizine kopyalanmaz.

`stored_bytes` değerleri, dizin ilk hazırlandığında sabitlenir. Daha sonra yapılacak
bir profil taşıma işlemi, verileri koruyarak farklı bir gzip kodlaması kullanabilir.
Eğri nesneleri için `checksum_scope: uncompressed_json_bytes`, SHA-256'nın indirilen
`.json.gz` dosyasına değil, kodu çözülmüş JSON baytlarına ait olduğunu belirtir.
Manifestin kendisinde ise sağlama toplamı, dosyanın gerçek UTF-8 JSON baytlarını kapsar.

Etkin revizyonu ve manifestinin kaydının kesinleştirilip kesinleştirilmediğini veritabanı
belirler; değiştirilebilir bir `latest.json` işaretçisi yoktur. Bir yükleme, başarısız
bir veritabanı onayından önce tamamlanmış olabilir; bu nedenle yalnızca dosyanın
varlığı, manifest başvurusunun kaydının kesinleştirildiğini kanıtlamaz. Kimlik doğrulamalı
normal `GET /v2/benchmarks/<id>/monitoring-archive` yanıtı bir `manifest` nesnesi içerir:

```json
{
  "state": "ready",
  "archive_revision": 1,
  "filename": "revision-000001.json",
  "size_bytes": 12345,
  "sha256": "<sha256>",
  "checksum_scope": "utf8_json_bytes"
}
```

Aynı alan `pending`, güvenli bir `last_error` ile birlikte `failed` veya `unavailable`
durumunu da bildirir. Özel bucket veya profil yapılandırmasını ya da tam nesne
anahtarlarını açığa çıkarmaz. Kapasite nedeniyle reddedilme dahil bir dizin hatası,
veri yakalama durumunu değiştirmez veya önceden kaydedilmiş grafikleri geçersiz
kılmaz; arka plandaki yeniden denemeler işlemi daha sonra tamamlayabilir.

Manifestler paylaşılan bayt defterini kullanır; profil taşımaya, bütünlük denetimlerine,
Benchmark silme işlemlerine ve arşiv süresinin dolmasına dahildir. İzlemenin çalışma
başına bayt bütçesini tüketmezler. Sonucu bilinmeyen yüklemeler, dizindeki işlem
niyetini ve rezervasyonunu korur; sorunsuz bir yeniden denemeyle durumları uzlaştırılır.
Güncel olmayan bir temizleme işleminin yeniden denenmiş bir yayını silmesini önlemek
için sabit manifest anahtarları olağan sahipsiz nesne ve kapasite temizliğine dahil
edilmez; arşivin açıkça silinmesi veya süresinin dolması yine de bunların tümünü
kaldırır. Reddedilmiş ve hiç denenmemiş bir yükleme, sahipsiz nesne bırakmaz.

## Genel kapasite { #global-capacity }

Kapasite, tüm Sunucular ve yapılandırılmış depolama profilleri genelinde bu GPUStack
kurulumu için geçerlidir. Korunan arşiv revizyonları, yayımlanmamış nesneler ve profil
taşıma sırasındaki her iki kopya dahil, yönetilen nesnelerin sıkıştırılmış baytlarını
sayar. S3 sonucu henüz doğrulanmamış yazma işlemleri için ayrılan alanı da sayar.
Bucket'taki diğer dosyalar, nesne deposu çoğaltma yükü, saklanan S3 nesne sürümleri
ve SeaweedFS'nin henüz geri kazanmadığı dosya sistemi alanı bu hesaba dahil değildir.

Varsayılan **sınırsız** kapasitedir. Politikanın ilk başlatmadaki başlangıç değerini
belirlemek için sunucu başlatılırken YAML'da `artifact_storage_max_total_bytes`,
ortamda `GPUSTACK_ARTIFACT_STORAGE_MAX_TOTAL_BYTES` veya
`--artifact-storage-max-total-bytes` bayrağını ayarlayın. Değerler tam sayı olarak
bayt cinsindendir; `0` sınırsız anlamına gelir. Yapılandırılabilen en büyük desteklenen
sınır `9007199254740991` bayttır. Tüm Sunucular aynı başlangıç yapılandırmasını
kullanmalıdır. Başlatmadan sonra paylaşılan veritabanı politikası yetkilidir:
bir Sunucuyu farklı bir başlangıç değeriyle yeniden başlatmak, yöneticinin kaydettiği
politikanın üzerine yazmaz.

Platform yöneticileri, Sunucuları yeniden başlatmadan `GET` /
`PUT /v2/artifact-storage/capacity` üzerinden etkin politikayı inceleyebilir veya
değiştirebilir; PUT, sınırsız kapasite için `{"max_bytes": 0}` ya da pozitif bir bayt
sınırı kabul eder. Yanıt `used_bytes`, `reserved_bytes`, `total_bytes`,
`remaining_bytes`, `usage_percent` ve `state` alanlarını içerir. Sınırsız modda kalan
bayt ve yüzde değerleri null olarak döner. Sınırlı mod %80'de uyarır ve sınıra
ulaşıldığında veya sınır aşıldığında `full` durumuna geçer.

CLI aynı paylaşılan politikayı sunar:

```sh
gpustack artifact-storage --config-file /etc/gpustack/config.yaml capacity
gpustack artifact-storage --config-file /etc/gpustack/config.yaml \
  capacity --max-bytes 107374182400
gpustack artifact-storage --config-file /etc/gpustack/config.yaml \
  capacity --max-bytes 0
```

Rezervasyonlar, yüklemeden önce paylaşılan veritabanında sıralı olarak işlenir.
Kapasiteye sığmayan bir yazma işlemi `storage_capacity_exceeded` bildirir; geçmiş
verileri sessizce çıkaramaz veya arşivi kesemez. Mevcut arşivler okunabilir kalır;
Benchmark isteklerinin yürütülmesi ve istemci istatistikleri bağımsız olarak devam
eder. Sınırı yükseltmek, sınırsız kapasiteyi seçmek veya yönetilen arşivleri silmek,
bekleyen veri yakalama işleminin devam etmesini sağlayabilir. Kaçırılan bir kaynak
aralığı yalnızca Prometheus onu hâlâ saklıyorsa geri alınabilir. Sınırın mevcut
kullanımın altına indirilmesine izin verilir; bu, ek alan ayırmayı engeller ve mevcut
verileri silmez.

Alan ancak S3 silme işlemini onayladıktan sonra serbest bırakılır. Başarısız veya
kesintiye uğramış yüklemeler, uzlaştırma veya temizleme sonucu doğrulayana kadar
tedbir amaçlı rezervasyonlarını korur. Mevcut nesneler ilk kullanımda deftere alınır;
sıkıştırılmış boyutu bilinmeyen eski işlem niyeti nesneleri veya taşıma kopyaları
için sıkıştırılmamış boyutlarına dayanan ihtiyatlı bir rezervasyon kullanılır. Bu,
ayrılmış alanın geçici olarak gerçek depolanan bayt sayısından fazla görünmesine
yol açabilir. Bu mekanizma, tüm bucket'ın kullanımını taramaz veya S3 yaşam döngüsü
politikasını yönetmez. Bucket sürümleme, nesne kilidi ve fiziksel disk alanının
geri kazanılması depolama yöneticisinin sorumluluğunda kalır.

## Veri yakalama durumu ve kaynak kapsamı { #capture-status-and-source-coverage }

`capture_complete`, sabitlenmiş arşiv revizyonunu açıklar. `ready` ve `no_data`
revizyonları için true; veri yakalama hataları, geçersiz kılınmış kimlikler veya
kullanılamayan ölçüm kapsamları içeren bir `partial` revizyonu için false değerindedir.
`archive_state`, istemci eski bir revizyonu okumaya devam ederken değişebilen
geçerli iş durumunu bildirir.

`data_coverage`, kaydedilen kaynak gözlemlerini ayrıca açıklar. Kısa bir aşamanın ilk
örnekleme anı ilk metrik toplama işleminden önceyse veya kayan gecikme penceresi çok
kısaysa, aşama `capture_complete: true` ve `data_coverage: partial` ile `ready`
durumunda olabilir. Bu koşullarda metrik durumları, null değerleri, sıfır değerleri
ve kapsam istatistikleri korunur; başarılı bir veri yakalama işlemini yeniden denemek
gerekmez. Sorgu hataları, yeniden denenebilir veri yakalama hataları olarak kalır.
Başarılı bir yeniden deneme yeni bir revizyon yayımlar ve sabitlenmiş revizyonu
okuyan istemciler için önceki revizyonu korur.

## Depolama profilleri ve değişiklikler { #storage-profiles-and-changes }

Bir profil kimliği, bir uç noktayı, bucket'ı, öneki ve bölgeyi kalıcı olarak tanımlar.
Bağdaştırıcı, `artifacts/profiles` altında bir parmak izi eşlemesi saklar ve bu
kimliğin farklı bir hedef için yeniden kullanılmasını reddeder. Arşiv dizinleri de
profil kimliğini korur. Aynı hedefin kimlik bilgilerinin yenilenmesi yeni bir kimlik
gerektirmez.

Hedefi değiştirirken yeni bir yazma profili kimliği seçin ve eski bağlantıyı
`artifact_storage_read_profiles` altında koruyun. Profil girdileri, aynı ayarları
`artifact_storage_` öneki olmadan kullanır. Bu eşlemeyi YAML veya
`GPUSTACK_ARTIFACT_STORAGE_READ_PROFILES` JSON ortam değişkeniyle yapılandırın.

```yaml
artifact_storage_profile_id: monitoring-eu-v2
artifact_storage_bucket: new-private-bucket
artifact_storage_read_profiles:
  monitoring-eu-v1:
    endpoint: https://old-s3.example.com
    bucket: gpustack-private
    prefix: installations/production
    region: eu-west-1
    addressing_style: path
    # Credentials may be omitted to use the SDK credential chain.
```

Bağımlı tüm arşivler taşınana veya silinene kadar eski profilleri ve nesnelerini
erişilebilir tutun. Harici veya devre dışı moda geçmek, paketle gelen SeaweedFS
hizmetini durdurur; önce yerel nesneleri taşıyın veya eski verileri okumak için
ayrıca erişilebilen bir hizmeti koruyun. Dosyaları yalnızca yeni bir bucket'a kopyalamak
arşiv dizinlerini yeniden yazmaz. Değişmiş bir hedefin kontrollerden geçmesini
sağlamak amacıyla profil eşlemelerini asla kaldırmayın.

## Gerekli S3 işlemleri { #required-s3-operations }

Bağdaştırıcı, SigV4 ile `HeadObject`, `PutObject`, `GetObject` ve `DeleteObject`
işlemlerini kullanır. Çalışma zamanına yalnızca yapılandırılmış ad alanına erişim
verin. Desteklendiğinde `If-None-Match: *` içeren koşullu `PutObject` kullanılır.
`artifact_storage_conditional_writes: false`, taşınabilir S3 davranışını açıkça seçer;
bağdaştırıcı, koşulun desteklenmediğini açıkça belirten bir yanıtta da bu davranışa
geri döner. İzin hataları ve eşzamanlılık çakışmaları hiçbir zaman bu geri dönüşü
tetiklemez. Taşınabilir yazma işlemleri mevcut içeriği kontrol eder ancak HEAD ile
PUT işlemlerini atomik hâle getiremez: çağıranlar, Benchmark arşivleyicisinin yaptığı
gibi benzersiz nesne anahtarları ve veritabanı fencing mekanizmasıyla korunan
yayımlama kullanmalıdır. Aynı işlemin yeniden denenmesinde saklanan sağlama toplamı
doğrulanır. ETag değerleri opaktır ve hiçbir zaman MD5 sağlama toplamı olarak ele alınmaz.

Salt okunur bağlantı kontrolü `HeadBucket` kullanır; AWS'de bunun için `s3:ListBucket`
izni gerekir. Bu nedenle reddedilen bir kontrol, nesne işlemlerine izin verilse bile
sağlık kontrolü izninin eksik olduğunu gösterebilir. Kontrol hiçbir zaman bucket
oluşturmaz veya veri değiştirmez. Yalnızca yerleşik modun başlatılması, yönettiği
bucket için `CreateBucket` kullanır.

Her belge bağımsız olarak gzip ile sıkıştırılır ve standartlaştırılmış, sıkıştırılmamış
UTF-8 JSON'un SHA-256 değerini taşır. Okuma işlemleri saklanan meta verileri ve
dizindeki beklenen sağlama toplamını doğrular, hem sıkıştırılmış hem de kodu çözülmüş
baytları sınırlar ve yanıt akışını kapatır. Tüm SDK G/Ç işlemleri ve sıkıştırma,
olay döngüsünün dışında çalışır. Hatalar, SDK istisna gövdelerini veya kimlik
bilgilerini içermeyen, kararlı ve güvenli nedenler sunar.

Depolama bağlantısı bayrakları, ortam değişkenleri ve YAML ayarları
[CLI yapılandırma başvurusunda](../cli-reference/start.md#artifact-storage-options)
açıklanmıştır. Harici mod, mevcut bir S3 uyumlu hizmete bağlanır; GPUStack bu hizmetin
replikalarını, meta veri veritabanını veya kullanılabilirliğini dağıtmaz ya da yönetmez.

Paketle gelen hizmet, HTTP portları 18333 (S3), 19333 (master), 18080 (volume),
18888 (filer) ile dahili gRPC portları 29333, 28080, 28888'i kullanır. Tümü loopback
adresine bağlanır. Yalnızca S3 portu yapılandırılabilir. Master verileri, nesne verileri,
Filer LevelDB ve kimlik bilgilerinin tamamı `artifacts/builtin` altında bulunur;
birimin kısmen yedeklenmesi yeterli değildir.

## Doğrulama kapsamı { #verification-scope }

Birim testleri ağ bağlantısı olmadan çalışır; bayt sınırlarını, sağlama toplamlarını,
değişmez yeniden denemeleri ve yarışan yazıcıları, güvenli hataları, yapılandırmayı,
kimlik bilgilerinin kalıcılığını, devre dışı modu, okuma profillerini ve SDK
işlemlerinin olay döngüsü dışına aktarılmasını kapsar. SeaweedFS 4.54 ile yapılan
yerel bir temel işlev testi; S3 yükleme, indirme, yeniden başlatma sonrası kalıcılık,
sağlama toplamı doğrulaması ve silme işlemlerini doğrular. Her iki mimaride Linux
imaj derlemeleri ile AWS veya diğer sağlayıcıların kimlik, TLS, izin, performans
ve hata senaryoları dağıtım düzeyinde doğrulama gerektirir.

Sabitlenen ikili dosyanın bayrakları ve sağlama toplamları,
[resmî 4.54 sürümünü](https://github.com/seaweedfs/seaweedfs/releases/tag/4.54),
[sunucu komutunu](https://github.com/seaweedfs/seaweedfs/blob/4.54/weed/command/server.go)
ve [S3 yapılandırma belgelerini](https://github.com/seaweedfs/seaweedfs/wiki/Amazon-S3-API) esas alır.

## Profil taşıma ve bütünlük denetimi { #profile-migration-and-integrity-audit }

Yönetici CLI'si, yapılandırılmış veritabanı bağlantısını doğrudan kullanır. Mevcut
bir şemayı açar; tablo oluşturmaz, veritabanı geçişlerini çalıştırmaz, kimlik
doğrulama sağlayıcılarını başlatmaz veya depolama hizmetlerini çalıştırmaz. Komutu
çalıştırmadan önce hem eski hem de hedef profilleri yapılandırın. Veritabanı ile
nesnelerin birbiriyle tutarlı bir yedeğini saklayın.

Seçilen bir yürütmeyi yükleme veya veritabanı değişikliği yapmadan önizleyin:

```sh
gpustack artifact-storage --config-file /etc/gpustack/config.yaml \
  migrate-profile --source-id monitoring-eu-v1 --target-id monitoring-eu-v2 \
  --benchmark-id 7 --run-id RUN_UUID --dry-run --manifest /secure/migration-plan.json
```

Aynı seçimi uygulamak için `--apply` kullanın. Her iki eylem bayrağı da atlanırsa
yine deneme çalıştırması yapılır. `--archive-id`, `--benchmark-id` ve `--run-id`
seçimi daraltabilir; `--limit`, tek bir çağrının işleyeceği arşiv sayısını sınırlar
(varsayılan 100, en fazla 1000 arşiv). Komut yalnızca `ready`, `partial` veya `no_data`
durumundaki arşivleri işler ve etkin kiralaması olan bir arşivi sahiplenmeyi reddeder.
Prometheus'tan geriye dönük veri doldurmaz veya tamamlanmamış ya da başarısız veri
yakalamalarını taşımaz.

Taşıma işlemi her arşiv için mevcut veritabanı kiralamasını alır, fencing token
değerini artırır ve yüklemeden önce planlanan kaynak ve hedef nesne başvurularını
kalıcı olarak kaydeder. Hedef profilde sabit nesne anahtarlarını yeniden kullanır,
kaynak ve hedefin SHA-256 değerlerini doğrular, ilerlemeyi nesne başına kontrol
noktalarıyla kaydeder ve tüm nesneler doğrulamayı geçtikten sonra arşivin ve
yayımlanmış parçalarının profil kimliklerini tek bir fencing korumalı işlemde
değiştirir. Kapsam kimlikleri, arşiv revizyonları, zaman damgaları ve veri sağlama
toplamları değişmeden kalır. Böylece yayımlanmış revizyonlar aynı verileri yeniden
sunar. Farklı arşivlerin işlemleri bağımsız olarak kesinleştirilir; başarısız olan
bir toplu işlemde bile tamamlanmış arşivler bulunabilir.

Kesintiden sonra aynı komutu yeniden çalıştırın. Tamamlanan arşivler artık kaynak
profille eşleşmez; tamamlanmamış kopyalar ise kalıcı olarak kaydedilmiş ilerlemelerinden
devam eder. Kiralamanın kaybedilmesi yayımlamayı engeller ve kiralama süresi
dolduktan sonra (en fazla üç dakika) başka bir süreç devam edebilir. Eksik bir hedef
kopya, doğrulanmış kaynağından yeniden oluşturulur; bozuk veya çakışan bir kopya,
yöneticinin incelemesi için taşımayı durdurur. Kaynak kopyalar, başarılı taşıma
sonrası da dahil olmak üzere işlem boyunca bozulmadan kalır.

Korunan kaynak kopyalar ve tamamlanmamış hedef kopyalar, arşivin taşıma geçmişinde
dizinlenmiş olarak kalır. Normal Benchmark silme işlemi veya yapılandırılmış arşiv
süresinin dolması, aynı yeniden denenebilir GC işiyle bunları temizler. Temizleme
tamamlanana kadar depolama profillerini yapılandırılmış hâlde tutun. Kaynağı hemen
temizleyen örtük bir işlem veya bucket genelinde temizleme komutu yoktur. Geçici
olarak iki kopya saklamak için gereken kapasiteyi planlayın.

Depolamayı veya veritabanı satırlarını değiştirmeden, geçerli dizinde kayıtlı
nesneleri denetleyin:

```sh
gpustack artifact-storage --config-file /etc/gpustack/config.yaml \
  audit --profile-id monitoring-eu-v2 --manifest /secure/integrity-report.json
```

`check`, `audit` komutunun diğer adıdır. Aynı arşiv, çalışma ve Benchmark filtreleri
ile sınır burada da geçerlidir. Rapor; eksik nesneleri, sağlama toplamı uyuşmazlıklarını,
kullanılamayan profilleri ve güvenli depolama hata nedenlerini belirtir; veri yüklerini
veya kimlik bilgilerini içermez. Çıkış durumu başarı için 0, denetim sorun bulduğunda
2, işlem başarısızlığında ise 1'dir. `--manifest` sağlandığında raporlar özel erişim
izinleriyle yazılır. Denetim, tüm bucket'ı taramak veya dizinde kayıtlı olmayan hangi
nesnelerin silinebileceğini tahmin etmek yerine, yayımlanmış ve dizinde kayıtlı
nesneleri kontrol eder.
