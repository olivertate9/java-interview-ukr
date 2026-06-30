[Питання для співбесіди](../README.md)

# Apache Kafka
* [Що таке Apache Kafka?](#Що-таке-Apache-Kafka)
* [Основні компоненти Kafka](#Основні-компоненти-Kafka)

**Архітектура компонентів**

* Topic
  * [Архітектура топіка](#Архітектура-топіка)
  * [Налаштування топіка Kafka](#Налаштування-топіка-Kafka)
* Broker
  * [Архітектура брокера](#Архітектура-брокера)
  * [Налаштування брокера Kafka](#Налаштування-брокера-Kafka)
* Producer
  * [Архітектура продюсера](#Архітектура-продюсера)
  * [Налаштування продюсера](#Налаштування-продюсера)
  * [Приклад конфігурації Kafka Producer](#Приклад-конфігурації-Kafka-Producer)
* Consumer
  * [Архітектура консюмера](#Архітектура-консюмера)
  * [Налаштування консюмера](#Налаштування-консюмера)
  * [Приклад конфігурації Kafka Consumer](#Приклад-конфігурації-Kafka-Consumer)

**Kafka API**

* [Основні API Kafka](#Основні-API-Kafka)
* [Яка роль Producer API?](#Яка-роль-Producer-API)
* [Яка роль Consumer API?](#Яка-роль-Consumer-API)
* [Яка роль Connector API?](#Яка-роль-Connector-API)
* [Яка роль Streams API?](#Яка-роль-Streams-API)
* [Яка роль Transactions API?](#Яка-роль-Transactions-API)
* [Яка роль Quota API?](#Яка-роль-Quota-API)
* [Яка роль AdminClient API?](#Яка-роль-AdminClient-API)

**Kafka Consumer**

* [Навіщо потрібен координатор групи?](#Навіщо-потрібен-координатор-групи)
* [Навіщо потрібен Consumer heartbeat thread?](#Навіщо-потрібен-Consumer-heartbeat-thread)
* [Як Kafka обробляє повідомлення?](#Як-Kafka-обробляє-повідомлення)
* [Як Kafka обробляє затримку консюмера?](#Як-Kafka-обробляє-затримку-консюмера)
* [Навіщо потрібні методи subscribe() і poll()?](#Навіщо-потрібні-методи-subscribe-і-poll)
* [Навіщо потрібен метод position()?](#Навіщо-потрібен-метод-position)
* [Навіщо потрібні методи commitSync() і commitAsync()?](#Навіщо-потрібні-методи-commitSync-і-commitAsync)

**Інші питання**

* [Навіщо потрібен ідемпотентний продюсер?](#Навіщо-потрібен-ідемпотентний-продюсер)
* [Навіщо потрібен інтерфейс Partitioner?](#Навіщо-потрібен-інтерфейс-Partitioner)
* [Навіщо потрібен Broker log cleaner thread?](#Навіщо-потрібен-Broker-log-cleaner-thread)
* [Навіщо потрібен Kafka Mirror Maker?](#Навіщо-потрібен-Kafka-Mirror-Maker)
* [Навіщо потрібен Schema Registry?](#Навіщо-потрібен-Schema-Registry)
* [Навіщо потрібен Streams DSL?](#Навіщо-потрібен-Streams-DSL)
* [Як Kafka забезпечує версіонування повідомлень?](#Як-Kafka-забезпечує-версіонування-повідомлень)
* [Як споживачі отримують повідомлення від брокера?](#Як-споживачі-отримують-повідомлення-від-брокера)

**Порівняння з іншими компонентами та системами**

* [У чому різниця між Kafka Consumer і Kafka Streams?](#У-чому-різниця-між-Kafka-Consumer-і-Kafka-Streams)
* [У чому різниця між Kafka Streams і Apache Flink?](#У-чому-різниця-між-Kafka-Streams-і-Apache-Flink)
* [У чому різниця між Kafka і Flume?](#У-чому-різниця-між-Kafka-і-Flume)
* [У чому різниця між Kafka і RabbitMQ?](#У-чому-різниця-між-Kafka-і-RabbitMQ)


## Що таке Apache Kafka?

Ментальна модель: Kafka — це **розподілений, реплікований лог подій**, що зберігається на диску. Не «черга, з якої повідомлення зникає після прочитання», а «журнал, який можна перечитувати з будь-якого місця». Це з відкритим вихідним кодом розподілена платформа для потокової передачі даних, розрахована на високу пропускну здатність великих обсягів даних з мінімальною затримкою.

### Переваги

* Персистентність даних (повідомлення зберігаються на диску)
* Висока пропускна здатність
* Незалежність пайплайнів обробки (різні консюмери читають той самий топік незалежно)
* Можливість перечитати історію записів заново (за рахунок offset)
* Гнучкість у використанні

### Коли використовувати

* λ-архітектура (Lambda) або k-архітектура (Kappa)
* Стримінг великих даних
* Багато клієнтів (producer і consumer)
* Потрібне горизонтальне масштабування

### Чого в Kafka немає «з коробки»

Kafka — це не класичний брокер повідомлень у стилі AMQP, тож деякі звичні для черг можливості відсутні або потребують додаткової реалізації:

* Це не брокер повідомлень у класичному сенсі (модель — лог, а не черга)
* Відкладені повідомлення (delayed messages)
* DLQ (dead letter queue) — реалізується вручну
* AMQP / MQTT (протоколи не підтримуються нативно)
* TTL на окреме повідомлення (retention налаштовується на рівні топіка, а не повідомлення)
* Черги з пріоритетами

[до змісту](#apache-kafka)

## Основні компоненти Kafka

* **Producer (Виробник)** — застосунок, який публікує повідомлення в топіки Kafka
* **Consumer (Споживач)** — застосунок, який підписується на топіки і читає повідомлення
* **Broker (Брокер)** — сервер Kafka, який приймає, зберігає й роздає повідомлення. У кластері Kafka може бути кілька брокерів
* **Topic (Топік)** — логічний поділ, за яким організовуються дані. Виробники надсилають повідомлення в топіки, а споживачі читають із них
* **Partition (Партиція/Розділ)** — кожен топік поділений на партиції для паралельної обробки. Повідомлення впорядковані тільки в межах однієї партиції (глобального впорядкування на весь топік немає)
* **Zookeeper** — сервіс, який Kafka історично використовувала для управління станом кластера й координації брокерів.
  Однак у нових версіях Kafka відмовляється від Zookeeper на користь власного механізму метаданих **KRaft** (Kafka Raft).
  KRaft — це нова внутрішня архітектура метаданих Kafka, що усуває залежність від Zookeeper. Вона базується на консенсусі Raft,
  дозволяючи брокерам Kafka самостійно керувати метаданими й координувати взаємодію між собою. KRaft став production-ready у Kafka 3.3 (2022),
  а починаючи з Kafka 4.0 (2025) Zookeeper повністю прибрано — режим KRaft є єдиним.

[до змісту](#apache-kafka)

## Архітектура топіка

* **Топік розбито на партиції** — повідомлення в топіку розподіляються по партиціях для ефективнішої паралельної обробки та зберігання
* **Партиції зберігаються на диску** — Kafka зберігає дані на диск, що дозволяє довготривало тримати повідомлення
* **Партиції діляться на сегменти** — сегмент є звичайним файлом на диску; сегменти діляться на пасивні й активний.
  Запис відбувається в активний сегмент
* **Дані видаляються або за часом, або за розміром**. Видалення відбувається посегментно, починаючи з найстарішого сегмента
  * **retention.bytes** — за максимальним розміром
  * **retention.ms** — за часом
* **Повідомлення можна швидко знайти за його offset** — кожному повідомленню в партиції присвоюється унікальний послідовний індекс (offset), за яким легко знайти повідомлення

[до змісту](#apache-kafka)

## Налаштування топіка Kafka

### Реплікація

* `replication.factor`
  * **Опис**: кількість реплік для кожної партиції топіка
  * **Приклад**: `replication.factor=3`
* `min.insync.replicas`
  * **Опис**: мінімальна кількість синхронізованих реплік
  * **Приклад**: `min.insync.replicas=2`

### Зберігання даних

* `retention.ms`
  * **Опис**: час зберігання повідомлень у топіку в мілісекундах
  * **Приклад**: `retention.ms=604800000` (7 днів)
* `retention.bytes`
  * **Опис**: максимальний обсяг даних у топіку (на партицію), після перевищення якого старі повідомлення видаляються
  * **Приклад**: `retention.bytes=10737418240` (10 GB)
* `segment.bytes`
  * **Опис**: розмір сегмента логів топіка
  * **Приклад**: `segment.bytes=1073741824` (1 GB)

### Політики очищення

* `cleanup.policy`
  * **Опис**: як Kafka обробляє старі повідомлення
  * **Значення**: `delete`, `compact` (можна комбінувати: `compact,delete`)
  * **Приклад**: `cleanup.policy=delete`

### Партиції

* `num.partitions`
  * **Опис**: кількість партицій у топіку
  * **Приклад**: `num.partitions=3`

[до змісту](#apache-kafka)

## Архітектура брокера

* **У кожної партиції свій лідер** — у Kafka для кожної партиції топіка призначається брокер-лідер, який відповідає
  за запис і читання даних
* **Повідомлення пишуться в лідера** — виробники надсилають повідомлення безпосередньо в брокер-лідер партиції
* **Дані реплікуються між брокерами** — для відмовостійкості Kafka реплікує дані партицій на
  інші брокери, які стають репліками (followers)
* **Автоматичний failover лідера** — у разі збою брокера-лідера Kafka автоматично призначає нового лідера з числа
  синхронізованих реплік (ISR), забезпечуючи безперебійну роботу системи

[до змісту](#apache-kafka)

## Налаштування брокера Kafka

### Реплікація та консистентність

* `min.insync.replicas`
  * **Опис**: мінімальна кількість синхронізованих реплік для підтвердження запису (працює разом з `acks=all`)
  * **Приклад**: `min.insync.replicas=2`
* `unclean.leader.election.enable`
  * **Опис**: дозволяє обрати лідера з несинхронізованих реплік, якщо немає синхронізованих (ризик втрати даних)
  * **Приклад**: `unclean.leader.election.enable=false`

### Логування та зберігання даних

* `log.dirs`
  * **Опис**: директорія на диску, де зберігаються логи партицій
  * **Приклад**: `log.dirs=/var/lib/kafka/logs`
* `log.retention.hours`
  * **Опис**: максимальний час зберігання даних у логах
  * **Приклад**: `log.retention.hours=168` (7 днів)
* `log.segment.bytes`
  * **Опис**: максимальний розмір сегмента логу, після якого створюється новий
  * **Приклад**: `log.segment.bytes=1073741824` (1 GB)

### Продуктивність і затримки

* `num.network.threads`
  * **Опис**: кількість потоків для обробки мережевих запитів
  * **Приклад**: `num.network.threads=3`
* `num.io.threads`
  * **Опис**: кількість потоків для вводу-виводу
  * **Приклад**: `num.io.threads=8`
* `socket.send.buffer.bytes`
  * **Опис**: розмір буфера для надсилання даних мережею
  * **Приклад**: `socket.send.buffer.bytes=102400`

### Управління повідомленнями

* `message.max.bytes`
  * **Опис**: максимальний розмір повідомлення (батчу), яке брокер може прийняти
  * **Приклад**: `message.max.bytes=1048576` (1 MB)
* `replica.fetch.max.bytes`
  * **Опис**: максимальний розмір даних на запит реплікації
  * **Приклад**: `replica.fetch.max.bytes=1048576` (1 MB)

### Безпека

* `ssl.keystore.location`
  * **Опис**: шлях до сховища ключів SSL
  * **Приклад**: `ssl.keystore.location=/var/private/ssl/kafka.keystore.jks`
* `ssl.truststore.location`
  * **Опис**: шлях до сховища довірених сертифікатів
  * **Приклад**: `ssl.truststore.location=/var/private/ssl/kafka.truststore.jks`

[до змісту](#apache-kafka)

## Архітектура продюсера

* **Створення повідомлення (Record)**: продюсер формує повідомлення, що містить ключ (необов'язковий), значення та метадані,
  як-от час відправлення. Повідомлення надсилається в топік (Topic), що складається з однієї або кількох партицій
* **Вибір партиції**: якщо ключ повідомлення вказано, Kafka хешує його, щоб визначити, у яку партицію
  записати повідомлення (повідомлення з однаковим ключем потрапляють в одну й ту саму партицію). Якщо ключа немає, Kafka розподіляє
  повідомлення по партиціях (за замовчуванням — sticky-партиціонування для ефективнішого батчингу)
* **Надсилання повідомлень у буфер (Batching)**: для підвищення продуктивності продюсер Kafka не надсилає кожне повідомлення
  окремо, а групує кілька повідомлень у пакети (batch), перш ніж відправити їх брокеру. Це знижує
  мережеві затримки й навантаження на брокер
* **Стиснення (Compression)**: для зменшення обсягу переданих даних продюсер може стискати повідомлення такими
  алгоритмами, як GZIP, Snappy, LZ4 або Zstd. Стиснення знижує навантаження на мережу й сховище, але додає невеликі накладні
  витрати на CPU
* **Асинхронне надсилання**: продюсер надсилає пакети повідомлень асинхронно. Це означає, що повідомлення записуються в
  буфер пам'яті й надсилаються брокеру, не очікуючи завершення попередніх операцій. Це підвищує пропускну здатність
* **Підтвердження (Acknowledgments)**: Kafka дозволяє налаштувати рівень підтверджень від брокерів (`acks`)
* **Ретрай та ідемпотентність**: якщо надсилання повідомлення не вдалося, продюсер може повторити спробу (retry).
  Також можна увімкнути ідемпотентний режим продюсера, що запобігає повторному запису того самого повідомлення в
  разі ретраю, забезпечуючи запис унікального повідомлення рівно один раз
* **Обробка помилок**: продюсер обробляє помилки під час надсилання. Залежно від налаштувань продюсер може
  повторити надсилання або повідомити про проблему через callback

### Резюме

* Продюсер обирає партицію для повідомлення
* Продюсер обирає рівень гарантії доставки
* У продюсері можна тюнити продуктивність

[до змісту](#apache-kafka)

## Налаштування продюсера

### Bootstrap-сервери (`bootstrap.servers`)

* **Опис**: вказує адреси брокерів Kafka, до яких продюсер має підключатися для надсилання повідомлень
* **Приклад**: `bootstrap.servers: localhost:9092,localhost:9093`
* **Навіщо це потрібно**: продюсер Kafka використовує ці брокери для отримання метаданих про кластер (наприклад, інформацію про топіки й партиції). Ці брокери слугують точками входу в кластер Kafka.

### Серіалізація ключа й значення

Продюсер має перетворювати (серіалізувати) дані у байтовий формат перед надсиланням у Kafka

* **Ключове налаштування для серіалізації ключа:**
  * `key.serializer`
  * Приклад: `key.serializer: org.apache.kafka.common.serialization.StringSerializer`
* **Ключове налаштування для серіалізації значення:**
  * `value.serializer`
  * Приклад: `value.serializer: org.apache.kafka.common.serialization.StringSerializer`

**Варіанти серіалізаторів:**
* `StringSerializer` для рядків
* `ByteArraySerializer` для масиву байтів
* `LongSerializer` для чисел
* Також можна реалізувати власні серіалізатори

### Надсилання повідомлень у буфер

Продюсер Kafka надсилає повідомлення асинхронно, і для цього використовується буферизація повідомлень

* **batch.size**: розмір одного пакета (batch), який продюсер надсилає брокеру
  * **Опис**: визначає кількість байтів повідомлень, які можуть бути буферизовані в одному пакеті перед надсиланням брокеру
  * **Приклад**: `batch.size: 16384` (16 KB)
  * **Навіщо це потрібно**: більші пакети можуть підвищити продуктивність, але можуть збільшити затримку
* **linger.ms**: максимальний час очікування перед надсиланням пакета
  * **Опис**: продюсер може трохи зачекати, поки буфер накопичить повідомлення, щоб надіслати більше даних за раз
  * **Приклад**: `linger.ms: 5` (час очікування 5 мс)
  * **Навіщо це потрібно**: дозволяє продюсеру зібрати більше повідомлень у пакет перед надсиланням, що може покращити ефективність використання мережі
* **buffer.memory**: розмір виділеної пам'яті для буферизації повідомлень
  * **Опис**: загальний обсяг пам'яті, який продюсер може використати для зберігання повідомлень, що очікують на надсилання
  * **Приклад**: `buffer.memory: 33554432` (32 MB)
  * **Навіщо це потрібно**: якщо буфер заповнюється, продюсер призупиняє надсилання повідомлень, доки буфер не звільниться

### Стиснення повідомлень

Продюсер може стискати повідомлення для зменшення обсягу переданих даних

* **compression.type**
  * **Опис**: вказує тип стиснення для повідомлень
  * **Приклад**: `compression.type: gzip` (варіанти: none, gzip, snappy, lz4, zstd)
  * **Навіщо це потрібно**: стиснення зменшує обсяг даних, що передаються мережею, що може знизити навантаження на мережу й сховище,
    особливо при великих обсягах повідомлень. Однак це може потребувати додаткових ресурсів на стиснення/розпакування

### Розподіл повідомлень по партиціях (партиціонування)

* **partitioner.class**
  * **Опис**: визначає логіку, за якою продюсер обирає партицію для кожного повідомлення
  * **Приклади**:
    * **якщо налаштування не задано**, за замовчуванням використовується вбудована логіка партиціонування,
      яка розподіляє повідомлення за хешем ключа, а за відсутності ключа — sticky-партиціонування (привʼязка до однієї
      партиції на короткий час для ефективнішого батчингу).
      > Примітка: клас `DefaultPartitioner` був прибраний у Kafka 3.3; тепер дефолтна логіка вбудована в сам продюсер.
    * `partitioner.class: org.apache.kafka.clients.producer.RoundRobinPartitioner` використовує метод Round Robin для розподілу повідомлень
    * `partitioner.class: org.apache.kafka.clients.producer.UniformStickyPartitioner` рівномірно надсилає повідомлення, привʼязуючись
      до партиції на короткий проміжок часу, щоб зменшити навантаження на брокери (deprecated у нових версіях, бо sticky-логіка стала дефолтною)

### Підтвердження (acks)

Налаштування визначає, скільки брокерів мають підтвердити отримання повідомлення, перш ніж продюсер вважатиме його
успішно надісланим

* **acks**
  * **Опис**: визначає кількість підтверджень від брокерів
  * **Значення**:
    * `0`: продюсер не чекає підтверджень (найшвидше надсилання, але високий ризик втрати повідомлень)
    * `1`: продюсер чекає підтвердження від лідера партиції
    * `all` (або `-1`): продюсер чекає підтверджень від усіх синхронізованих реплік (найвища надійність, але збільшена затримка)
  * **Приклад**: `acks: all`
  * **Навіщо це потрібно**: дозволяє обрати баланс між швидкістю й надійністю надсилання даних.

### Додаткові важливі налаштування

* **Кількість повторних спроб (retries):**
  * **Опис**: визначає, скільки разів продюсер має спробувати надіслати повідомлення в разі невдачі
  * **Приклад**: `retries: 3` (за замовчуванням з Kafka 2.1 — `Integer.MAX_VALUE`, обмежується `delivery.timeout.ms`)
  * **Навіщо це потрібно**: якщо стався тимчасовий збій, продюсер може повторити надсилання, що
    збільшує шанс доставки
* **Ідемпотентність продюсера (enable.idempotence):**
  * **Опис**: увімкнення ідемпотентного режиму, що запобігає дублюванню повідомлень при ретраях
  * **Приклад**: `enable.idempotence: true` (з Kafka 3.0 увімкнено за замовчуванням)
  * **Навіщо це потрібно**: гарантує, що кожне повідомлення буде записано рівно один раз (у межах сесії продюсера)
* **Максимальний розмір запиту (max.request.size):**
  * **Опис**: максимальний розмір повідомлення, яке продюсер може надіслати брокеру
  * **Приклад**: `max.request.size: 1048576` (1 MB)
  * **Навіщо це потрібно**: обмежує розмір повідомлень, щоб уникнути перевантаження мережі й брокерів.
* **Таймаут очікування підтверджень (request.timeout.ms):**
  * **Опис**: максимальний час очікування підтвердження від брокера
  * **Приклад**: `request.timeout.ms: 30000` (30 секунд)
  * **Навіщо це потрібно**: допомагає уникнути нескінченного очікування відповіді від брокера в разі його збою

[до змісту](#apache-kafka)

## Приклад конфігурації Kafka Producer

> Зверни увагу: у прикладі нижче значенням є `String[]`, тож типи й серіалізатор мають це враховувати. Тут навмисно показано
> налаштований `value.serializer` для рядків — для масиву потрібен власний серіалізатор. Нижче типи приведено до узгодженого вигляду.

```java
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class KafkaStringProducer {

    public static void main(String[] args) {
        // Налаштування Kafka Producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Створення Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String key = "user123";
        String value = "message1";

        // Створення запису й додавання заголовків
        ProducerRecord<String, String> record = new ProducerRecord<>("my_topic", key, value);
        record.headers().add("traceId", "someTraceId".getBytes());

        // Надсилання повідомлення в Kafka
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                System.out.println("Помилка під час надсилання повідомлення: " + exception.getMessage());
            } else {
                System.out.println("Повідомлення надіслано в топік " + metadata.topic() + " у партицію " + metadata.partition());
            }
        });

        producer.close();
    }
}
```

```properties
acks=all
retries=3
compression.type=gzip
```

### З використанням Spring Kafka

```java
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaProducerConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getServer());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, kafkaProperties.getProducerId());
        props.put(
                ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "com.example.configuration.kafka.KafkaProducerLoggingInterceptor"
        );

        if ("SASL_SSL".equals(kafkaProperties.getSecurityProtocol())) {
            props.put("ssl.truststore.location", kafkaProperties.getSslTrustStoreLocation());
            props.put("ssl.truststore.password", kafkaProperties.getSslTrustStorePassword());
            props.put("ssl.truststore.type", kafkaProperties.getSslTrustStoreType());
            props.put("ssl.keystore.type", kafkaProperties.getSslKeyStoreType());

            props.put("sasl.mechanism", kafkaProperties.getSaslMechanism());
            props.put("security.protocol", kafkaProperties.getSecurityProtocol());
            props.put("sasl.jaas.config", kafkaProperties.getJaasConfigCompiled());
        }

        return props;
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        var stringSerializerKey = new StringSerializer();
        stringSerializerKey.configure(Map.of("key.serializer.encoding", "UTF-8"), true);

        var stringSerializerValue = new StringSerializer();
        stringSerializerValue.configure(Map.of("value.serializer.encoding", "UTF-8"), false);

        return new DefaultKafkaProducerFactory<>(producerConfigs(), stringSerializerKey, stringSerializerValue);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
```

```java
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String message, String key, String topic) {
        try {
            log.info("Sending message {}", message);
            kafkaTemplate.send(topic, key, message);
            log.info("Successfully sent message {}", message);
        } catch (Exception ex) {
            log.error("Failed to send message to {} topic by key {}", topic, key, ex);
            throw ex;
        }
    }
}
```

```java
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @PostMapping("/send")
    public String sendMessage(@RequestParam String message, @RequestParam String key, @RequestParam String topic) {
        kafkaProducerService.sendMessage(message, key, topic);
        return "Message sent to Kafka!";
    }
}
```

### З використанням Spring Cloud Stream

> Примітка: анотація `@EnableBinding` і модель `Source/Sink/Processor` оголошені застарілими (deprecated) у Spring Cloud Stream 3.x
> і прибрані в 4.x. Сучасний підхід — функціональна модель на основі `java.util.function.Supplier`/`Function`/`Consumer`.
> Приклад нижче лишено для розуміння старого коду, який ще трапляється в legacy-проєктах.

```yaml
spring:
  cloud:
    stream:
      bindings:
        output:
          destination: my_topic
      kafka:
        binder:
          brokers: localhost:9092
```

Сучасний (функціональний) варіант:

```java
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Sinks;

import java.util.function.Supplier;

@Configuration
public class KafkaStreamProducer {

    private final Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();

    public void sendMessage(String message) {
        sink.tryEmitNext(message);
    }

    @Bean
    public Supplier<reactor.core.publisher.Flux<String>> output() {
        return sink::asFlux;
    }
}
```

[до змісту](#apache-kafka)

## Архітектура консюмера

Споживачі використовують **Kafka Consumer API** для взаємодії з брокерами Kafka. Вони отримують повідомлення й обробляють
їх відповідно до своєї логіки. Споживачів можна об'єднувати в групи **Consumer Groups**.

### Резюме

* «Розумний» (smart) консюмер — логіка обробки на стороні клієнта
* Консюмер опитує (poll) Kafka, а не навпаки
* Консюмер відповідає за гарантію обробки (через коміт offset)
* Автоматичний failover у межах консюмер-групи
* Незалежна обробка різними консюмер-групами

### Компоненти

#### Consumer Group

Kafka використовує концепцію Consumer Groups, що дозволяє кільком споживачам працювати разом, щоб паралельно
обробляти дані з топіків. Кожен споживач у групі обробляє лише частину даних топіка, забезпечуючи масштабованість і балансування навантаження.

* Усі повідомлення з одного топіка Kafka діляться між споживачами в групі
* Якщо в групі кілька споживачів, Kafka гарантує, що кожну партицію топіка обробляє лише один споживач цієї групи
* Якщо один зі споживачів виходить з ладу, його партиції автоматично перерозподіляються між активними споживачами (rebalance)

#### Offset (Зміщення)

Споживач відстежує offset кожної партиції, щоб розуміти, з якого повідомлення продовжувати читання. Offset — це
унікальний послідовний ідентифікатор кожного повідомлення в партиції.

Споживачі можуть зберігати offset у Kafka (у внутрішньому топіку `__consumer_offsets`) або поза нею (наприклад, у базі даних чи файловій системі). Якщо споживач
вимикається, він може відновити обробку з місця, де зупинився, прочитавши збережений offset.

#### Poll (Опитування)

Споживачі використовують метод `poll()` для опитування Kafka на наявність нових повідомлень.

* Споживач може вказати таймаут, після якого метод `poll()` поверне порожній результат, якщо повідомлень немає.
* Споживач має обробити повідомлення, а потім знову опитати Kafka для отримання нових даних. Виклик `poll()` також
  підтримує життєздатність споживача в групі (надсилає heartbeat-и).

### Процес роботи

1. **Ініціалізація**: споживач підключається до брокерів Kafka й приєднується до consumer group. Він отримує інформацію про партиції топіка, який читатиме.
2. **Підписка на топік**: споживач підписується на певні топіки за допомогою методу `subscribe()`.
3. **Опитування**: споживач викликає метод `poll()` для отримання нових повідомлень. Якщо доступні повідомлення є, вони передаються споживачеві для обробки.
4. **Обробка повідомлень**: споживач обробляє повідомлення, витягуючи корисну інформацію з кожного.
5. **Підтвердження обробки**: після обробки споживач комітить offset за допомогою `commit()`.
   Це оновлює **offset**, дозволяючи споживачеві продовжити читання з місця, де зупинився.
6. **Обробка помилок**: у разі помилки споживач може вирішити, як повторити обробку повідомлення
   (наприклад, за допомогою механізму повторних спроб).
7. **Завершення роботи**: коли споживач завершує обробку, він виходить із consumer group і може закрити з'єднання з Kafka.

[до змісту](#apache-kafka)

## Налаштування консюмера

* **bootstrap.servers** — список брокерів, до яких підключатиметься споживач
* **group.id** — ідентифікатор групи споживачів
* **auto.offset.reset** — поведінка за відсутності збереженого offset (`earliest` — читати з самого початку, `latest` — з кінця)
* **enable.auto.commit** — чи має споживач автоматично комітити offset. Якщо `false`, споживач робить це вручну
* **auto.commit.interval.ms** — інтервал між автоматичними комітами offset, якщо ввімкнено автокоміт
* **max.poll.records** — максимальна кількість повідомлень, які споживач отримає за один виклик `poll()`
* **session.timeout.ms** — максимальний час без heartbeat-ів від споживача, після якого він вважається недоступним і запускається rebalance
* **client.rack** — використовується для вказання серверної стійки (rack) або дата-центру споживача. Це важливо, якщо у вас
  розподілена інфраструктура Kafka з кількома стійками чи дата-центрами, де дані можуть бути репліковані
  між різними фізичними локаціями. Дозволяє читати з найближчої репліки (fetch from follower, з Kafka 2.4).

### Що таке Rack у контексті Kafka?

**Rack** — це мітка, що ідентифікує фізичне розташування брокерів Kafka. У Kafka можна задати rack для кожного брокера
параметром `broker.rack`, щоб керувати реплікацією даних, бажано розміщуючи репліки на різних фізичних машинах або в різних дата-центрах.

**Переваги використання client.rack**

* **Зниження затримок**: Kafka надає перевагу читанню з репліки в тому самому rack, де перебуває клієнт, що зменшує час відгуку та крос-зональний трафік
* **Підвищена відмовостійкість**: правильне налаштування `client.rack` і `broker.rack` покращує відмовостійкість
  за рахунок розміщення реплік у різних фізично віддалених місцях
* **Краще використання ресурсів**: правильний розподіл навантаження по rack допомагає уникнути перевантаження однієї фізичної локації

[до змісту](#apache-kafka)

## Приклад конфігурації Kafka Consumer

```java
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;

public class KafkaConsumerExample {

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String groupId = "my-consumer-group";
        String topic = "my-topic";

        // Налаштування Consumer
        Map<String, Object> consumerConfigs = new HashMap<>();
        consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Створення Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfigs);

        // Підписка на топік
        consumer.subscribe(Collections.singletonList(topic));

        try {
            // Читання повідомлень із Kafka
            while (true) {
                var records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(record -> System.out.println("Received message: " + record.value()));
            }
        } finally {
            consumer.close();
        }
    }
}
```

**At least once** (щонайменше один раз)

Щоб гарантувати обробку повідомлень щонайменше один раз, потрібно комітити **після** обробки. Якщо збій станеться між обробкою
й комітом, повідомлення буде оброблено повторно — тож обробка має бути ідемпотентною.

```java
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;

public class KafkaConsumerAtLeastOnce {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createConsumer(); // налаштування опущено для стислості
        try {
            while (true) {
                var records = consumer.poll(Duration.ofSeconds(1)); // очікування 1 секунду на повідомлення
                process(records);
                consumer.commitAsync(); // commit ПІСЛЯ обробки
            }
        } finally {
            consumer.close();
        }
    }
}
```

**At most once** (щонайбільше один раз)

Щоб гарантувати обробку повідомлень не більше одного разу, потрібно комітити **до** обробки або ввімкнути автокоміт
`enable.auto.commit=true`. Якщо збій станеться після коміту, але до обробки, повідомлення буде втрачено.

```java
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;

public class KafkaConsumerAtMostOnce {

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = createConsumer(); // налаштування опущено для стислості
        try {
            while (true) {
                var records = consumer.poll(Duration.ofSeconds(1)); // очікування 1 секунду на повідомлення
                consumer.commitAsync(); // commit ПЕРЕД обробкою
                process(records);
            }
        } finally {
            consumer.close();
        }
    }
}
```

### З використанням Spring Kafka

```java
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getServer());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumerGroupId());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return configs;
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
```

```java
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class MyKafkaConsumer {

    @KafkaListener(topics = "my_topic", groupId = "group_id")
    public void listen(@Payload String message,
                       @Header("traceId") String traceId,
                       @Header("correlationId") String correlationId) {
        System.out.println("Received message: " + message);
        System.out.println("Trace ID: " + traceId);
        System.out.println("Correlation ID: " + correlationId);
    }
}
```

**At least once**

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: false  # Вимкнення автокоміту
      auto-offset-reset: earliest  # Починати читання з початку (якщо немає offset)
      group-id: my-consumer-group
      max-poll-records: 500  # Максимальна кількість повідомлень за один poll
    listener:
      ack-mode: manual  # Ручне підтвердження
```

```java
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@EnableKafka
@Service
public class AtLeastOnceConsumer {

    @KafkaListener(topics = "my-topic", groupId = "my-consumer-group")
    public void listen(String message, Acknowledgment acknowledgment) {
        System.out.println("Received message: " + message);
        // Обробка повідомлення
        // Підтвердження offset вручну ПІСЛЯ успішної обробки
        acknowledgment.acknowledge();
    }
}
```

**At most once**

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: true  # Увімкнення автокоміту
      group-id: my-consumer-group
      auto-offset-reset: earliest  # Починати читання з початку
      max-poll-records: 100  # Максимальна кількість повідомлень за один poll
```

```java
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class AtMostOnceConsumer {

    @KafkaListener(topics = "my-topic", groupId = "my-consumer-group")
    public void listen(String message) {
        System.out.println("Received message: " + message);
        // Обробка повідомлення...
        // Offset буде зафіксовано автоматично
    }
}
```

### З використанням Spring Cloud Stream

> Як і в продюсері: модель `@EnableBinding` / `Sink` / `@StreamListener` застаріла (deprecated у 3.x, прибрана у 4.x).
> Сучасний підхід — `java.util.function.Consumer<T>` як bean. Приклади нижче лишено для розуміння legacy-коду; під ними наведено сучасний варіант.

```yaml
spring:
  cloud:
    stream:
      bindings:
        input:
          destination: my-topic
          group: my-consumer-group
          content-type: application/json
      kafka:
        binder:
          brokers: localhost:9092
          auto-create-topics: false
```

Сучасний (функціональний) варіант споживача:

```java
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
public class KafkaConsumerService {

    @Bean
    public Consumer<String> input() {
        return message -> System.out.println("Received message: " + message);
    }
}
```

**At least once**

```yaml
spring:
  cloud:
    stream:
      bindings:
        input:
          destination: my-topic
          group: my-consumer-group
          content-type: application/json
          consumer:
            max-attempts: 3  # Максимальна кількість спроб
      kafka:
        bindings:
          input:
            consumer:
              ack-mode: MANUAL  # Ручне підтвердження
```

```java
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;
import org.springframework.kafka.support.KafkaHeaders;

import java.util.function.Consumer;

@Configuration
public class AtLeastOnceConsumer {

    @Bean
    public Consumer<Message<String>> input() {
        return message -> {
            // Обробка повідомлення
            System.out.println("Received message: " + message.getPayload());
            // Ручне підтвердження ПІСЛЯ успішної обробки
            Acknowledgment ack = message.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
            if (ack != null) {
                ack.acknowledge();
            }
        };
    }
}
```

**At most once**

```yaml
spring:
  cloud:
    stream:
      bindings:
        input:
          destination: my-topic
          group: my-consumer-group
          content-type: application/json
      kafka:
        bindings:
          input:
            consumer:
              ack-mode: BATCH  # Автоматичне підтвердження після пакета повідомлень
```

```java
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import java.util.function.Consumer;

@Configuration
public class AtMostOnceConsumer {

    @Bean
    public Consumer<Message<String>> input() {
        return message -> {
            // Обробка повідомлення
            System.out.println("Received message: " + message.getPayload());
            // Offset буде зафіксовано автоматично
        };
    }
}
```

**Mostly Once** (здебільшого один раз)

Це гібридний режим — щось середнє між At least once і At most once. Він припускає, що повідомлення
зазвичай доставляються один раз, але іноді, у разі збоїв, можуть бути оброблені повторно. Для реалізації
такого режиму потрібна додаткова логіка, наприклад фільтрація дублікатів за унікальними ідентифікаторами повідомлень (дедуплікація).

> На практиці «Mostly Once» — це не офіційна гарантія Kafka, а просто At-least-once з дедуплікацією на боці споживача.
> Зверни увагу: у прикладі нижче `processedMessageIds` — це звичайний `HashSet` у пам'яті, тож він не переживає рестарт
> і не працює в кількох інстансах. У реальному коді використовуй спільне сховище (наприклад, Redis чи БД).

```java
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Configuration
public class MostlyOnceConsumer {

    private final Set<String> processedMessageIds = ConcurrentHashMap.newKeySet();

    @Bean
    public Consumer<Message<String>> input() {
        return message -> {
            MessageHeaders headers = message.getHeaders();
            String messageId = headers.get("messageId", String.class);
            if (messageId != null && !processedMessageIds.add(messageId)) {
                System.out.println("Duplicate message: " + messageId);
                return; // Пропускаємо дубльоване повідомлення
            }
            // Обробка повідомлення
            System.out.println("Received message: " + message.getPayload());
        };
    }
}
```

[до змісту](#apache-kafka)

## Основні API Kafka

* Producer API
* Consumer API
* Streams API
* Connector (Connect) API
* Admin API

[до змісту](#apache-kafka)

## Яка роль Producer API?

Використовується для публікації потоку повідомлень у топіки Kafka. Він керує партиціонуванням повідомлень, стисненням і балансуванням
навантаження між кількома брокерами. Продюсер також відповідає за повторні спроби невдалих публікацій і може бути
налаштований на різні рівні гарантій доставки.

[до змісту](#apache-kafka)

## Яка роль Consumer API?

Надає механізм для споживання повідомлень із топіків. Він дозволяє застосункам і мікросервісам читати дані,
що надходять у Kafka, і обробляти їх для подальшого використання — будь то зберігання, аналіз чи реактивна обробка.

[до змісту](#apache-kafka)

## Яка роль Connector API?

Connector API в Apache Kafka є частиною **Kafka Connect** — інфраструктури для інтеграції
зовнішніх систем із Kafka. Connector API спрощує процес підключення різних джерел даних
і систем-приймачів до Kafka, надаючи можливість автоматичного переміщення даних між ними (source- і sink-конектори).

[до змісту](#apache-kafka)

## Яка роль Streams API?

Це компонент Apache Kafka, призначений для створення застосунків і мікросервісів, які обробляють потоки даних
у реальному часі. Його основна роль — дозволити розробникам легко обробляти й аналізувати
дані, що надходять у вигляді безперервних потоків із топіків. Kafka Streams API надає високорівневий інтерфейс для
таких операцій, як фільтрація, агрегація, об'єднання (join) даних і обчислення віконних (windowing) функцій.

[до змісту](#apache-kafka)

## Яка роль Transactions API?

Kafka Transactions API дозволяє виконувати атомарні оновлення для кількох топіків/партицій. Він забезпечує **exactly-once**
гарантію для застосунків, які читають дані з одного топіка й пишуть в інший. Це особливо корисно для застосунків потокової
обробки, яким потрібно гарантувати, що кожна вхідна подія впливає на вихідні дані рівно один раз, навіть у разі збоїв.

[до змісту](#apache-kafka)

## Яка роль Quota API?

Quota API дозволяє налаштовувати квоти для кожного клієнта, щоб обмежити швидкість виробництва чи споживання даних і не дати
одному клієнту спожити надто багато ресурсів брокера. Це допомагає забезпечити справедливий розподіл ресурсів і
запобігти сценаріям відмови в обслуговуванні (denial of service).

[до змісту](#apache-kafka)

## Яка роль AdminClient API?

AdminClient API надає операції для управління топіками, брокерами, конфігурацією та іншими об'єктами Kafka.
Його можна використовувати для створення, видалення й опису топіків, управління списками ACL, отримання інформації про кластер і
програмного виконання інших адміністративних завдань.

[до змісту](#apache-kafka)

## Навіщо потрібен координатор групи?

Координатор групи (Group Coordinator) — це брокер, який відповідає за управління групами споживачів. Він керує членством у групах,
призначає партиції споживачам усередині групи й керує комітом offset. Коли споживач приєднується до групи або залишає її,
координатор групи запускає **rebalance** (перебалансування) для перепризначення партицій серед наявних споживачів.

[до змісту](#apache-kafka)

## Навіщо потрібен Consumer heartbeat thread?

Consumer Heartbeat Thread відповідає за надсилання періодичних сигналів (heartbeat) брокеру Kafka (зокрема, координатору групи).
Ці сигнали свідчать, що споживач живий і досі є частиною групи. Якщо споживач не надсилає
ці сигнали протягом налаштованого періоду (`session.timeout.ms`), він вважається неживим, і координатор групи ініціює rebalance
для перепризначення його партицій іншим споживачам у групі.

[до змісту](#apache-kafka)

## Як Kafka обробляє повідомлення?

Kafka підтримує два основні способи обробки повідомлень:
* **Queue (черга)**: кожне повідомлення обробляється одним споживачем у групі. Це досягається за рахунок наявності
  в групі кількох споживачів, кожен із яких читає дані з окремих партицій.
* **Publish-Subscribe (публікація-підписка)**: усі повідомлення обробляються всіма споживачами. Це досягається за рахунок того, що кожен
  споживач перебуває у власній групі, що дозволяє всім споживачам читати всі повідомлення.

[до змісту](#apache-kafka)

## Як Kafka обробляє затримку консюмера?

Затримка (lag) консюмера в Kafka — це різниця між offset останнього записаного повідомлення й offset останнього
обробленого (закоміченого) повідомлення. Kafka надає інструменти й API для моніторингу лагу консюмерів, як-от
утиліта командного рядка `kafka-consumer-groups` та AdminClient API. Високий лаг консюмерів може вказувати на проблеми з
продуктивністю або недостатню пропускну здатність консюмерів. Kafka не обробляє лаг автоматично,
але надає інформацію, необхідну застосункам для прийняття рішень про масштабування чи оптимізацію продуктивності.

[до змісту](#apache-kafka)

## Навіщо потрібні методи subscribe() і poll()?

Метод `subscribe()` використовується для підписки на один або кілька топіків. Сам по собі він не витягує жодних даних.
Метод `poll()`, навпаки, витягує дані з топіків. Він повертає записи, опубліковані
з моменту останнього запиту. Метод `poll()` зазвичай викликається в циклі для безперервного отримання даних, а також
підтримує життєздатність споживача в групі (надсилає heartbeat-и).

[до змісту](#apache-kafka)

## Навіщо потрібен метод position()?

Метод `position()` повертає offset наступного запису, який буде витягнуто для даної партиції. Це корисно для
відстеження ходу отримання даних і може використовуватися разом із методом `committed()`, щоб визначити, наскільки
споживач відстав від свого останнього закоміченого offset. Ця інформація цінна для моніторингу й
управління показниками споживача.

[до змісту](#apache-kafka)

## Навіщо потрібні методи commitSync() і commitAsync()?

Ці методи використовуються для коміту offset:
* **commitSync()**: синхронно комітить останній offset, повернутий `poll()`. Він повторюватиме спробу доти,
  доки не завершиться успішно або не натрапить на невідновлювану помилку. Блокує потік до завершення.
* **commitAsync()**: асинхронно комітить offset. Він не повторює спробу в разі збою, що робить його швидшим,
  але менш надійним, ніж `commitSync()`. Вибір між цими методами залежить від балансу між продуктивністю й надійністю,
  потрібного застосунку. Поширений патерн — `commitAsync()` у циклі + `commitSync()` у `finally` під час закриття.

[до змісту](#apache-kafka)

## Навіщо потрібен ідемпотентний продюсер?

Ідемпотентний продюсер запобігає дублюванню записів у Kafka в разі
повторних спроб надсилання повідомлень (за рахунок sequence number і producer ID). Це забезпечує гарантію **exactly-once** на рівні запису в межах однієї сесії продюсера
й важливо для підтримання цілісності даних,
особливо в розподілених системах, де можливі помилки зв'язку чи збої. З Kafka 3.0 увімкнено за замовчуванням.

[до змісту](#apache-kafka)

## Навіщо потрібен інтерфейс Partitioner?

Інтерфейс `Partitioner` у Producer API визначає, у яку партицію топіка буде надіслано повідомлення. Дефолтна логіка
використовує хеш ключа (якщо він є) для вибору партиції, гарантуючи, що повідомлення з одним і тим самим ключем завжди
потрапляють в одну й ту саму партицію. Можна реалізувати власні Partitioner для керування розподілом
повідомлень по партиціях на основі певної бізнес-логіки чи характеристик даних.

[до змісту](#apache-kafka)

## Навіщо потрібен Broker log cleaner thread?

Потік очищення логу (log cleaner) у Kafka відповідає за виконання **log compaction** (стиснення логу). Стиснення логу — це
механізм, за якого Kafka видаляє надлишкові записи, зберігаючи лише останнє значення для кожного ключа. Це корисно тоді, коли потрібне
лише останнє оновлення для даного ключа, наприклад для обслуговування changelog або стану БД. Log cleaner
періодично запускається для стиснення відповідних партицій (для топіків із `cleanup.policy=compact`).

[до змісту](#apache-kafka)

## Навіщо потрібен Kafka Mirror Maker?

Це інструмент, що дозволяє реплікувати дані між кластерами Kafka, які можуть перебувати в різних дата-центрах.
Він працює, споживаючи дані з одного кластера й передаючи їх в інший. Використовується для резервного копіювання даних,
об'єднання даних із кількох дата-центрів в єдине сховище або для перенесення даних між кластерами.
Сучасна версія — MirrorMaker 2 (MM2), побудована на Kafka Connect.

[до змісту](#apache-kafka)

## Навіщо потрібен Schema Registry?

Schema Registry надає RESTful-інтерфейс для зберігання й отримання схем (Avro, JSON Schema, Protobuf). Schema Registry використовується
разом із Kafka для забезпечення сумісності схем даних між виробниками й споживачами. Це особливо корисно
під час еволюції моделей даних з часом, зберігаючи зворотну (backward) і пряму (forward) сумісність.
> Зауваж: Schema Registry — це продукт Confluent, а не частина самої Apache Kafka.

[до змісту](#apache-kafka)

## Навіщо потрібен Streams DSL?

Kafka Streams DSL надає високорівневий API для операцій потокової обробки. Він дозволяє розробникам описувати
складну логіку обробки — фільтрацію, перетворення, агрегацію й об'єднання потоків даних. DSL абстрагує
багато низькорівневих деталей потокової обробки, спрощуючи створення й обслуговування застосунків потокової обробки
(альтернатива нижчого рівня — Processor API).

[до змісту](#apache-kafka)

## Як Kafka забезпечує версіонування повідомлень?

Сама по собі Kafka не забезпечує версіонування повідомлень напряму, але надає механізми, що дозволяють його реалізувати.
Один із поширених підходів — включити поле версії у схему повідомлення. Для складніших завдань
управління версіями використовують реєстри схем (наприклад, Confluent Schema Registry), які керують еволюцією схеми й сумісністю.

[до змісту](#apache-kafka)

## Як споживачі отримують повідомлення від брокера?

Kafka використовує **pull-модель** для отримання повідомлень. Споживачі запитують повідомлення в брокерів, а не брокери
надсилають повідомлення споживачам. Це дозволяє споживачам контролювати швидкість, з якою вони отримують повідомлення (природний backpressure).
Споживачі надсилають fetch-запити брокеру, вказуючи топік, партицію та початковий offset для кожної партиції.
Брокер відповідає повідомленнями обсягом до вказаного максимального ліміту в байтах.

[до змісту](#apache-kafka)

## У чому різниця між Kafka Streams і Apache Flink?

Kafka Streams і Apache Flink — це два потужні інструменти для обробки потоків даних у реальному часі, але
вони різняться за архітектурою, можливостями й сценаріями застосування.

### Порівняння Kafka Streams і Apache Flink

| **Критерій**            | **Kafka Streams**                           | **Apache Flink**                        |
|-------------------------|---------------------------------------------|-----------------------------------------|
| **Архітектура**         | Вбудована бібліотека, що працює всередині застосунку. Залежить від Kafka. | Незалежна розподілена система потокової обробки з можливістю інтеграції з різними джерелами й приймачами даних. |
| **Обробка даних**       | Обробляє потоки подій безпосередньо з Kafka. Підходить для обробки подій і транзакційних даних з мінімальною затримкою. | Підтримує як потокову (streaming), так і пакетну (batch) обробку. Спеціалізується на складній обробці подій із гнучким управлінням станом. |
| **Залежність від Kafka**| Побудована виключно навколо Kafka. Потребує Kafka для отримання й надсилання даних. | Працює з широким спектром джерел (Kafka, HDFS, бази даних тощо). Kafka — лише одне з багатьох джерел. |
| **Встановлення**        | Легко інтегрується в наявний Java/Scala-застосунок як бібліотека. Не потребує розгортання кластерів. | Потребує окремого кластера для виконання, що підходить для високопродуктивних розподілених систем. |
| **Управління станом**   | Вбудоване сховище стану на RocksDB, із реплікацією стану через changelog-топіки. | Розвинена система управління станом, підтримує складні функції відновлення стану й обробки. |
| **Гарантія доставки**   | Підтримує «at-least-once» і «exactly-once» семантику за відповідного налаштування Kafka. | Гнучкі гарантії доставки: «exactly-once», «at-least-once» та «at-most-once». |
| **Масштабованість**     | Масштабується автоматично разом із партиціями Kafka. Кожен інстанс обробляє свій набір партицій. | Масштабування на рівні задач (task), із гнучкішою моделлю масштабування й управління ресурсами. |
| **Обробка подій**       | Підходить для обробки подій із низькою затримкою та транзакційними вимогами. | Спеціалізується на складній обробці подій: windowing, агрегація, робота зі станом, що змінюється. Підтримує складну аналітику. |
| **Інструменти та API**  | Легковагова бібліотека з простими API. Основні операції — фільтрація, маппінг, join, windowing. | Просунута система з багатими API для складних обчислень, потокової й пакетної обробки. |
| **Вимоги до ресурсів**  | Менш ресурсоємна, бо не потребує окремого кластера. Працює в межах JVM-застосунку. | Потребує більше обчислювальних ресурсів, бо виконується на окремому кластері з високим паралелізмом. |

### Коли обрати Kafka Streams
- Якщо ви вже використовуєте Kafka і вам потрібна легковагова бібліотека для обробки даних безпосередньо всередині застосунку.
- Для сценаріїв із низькою затримкою, де дані приходять із Kafka й мають бути швидко оброблені з мінімальними накладними витратами.
- Якщо потрібно вбудувати обробку потоків у наявну Java/Scala-програму без розгортання окремих кластерів.

### Коли обрати Apache Flink
- Якщо ви працюєте з потоковою й пакетною обробкою, де джерела й приймачі — не лише Kafka, а й інші системи (наприклад, HDFS, бази даних).
- Для складних завдань обробки подій, що потребують управління станом, часових вікон, аналітики й відновлення після збоїв.
- Якщо проєкт потребує високої продуктивності, гнучкості, точних гарантій доставки й розподіленої обробки в кластері.

### Висновок
- **Kafka Streams** — ідеальний вибір, якщо ваша інфраструктура вже базується на Kafka і потрібна швидка й легковагова обробка потоків.
- **Apache Flink** — потужний інструмент для складних аналітичних завдань і потокової обробки в реальному часі
  з підтримкою складних схем обробки та різноманітних джерел даних.

[до змісту](#apache-kafka)

## У чому різниця між Kafka Consumer і Kafka Streams?

**Kafka Consumer** — це клієнт, який читає дані з топіка й виконує певну обробку. Зазвичай використовується для
простих сценаріїв отримання даних. **Kafka Streams**, навпаки, — просунутіший клієнт (бібліотека), який може споживати,
обробляти й класти дані назад у Kafka. Він надає DSL для складних операцій потокової обробки: фільтрації,
перетворення, агрегації й об'єднання потоків, а також управління станом і обробку за часом подій (event time).

[до змісту](#apache-kafka)

## У чому різниця між Kafka і Flume?

**Apache Kafka** та **Apache Flume** — це два інструменти для обробки й передачі даних, проте вони мають
різні цілі й архітектури. Основні відмінності:

### 1. Призначення та використання
- **Kafka**: розподілена платформа для потокової передачі даних, що забезпечує високу пропускну здатність
  і низьку затримку. Kafka використовується для побудови стримінгових застосунків та обробки
  даних у реальному часі. Її можна застосовувати для передачі логів, подій, метрик та інших даних, що потребують
  високої доступності й масштабованості.
- **Flume**: розподілена система для збору, агрегації й передачі логів і подій. Flume зазвичай використовується для
  доставки логів із серверів у HDFS, HBase чи інші сховища. Його основне призначення — збір даних
  із різних джерел (наприклад, лог-файлів) і передача в системи зберігання чи аналітики.

### 2. Архітектура
- **Kafka**: дані надсилаються в топіки й партиції, які можуть незалежно читати кілька споживачів.
  Kafka орієнтована на високу пропускну здатність і масштабованість для потокових даних у реальному часі.
- **Flume**: складається з **джерел (sources)**, **каналів (channels)** і **приймачів (sinks)**. Джерело отримує
  дані, канал їх буферизує, а приймач відправляє в кінцеву систему. Flume використовує event-based модель і часто
  застосовується для збору логів.

### 3. Зберігання даних
- **Kafka**: зберігає повідомлення на диску протягом тривалого часу (за замовчуванням — 7 днів) у топіках.
  Споживачі можуть читати дані в будь-який момент; Kafka підтримує концепцію **збереження й повторного відтворення даних**.
- **Flume**: не має вбудованого механізму довготривалого зберігання. Він просто передає дані в призначені сховища
  (наприклад, HDFS). Дані у Flume не зберігаються довго, і якщо сховище недоступне, вони можуть бути втрачені.

### 4. Продуктивність
- **Kafka**: розрахована на високі обсяги даних. Підтримує масштабованість як по виробниках,
  так і по споживачах, і може обробляти мільйони повідомлень на секунду з мінімальною затримкою.
- **Flume**: може бути менш масштабованим порівняно з Kafka і більше орієнтований на збір логів і подій
  із різних джерел. Хоча Flume теж може обробляти великі обсяги, він не призначений для таких потоків, як Kafka.

### 5. Використання та кейси
- **Kafka**: стримінг даних, аналітика в реальному часі, інтеграція різних систем, робота з
  великими даними й побудова подієвих (event-driven) застосунків.
- **Flume**: збір, агрегація й передача логів і подій у сховища, як-от HDFS, HBase
  чи зовнішні системи. Ідеальний вибір для організації потоків логування й моніторингу.

### 6. Підтримка та інтеграція
- **Kafka**: підтримує широкий спектр інтеграцій і може використовуватися з різними системами для побудови
  розподілених застосунків і аналітичних рішень.
- **Flume**: орієнтований на інтеграцію з екосистемою Hadoop; основне його використання — інтеграція з HDFS,
  HBase та іншими сховищами цієї екосистеми.

### 7. Споживацька модель
- **Kafka**: підтримує багатьох споживачів, які можуть читати з одного топіка незалежно, а також
  можливість **повторного читання даних**.
- **Flume**: має фіксовану схему доставки даних і не підтримує такої гнучкості, як Kafka, у частині споживачів та обробки.

### 8. Гарантії доставки
- **Kafka**: підтримує **гарантії доставки** з різними рівнями підтвердження (acknowledgment), а також може
  забезпечувати **доставку повідомлень рівно один раз** (exactly-once semantics).
- **Flume**: забезпечує базові гарантії доставки, але вони менш суворі, ніж у Kafka, і більше орієнтовані на
  стійкість до збоїв, а не на гарантовану доставку.

[до змісту](#apache-kafka)

## У чому різниця між Kafka і RabbitMQ?

Ментальна модель: **RabbitMQ — це розумний брокер / тупий споживач** (брокер маршрутизує й видаляє повідомлення після обробки),
а **Kafka — тупий брокер / розумний споживач** (брокер лише зберігає лог, а логіку читання й позицію контролює споживач).

**RabbitMQ** та **Apache Kafka** — це дві популярні системи обміну повідомленнями, кожна з власними особливостями
й сферою застосування. Основні відмінності:

### 1. Архітектура
- **RabbitMQ** використовує **черги повідомлень**. Повідомлення надсилається в чергу (через exchange), і один споживач витягує його
  для обробки.
- **Apache Kafka** використовує **топіки й партиції**. Повідомлення надсилаються в топіки, які можуть бути поділені на
  партиції, і кілька споживачів можуть читати ці повідомлення. Kafka орієнтована на великі потоки даних і масштабованість.

### 2. Модель доставки повідомлень
- **RabbitMQ**: повідомлення передаються в чергу, і кожен споживач отримує одне повідомлення. Повідомлення можуть бути
  підтверджені (acknowledged) або відхилені (rejected). RabbitMQ зазвичай видаляє повідомлення після підтвердження.
- **Kafka**: повідомлення зберігаються в топіках на тривалий термін, і споживачі можуть читати їх повторно. Kafka
  не видаляє повідомлення після читання — вони лишаються до закінчення retention, що дозволяє багаторазове читання.

### 3. Гарантії доставки
- **RabbitMQ**: надає підтвердження доставки й може повторно надіслати повідомлення, якщо споживач не підтвердив
  його отримання. Можна налаштувати різні рівні надійності (наприклад, через підтвердження чи транзакції).
- **Kafka**: повідомлення зберігаються на диску, що дозволяє споживачам читати їх будь-коли. Kafka гарантує
  доставку за певної конфігурації реплікації та збереження.

### 4. Продуктивність і масштабованість
- **RabbitMQ**: краще підходить для невеликих і середніх систем, де потрібна висока надійність і гарантована доставка.
  Підтримує **горизонтальне масштабування**, але потребує додаткових зусиль для налаштування й управління.
- **Kafka**: вирізняється високою **пропускною здатністю** й здатністю обробляти великі обсяги даних. Легко
  масштабується за рахунок **партиціонування** й реплікації.

### 5. Споживацька модель
- **RabbitMQ**: один споживач отримує одне повідомлення. Якщо споживач не встигає обробити повідомлення, його може бути повторно надіслано.
- **Kafka**: споживачі можуть читати повідомлення незалежно. Kafka зберігає всі повідомлення в топіках,
  і споживачі можуть читати їх будь-коли. Kafka також підтримує концепцію **груп споживачів**, де кожен
  споживач групи обробляє різні партиції.

### 6. Використання та кейси
- **RabbitMQ**: ідеальний для обробки запитів-відповідей, розподілених застосунків, мікросервісів із гарантією доставки,
  бізнес-процесів із чергами завдань.
- **Kafka**: використовується для обробки потоків даних, інтеграції з великими даними, запису журналів, моніторингу,
  обробки подій у реальному часі й зберігання великих обсягів даних для подальшого аналізу.

### 7. Виробники й споживачі
- **RabbitMQ**: один виробник надсилає повідомлення в чергу, і кілька споживачів можуть їх обробляти.
- **Kafka**: багато виробників можуть надсилати повідомлення в топіки, і кілька споживачів можуть читати їх
  одночасно, підтримуючи масштабованість.

### 8. Повідомлення та зберігання
- **RabbitMQ**: повідомлення зазвичай видаляються з черги після обробки споживачем. Зберігання здебільшого короткочасне.
- **Kafka**: повідомлення зберігаються на диску в топіках, доки не закінчиться термін зберігання (за конфігурацією).
  Це дозволяє повторно читати дані.

[до змісту](#apache-kafka)
