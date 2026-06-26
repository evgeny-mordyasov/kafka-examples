# AGENTS.md

## Рабочие инструкции

- Для сложных фич и значимых рефакторингов использовать ExecPlan из `PLANS.md`
  от дизайна до реализации.
- ExecPlan писать на русском.
- Перед каждой сессией использовать `/caveman ultra`.

## Назначение проекта

Репозиторий содержит учебные примеры работы с Apache Kafka на Java.
Проект собран как Maven multi-module workspace и разделен на:

- простые Kafka clients без Spring;
- native Kafka API примеры со Spring Boot bootstrap-кодом;
- пример consumer на Spring Kafka.

## Технологии

- Java 21 (`maven.compiler.release=21`).
- Maven multi-module project.
- Spring Boot `4.0.6` через dependency management в корневом `pom.xml`.
- Apache Kafka client libraries.
- Spring Kafka в модуле `spring-kafka/spring-kafka-consumer`.
- Micrometer/Prometheus для метрик native consumer-модулей.
- Guava `33.5.0-jre` управляется в корневом dependency management.
- Локальная Kafka-инфраструктура описана в `docker-compose.yml`.

## Структура проекта

- `pom.xml` - корневой Maven parent project `kafka-examples`.
  GroupId проекта: `ru.gold.ordance`. Подключает модули `native-kafka`,
  `simple-kafka` и `spring-kafka`.
- `docker-compose.yml` - локальный Zookeeper и Kafka broker.
  Broker доступен на `localhost:9092`, внутренний listener - `broker:29092`.
- `simple-kafka/` - минимальные producer/consumer на `kafka-clients`,
  `slf4j-api` и `logback-classic`. Основной пакет: `rgo.simple`.
- `native-kafka/` - Maven parent для native Kafka API примеров. Подключает
  `common`, `native-kafka-producer`, `native-kafka-consumer-sync-handler` и
  `native-kafka-consumer-async-handler`. Также настраивает
  `maven-failsafe-plugin` `3.5.4` для integration tests.
- `native-kafka/common/` - общие Kafka utilities, DTO, asserts, producer/consumer
  factories и Prometheus/Micrometer metrics abstractions.
- `native-kafka/native-kafka-producer/` - producer на Kafka client API со Spring
  Boot bootstrap-кодом. Есть конфиги `application.yml`,
  `application-at-most-once.yml` и `application-at-least-once.yml`.
- `native-kafka/native-kafka-consumer-sync-handler/` - consumer с синхронной
  обработкой batch-сообщений, ручным commit и Prometheus/Micrometer метриками.
- `native-kafka/native-kafka-consumer-async-handler/` - consumer с асинхронной
  обработкой batch-сообщений, обработкой `BatchProcessingException` и
  Prometheus/Micrometer метриками.
- `spring-kafka/` - Maven parent для Spring Kafka примеров.
- `spring-kafka/spring-kafka-consumer/` - consumer на `spring-boot-starter-kafka`
  с вариантами конфигурации listener через Spring profiles.
- `todo.txt` - локальные рабочие заметки; не считать проектной документацией.

## Конфигурация

- Основной локальный bootstrap server: `localhost:9092`.
- Основной учебный topic в конфигурациях: `string-values`.
- Native consumer-конфиги используют prefix `kafka-consumer`.
- Native producer-конфиги используют prefix `kafka-producer`.
- Конфигурацию приложений размещать в `src/main/resources/application.yml` или
  `application.yaml`, следуя стилю конкретного модуля.
- Логи настраивать через существующие `logback.xml` и `logback-test.xml`.

## Команды

- Собрать весь проект:
  `mvn clean install`
- Собрать отдельный модуль вместе с зависимостями:
  `mvn -pl <module-path> -am clean install`
- Запустить unit/integration tests затронутого native-модуля:
  `mvn -pl native-kafka/<module-name> -am verify`
- Поднять локальную Kafka-инфраструктуру:
  `docker compose up -d`
- Остановить локальную Kafka-инфраструктуру:
  `docker compose down`

## Рекомендации для изменений

- Сохранять текущую Maven multi-module структуру.
- Не менять `groupId`, `artifactId` и межмодульные зависимости без явной причины.
- Для новых Java-классов придерживаться существующих package namespaces:
  `rgo.simple`, `rgo.nativekafka`, `rgo.spring.kafka`.
- Для native Kafka примеров переиспользовать utilities из `native-kafka/common`.
- Для consumer-метрик использовать существующие `MetricsService`,
  `CustomMetricsProcessor` и Prometheus provider abstractions.
- Для тестов использовать JUnit 5, Mockito, AssertJ и существующие patterns в
  `src/test/java`.
- Перед завершением изменений, влияющих на код, запускать как минимум сборку
  затронутого Maven-модуля.
- Не создавать отдельные ветки Git при реализации задач.
