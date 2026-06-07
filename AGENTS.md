# AGENTS.md

## Назначение проекта

Репозиторий содержит набор учебных примеров работы с Apache Kafka на Java.
Проект собран как Maven multi-module workspace и разделен на простые Kafka
clients, примеры на native Kafka API и примеры со Spring Kafka.

## Технологии

- Java 21 (`maven.compiler.release=21`).
- Maven multi-module project.
- Spring Boot 3.5.7 через dependency management в корневом `pom.xml`.
- Apache Kafka client libraries.
- Локальная инфраструктура Kafka описана в `docker-compose.yml`.

## Структура проекта

- `pom.xml` - корневой Maven parent project `kafka-examples`.
  Подключает модули `native-kafka`, `simple-kafka` и `spring-kafka`.
- `docker-compose.yml` - локальный Zookeeper и Kafka broker на `localhost:9092`.
- `simple-kafka/` - минимальные примеры producer/consumer на `kafka-clients`.
  Основной пакет: `rgo.simple`.
- `native-kafka/` - набор модулей с ручной конфигурацией Kafka client API и
  Spring Boot bootstrap-кодом.
- `native-kafka/common/` - общие утилиты для native Kafka примеров.
- `native-kafka/native-kafka-producer/` - producer на Kafka client API.
- `native-kafka/native-kafka-consumer/` - consumer с синхронной обработкой.
- `native-kafka/native-kafka-consumer-async-handler/` - consumer с асинхронной
  обработкой и метриками Prometheus/Micrometer.
- `spring-kafka/` - Maven parent для Spring Kafka примеров.
- `spring-kafka/spring-kafka-consumer/` - consumer на `spring-kafka`.

## Команды

- Собрать весь проект:
  `mvn clean install`
- Собрать отдельный модуль вместе с зависимостями:
  `mvn -pl <module-path> -am clean install`
- Поднять локальную Kafka-инфраструктуру:
  `docker compose up -d`
- Остановить локальную Kafka-инфраструктуру:
  `docker compose down`

## Рекомендации для изменений

- Сохранять текущую Maven multi-module структуру.
- Не менять groupId/artifactId и межмодульные зависимости без явной причины.
- Для новых Java-классов придерживаться существующих package namespaces:
  `rgo.simple`, `rgo.nativekafka`, `rgo.spring.kafka`.
- Конфигурацию приложений размещать в `src/main/resources/application.yml` или
  `application.yaml`, следуя стилю конкретного модуля.
- Логи настраивать через существующие `logback.xml` в модулях.
- Перед завершением изменений, влияющих на код, запускать как минимум сборку
  затронутого Maven-модуля.
