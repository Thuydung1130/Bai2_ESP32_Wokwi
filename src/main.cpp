#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <DHTesp.h>
#include <math.h>
#include <time.h>
#include <sys/time.h>
#include <esp_sntp.h>
#include "secrets.h"

const char* WIFI_SSID = "Wokwi-GUEST";
const char* WIFI_PASSWORD = "";

const char* THINGSBOARD_HOST = "eu.thingsboard.cloud";
const uint16_t THINGSBOARD_PORT = 1883;
const char* THINGSBOARD_TOPIC = "v1/devices/me/telemetry";
const char* HIVEMQ_HOST = "broker.hivemq.com";
const uint16_t HIVEMQ_PORT = 1883;
const char* HIVEMQ_TOPIC = "iotlab2/dung059/esp32/telemetry";
const char* DEVICE_ID = "esp32-dung059";

const uint8_t DHT_PIN = 15;
const uint8_t TRIG_PIN = 5;
const uint8_t ECHO_PIN = 18;
const uint8_t LED_PIN = 2;

const uint32_t SENSOR_INTERVAL = 2000;
const uint32_t SEND_INTERVAL = 5000;
const uint32_t WIFI_RETRY_INTERVAL = 5000;
const uint32_t MQTT_RETRY_INTERVAL = 2000;
const char* NTP_SERVER_1 = "pool.ntp.org";
const char* NTP_SERVER_2 = "time.google.com";
const time_t MIN_VALID_UNIX_TIME = 1700000000;

WiFiClient thingsBoardNetwork;
WiFiClient hiveMqNetwork;
PubSubClient thingsBoardClient(thingsBoardNetwork);
PubSubClient hiveMqClient(hiveMqNetwork);
DHTesp dht;

float temperature = NAN;
float humidity = NAN;
float distanceCm = NAN;
uint32_t sequenceNumber = 0;
uint32_t lastSensorRead = 0;
uint32_t lastSend = 0;
uint32_t lastThingsBoardAttempt = 0;
uint32_t lastHiveMqAttempt = 0;
uint32_t lastWifiAttempt = 0;
bool sensorDataValid = false;
bool ntpConfigured = false;
volatile bool ntpTimeSynchronized = false;
bool wifiStatusPrinted = false;

void onNtpTimeSynchronized(struct timeval* currentTime) {
  ntpTimeSynchronized = true;
  Serial.printf("[NTP] Synchronization completed: unix_ms=%llu\n",
                static_cast<unsigned long long>(currentTime->tv_sec) * 1000ULL +
                static_cast<unsigned long long>(currentTime->tv_usec) / 1000ULL);
}

void startWiFi() {
  Serial.println();
  Serial.println("[WIFI] Connecting to Wokwi-GUEST...");
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD, 6);
}

void maintainWiFi() {
  if (WiFi.status() == WL_CONNECTED) {
    if (!wifiStatusPrinted) {
      Serial.println();
      Serial.println("[WIFI] Connected!");
      Serial.print("[WIFI] IP: ");
      Serial.println(WiFi.localIP());
      Serial.print("[WIFI] RSSI: ");
      Serial.println(WiFi.RSSI());
      wifiStatusPrinted = true;
    }
    if (!ntpConfigured) {
      sntp_set_time_sync_notification_cb(onNtpTimeSynchronized);
      configTime(0, 0, NTP_SERVER_1, NTP_SERVER_2);
      ntpConfigured = true;
      Serial.println("[NTP] Synchronization requested");
    }
    return;
  }

  wifiStatusPrinted = false;
  uint32_t now = millis();
  if (now - lastWifiAttempt < WIFI_RETRY_INTERVAL) return;
  lastWifiAttempt = now;
  Serial.println("[WIFI] Reconnecting...");
  WiFi.disconnect();
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD, 6);
}

bool getUnixTimestampMs(uint64_t& timestampMs) {
  if (!ntpTimeSynchronized) return false;
  struct timeval currentTime;
  if (gettimeofday(&currentTime, nullptr) != 0 ||
      currentTime.tv_sec < MIN_VALID_UNIX_TIME) return false;
  timestampMs = static_cast<uint64_t>(currentTime.tv_sec) * 1000ULL +
                static_cast<uint64_t>(currentTime.tv_usec) / 1000ULL;
  return true;
}

void printTimestampDiagnostic(uint64_t timestampMs) {
  time_t seconds = static_cast<time_t>(timestampMs / 1000ULL);
  struct tm utcTime;
  char utcText[32];
  gmtime_r(&seconds, &utcTime);
  strftime(utcText, sizeof(utcText), "%Y-%m-%dT%H:%M:%S", &utcTime);
  Serial.printf("[TIME] device_timestamp_ms=%llu device_utc=%s.%03lluZ ntp_synced=%s\n",
                static_cast<unsigned long long>(timestampMs), utcText,
                static_cast<unsigned long long>(timestampMs % 1000ULL),
                ntpTimeSynchronized ? "yes" : "no");
}

String macWithoutSeparators() {
  String mac = WiFi.macAddress();
  mac.replace(":", "");
  return mac;
}

void maintainThingsBoard() {
  if (WiFi.status() != WL_CONNECTED || thingsBoardClient.connected()) return;
  uint32_t now = millis();
  if (now - lastThingsBoardAttempt < MQTT_RETRY_INTERVAL) return;
  lastThingsBoardAttempt = now;

  String clientId = "BAI1-ESP32-" + macWithoutSeparators();
  Serial.print("[ThingsBoard] Connecting... ");
  bool connected = thingsBoardClient.connect(
      clientId.c_str(), THINGSBOARD_ACCESS_TOKEN, "");
  if (connected) {
    Serial.println("OK");
  } else {
    Serial.print("FAILED, state=");
    Serial.println(thingsBoardClient.state());
  }
}

void maintainHiveMq() {
  if (WiFi.status() != WL_CONNECTED || hiveMqClient.connected()) return;
  uint32_t now = millis();
  if (now - lastHiveMqAttempt < MQTT_RETRY_INTERVAL) return;
  lastHiveMqAttempt = now;

  String clientId = "BAI2-ESP32-" + macWithoutSeparators();
  Serial.print("[HiveMQ] Connecting... ");
  bool connected = hiveMqClient.connect(clientId.c_str());
  if (connected) {
    Serial.println("OK");
  } else {
    Serial.print("FAILED, state=");
    Serial.println(hiveMqClient.state());
  }
}

float readDistance() {
  digitalWrite(TRIG_PIN, LOW);
  delayMicroseconds(2);
  digitalWrite(TRIG_PIN, HIGH);
  delayMicroseconds(10);
  digitalWrite(TRIG_PIN, LOW);
  uint32_t duration = pulseIn(ECHO_PIN, HIGH, 30000UL);
  if (duration == 0) return NAN;
  float distance = (duration * 0.0343f) / 2.0f;
  if (distance < 2.0f || distance > 400.0f) return NAN;
  return distance;
}

void readSensors() {
  TempAndHumidity dhtData = dht.getTempAndHumidity();
  float measuredDistance = readDistance();
  if (!isfinite(dhtData.temperature) || !isfinite(dhtData.humidity) ||
      !isfinite(measuredDistance)) {
    sensorDataValid = false;
    Serial.println("[SENSOR] Invalid data - skip");
    return;
  }

  temperature = dhtData.temperature;
  humidity = dhtData.humidity;
  distanceCm = measuredDistance;
  sensorDataValid = true;
  Serial.println();
  Serial.println("========== SENSOR ==========");
  Serial.print("Temperature : "); Serial.print(temperature, 2); Serial.println(" C");
  Serial.print("Humidity    : "); Serial.print(humidity, 2); Serial.println(" %");
  Serial.print("Distance    : "); Serial.print(distanceCm, 2); Serial.println(" cm");
  Serial.println("============================");
}

void blinkSuccessLed() {
  digitalWrite(LED_PIN, HIGH);
  delay(60);
  digitalWrite(LED_PIN, LOW);
}

void sendTelemetry() {
  if (!sensorDataValid) {
    Serial.println("[TELEMETRY] Sensor data invalid - skipped");
    return;
  }

  uint64_t timestampMs = 0;
  if (!getUnixTimestampMs(timestampMs)) {
    Serial.println("[NTP] Unix time not synchronized - telemetry skipped");
    return;
  }

  sequenceNumber++;
  unsigned long uptime = millis() / 1000UL;
  long rssi = WiFi.RSSI();
  char payload[384];
  int payloadLength = snprintf(
      payload, sizeof(payload),
      "{\"temperature\":%.2f,\"humidity\":%.2f,\"distance_cm\":%.2f,"
      "\"rssi\":%ld,\"sequence\":%lu,\"uptime_s\":%lu,"
      "\"device_id\":\"%s\",\"timestamp_ms\":%llu}",
      temperature, humidity, distanceCm, rssi,
      static_cast<unsigned long>(sequenceNumber), uptime, DEVICE_ID,
      static_cast<unsigned long long>(timestampMs));
  if (payloadLength < 0 || payloadLength >= static_cast<int>(sizeof(payload))) {
    Serial.println("[TELEMETRY] Payload creation failed - skipped");
    return;
  }

  Serial.println();
  Serial.println("------- TELEMETRY -------");
  printTimestampDiagnostic(timestampMs);
  Serial.println(payload);

  if (thingsBoardClient.connected()) {
    bool success = thingsBoardClient.publish(THINGSBOARD_TOPIC, payload);
    Serial.print("[ThingsBoard] Publish: ");
    Serial.println(success ? "SUCCESS" : "FAILED");
    if (success) blinkSuccessLed();
  } else {
    Serial.println("[ThingsBoard] Not connected - publish skipped");
  }

  if (hiveMqClient.connected()) {
    bool success = hiveMqClient.publish(HIVEMQ_TOPIC, payload);
    Serial.print("[HiveMQ] Publish: ");
    Serial.println(success ? "SUCCESS" : "FAILED");
  } else {
    Serial.println("[HiveMQ] Not connected - publish skipped");
  }
  Serial.println("-------------------------");
}

void setup() {
  Serial.begin(115200);
  delay(500);
  Serial.println();
  Serial.println("============================");
  Serial.println(" BAI 2 - MOC 1 ESP32 NODE");
  Serial.println("============================");

  pinMode(TRIG_PIN, OUTPUT);
  pinMode(ECHO_PIN, INPUT);
  pinMode(LED_PIN, OUTPUT);
  digitalWrite(LED_PIN, LOW);
  dht.setup(DHT_PIN, DHTesp::DHT22);

  thingsBoardClient.setServer(THINGSBOARD_HOST, THINGSBOARD_PORT);
  thingsBoardClient.setBufferSize(512);
  thingsBoardClient.setKeepAlive(30);
  thingsBoardClient.setSocketTimeout(2);
  hiveMqClient.setServer(HIVEMQ_HOST, HIVEMQ_PORT);
  hiveMqClient.setBufferSize(512);
  hiveMqClient.setKeepAlive(30);
  hiveMqClient.setSocketTimeout(2);

  startWiFi();
  lastSensorRead = millis();
  lastSend = millis();
}

void loop() {
  maintainWiFi();
  if (WiFi.status() == WL_CONNECTED) {
    maintainThingsBoard();
    maintainHiveMq();
  }
  if (thingsBoardClient.connected()) thingsBoardClient.loop();
  if (hiveMqClient.connected()) hiveMqClient.loop();

  uint32_t now = millis();
  if (now - lastSensorRead >= SENSOR_INTERVAL) {
    lastSensorRead = now;
    readSensors();
  }
  if (now - lastSend >= SEND_INTERVAL) {
    lastSend = now;
    sendTelemetry();
  }
  delay(10);
}
