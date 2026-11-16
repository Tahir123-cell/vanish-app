# Vanish — Classroom Chat

Vanish is a Flutter classroom chat app for short-lived conversations. Students join using a display name and exchange messages through a shared relay server.

Messages expire 30 seconds after the relay first accepts them. Messages received later show only their remaining lifetime.

## Features

- Join a classroom using a display name.
- Send and receive messages with countdowns.
- Queue messages locally while offline.
- Reconnect and process pending messages.
- Ignore duplicate messages using message IDs.
- Remove expired messages from the chat and local message storage.
- Show connection, storage and delivery uncertainty notices.

## Platform and Technologies

- Development environment: Windows and VS Code
- Main target: Android
- Physical test device: Vivo V2352
- Additional testing: Chrome
- Application: Flutter and Dart
- Communication: WebSocket with JSON messages
- Local storage: SQLite on Android
- Development protocol: `dev-v1`

## Requirements

- Flutter SDK
- Android SDK and a compatible Java installation
- An Android phone with USB debugging enabled for development
- A running Vanish development relay

Android Studio is optional when the Android SDK command-line tools are configured separately.

## Get the Source Code

```bash
git clone https://github.com/Tahir123-cell/vanish-app.git
cd vanish-app
flutter pub get
```

This repository is private. Access must be granted before another person can clone it.

## Start the Development Relay

From the project folder, open a terminal and run:

```bash
dart run tool/dev_relay.dart
```

Keep this terminal running while testing.

For connections from another device, the relay must listen on a network-accessible interface, such as `InternetAddress.anyIPv4`. The laptop firewall must allow the relay port.

## Relay Address Used for Testing

Local laptop testing:

```text
ws://127.0.0.1:8081
```

Physical phone testing over the local hotspot network:

```text
ws://10.30.28.169:8081
```

The second address was the laptop's Wi-Fi address during testing. It is a temporary local address, not a public server, and may change when the network changes.

To use another laptop or network:

1. Connect the phone and relay laptop to the same local network.
2. Run `ipconfig` on the Windows laptop.
3. Find the Wi-Fi IPv4 address.
4. Update the relay URL in `lib/main.dart` to `ws://<laptop-ip>:8081`.
5. Rebuild and run the app.

On a phone, `127.0.0.1` refers to the phone itself. It reaches the laptop relay only when USB port forwarding is configured.

## Run on Android

Connect an Android phone and approve the USB debugging prompt.

```bash
flutter devices
flutter run -d <device-id>
```

Replace `<device-id>` with the ID shown by `flutter devices`.

For USB testing with the loopback relay address, configure forwarding first:

```bash
adb reverse tcp:8081 tcp:8081
```

For wireless messaging, use the laptop's local network address instead. The relay must remain running.

## Run in Chrome

```bash
flutter run -d chrome
```

Use the phone and Chrome client together to test messages in both directions.

## Build an Android APK

```bash
flutter build apk --release
```

The APK is generated at:

```text
build/app/outputs/flutter-apk/app-release.apk
```

The relay URL configured in the source must be reachable from the receiving phone.

## Manual Testing

The following checks were reported during development:

- Messages exchanged between the Android phone and Chrome.
- Sender and receiver countdowns displayed.
- Messages disappeared after expiry.
- Offline messages entered the pending queue.
- Pending messages were processed after reconnection.
- Messaging worked over the local network after USB disconnection.
- The app's database row-count check showed accepted message rows removed after expiry.

The row-count check verifies logical database deletion, not forensic erasure of every copy.

## Class Interoperability Status

The current implementation uses the development protocol `dev-v1`.

Testing with an independently developed team's app is still pending. Using the same relay address alone does not establish compatibility. Clients must also agree on message formats, protocol version, timestamps, acknowledgements and reconnection behaviour.

## Limitations

- Screenshots and manually copied text cannot be prevented.
- The relay can access message content while handling it.
- The development connection uses plain `ws://`, without transport encryption.
- A powered-off phone cannot run deletion at the expiry instant.
- Startup and resume cleanup remove stored accepted messages; this can also remove still-live messages early.
- SQLite row deletion does not guarantee forensic erasure from flash storage, backups or system memory.
- Display names and device IDs are not secure authentication.
- Delivery acknowledgement does not prove that a person read a message.