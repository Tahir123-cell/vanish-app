# Vanish Class Network

Vanish is a Flutter chat app for students who want to share a quick message with their class. Everyone joins one room using a display name. If a student asks “Which lab are we using?” and a classmate replies “Lab 3, second floor,” each message has its own 30-second countdown. Students who join the room later see new messages as they arrive.

**Platform:** Flutter and Dart, developed in VS Code on Windows. The mobile development device was a Vivo Y28 (V2352). Chrome is used to run the web client.

**Configured class relay:** `wss://vanish-relay-sawaira.bonto.page/ws`  
**Protocol:** Version `1`, using JSON messages over a WebSocket connection.

## Getting started

### 1. Open the project

If the project is already on your computer, open the folder containing `pubspec.yaml` in VS Code. To get a fresh copy, open a terminal and run:

```powershell
git clone https://github.com/Tahir123-cell/vanish-app.git
cd vanish-app
```

The following commands run from that project folder. In VS Code, choose **Terminal → New Terminal**. Use a second terminal if the first one is already running the app.

### 2. Check Flutter and install the packages

```powershell
flutter doctor
flutter pub get
```

For Chrome, you need Flutter and Chrome. For a physical Android phone, you also need the Android SDK, platform tools and a compatible Java installation. The **Android toolchain** and **Connected device** sections in `flutter doctor` are the relevant checks for a phone. You can do this work from VS Code with the Android command-line tools.

### 3. Choose a device

| Where to run Vanish | What to do |
| --- | --- |
| Chrome on your laptop | Run `flutter run -d chrome`. Flutter opens a browser tab automatically. Keep the terminal open while testing. |
| An Android phone over USB | Enable **Developer options** and **USB debugging** on the phone. Connect it with a data cable, unlock it, and accept **Allow USB debugging**. Run `flutter devices`, then `flutter run -d DEVICE_ID`. Replace `DEVICE_ID` with the ID printed beside your phone. |
| A second Android phone with an APK | Run `flutter build apk --release`. Copy `build/app/outputs/flutter-apk/app-release.apk` to the second phone and install it. If Android asks for permission to install the file, follow the prompt for the app that opened it. |

The `localhost` address in Chrome opens the copy of Vanish running on that laptop. Both clients connect separately to the hosted class relay. A classmate can use mobile data or another Wi-Fi network. Once the app is installed on a phone, the laptop can be switched off. Live chat still needs internet access and a reachable relay.

## Joining and using the room

1. Enter a display name between 1 and 30 characters, then tap **Join class room**. The app remembers the name and creates a device ID on its first run.
2. Wait for **Connected** at the top of the Class Room screen. A first visit to the hosted relay can take about a minute if the service has been idle.
3. Type a message of up to 500 characters and tap **Send**. Once the relay accepts it, the sender and connected classmates receive the same message ID and expiry time. Your own messages appear on the right.
4. Tap the information icon to open **About this room**. It explains the timers and shows a snapshot of how many accepted messages and pending messages the app currently stores.

After a device has joined successfully at least once, **Open offline** appears on the Join screen. This lets a returning student open the room and queue a message while disconnected. In the room, the Wi-Fi button lets you try offline mode and reconnect for a demo.

## The two 30-second timers

The starting point matters. A message waiting on a phone and a message accepted by the relay have different deadlines.

| Timer | Starts when | What happens at the deadline |
| --- | --- | --- |
| Offline waiting time | The student taps **Send** | If the message is still waiting after 30 seconds, the phone drops it from the outbox. |
| Live chat countdown | The relay accepts the message and sets `sent_at` | At `expires_at`, the app removes the live message and deletes its stored text row. An `expired` event from the relay also triggers removal. |

For example, Ali taps **Send** at **10:00:00** while offline. If the relay accepts the waiting message at **10:00:20**, its live expiry is **10:00:50**. A classmate who receives the broadcast at **10:00:24** has about **26 seconds** left to read it. If Ali instead stays offline until **10:00:50**, the original message has already passed its queue deadline and is dropped.

The relay includes its time in the `welcome` event. While connected, the app estimates the current relay time by adding the time elapsed on a local stopwatch. It calculates:

```text
estimated relay time = relay time at welcome + elapsed time since welcome
remaining time       = expires_at - estimated relay time
```

This gives both phones a common deadline even if their clock settings differ by several minutes. A slow connection or a different screen refresh time can still cause a small visible difference between the two countdowns.

## How different teams’ apps communicate

Each team can build its own interface. To exchange messages, both apps need to join the same relay and send the fields expected by protocol v1. The relay uses the display name supplied at join time and sends accepted messages back to the sender as well as to the other joined apps.

| Event | Direction | What it does |
| --- | --- | --- |
| `join` | App → relay | Sends numeric protocol version `1`, a stable `device_id` and a display `name`. |
| `welcome` | Relay → app | Confirms the protocol version and supplies `server_time` for the countdown. |
| `message` | App → relay | Sends a new message with `id`, `sender` and `text`. |
| `message` | Relay → joined apps | Broadcasts the accepted message with `sent_at` and `expires_at`, in milliseconds since the Unix epoch. |
| `delivered` | Relay → sender | Reports how many joined clients received the message. A student may still have their phone on the desk and never read it. |
| `expired` | Relay → joined apps | Identifies the expired message by ID so clients can remove it. |
| `ping` and `pong` | App ↔ relay | Check whether the connection is still responding. |
| `error` | Relay → app | Reports a problem such as a bad name, incompatible protocol or invalid message. |

The client sends a ping every 15 seconds. After 30 seconds without a pong, it reconnects. For a temporary connection failure, it waits 1, 2, 4 and then 8 seconds between attempts. Further attempts use the 8-second delay. On rejoining, the app gets a fresh time reference. Protocol v1 provides no room-history replay.

## Offline delivery and duplicates

The app gives each outgoing message a unique ID when the student taps **Send**. It saves the text locally before clearing the input field. If the save fails, the text stays in the field so the student can try again.

The **outbox** is the list of messages waiting on the device. It can hold up to 50 pending entries. A student can cancel a waiting message. When space becomes available and the relay is connected, the app checks each waiting message’s age before sending it.

A connection can break just after a phone sends a message. The relay might already have broadcast it even though the sender never received confirmation. In that case, Vanish shows **Delivery outcome unknown** and skips the automatic resend. This avoids sending the same text twice at the cost of sometimes leaving a message undelivered. Recipients also keep track of IDs they have already processed, so receiving the same ID twice does not create two chat bubbles.

## Storage and privacy

| Stored item | Location | When it is removed or kept |
| --- | --- | --- |
| Display name and device ID | Local SQLite database, `profile` table | Kept between launches so the device can rejoin. |
| Waiting message text | Local `outbox` table | Removed after acceptance, cancellation or queue expiry. Unconfirmed attempts are discarded during recovery. |
| Accepted message text | Chat screen and local `messages` table | Removed on expiry. This version also clears accepted messages when the app is hidden or started again. A message can therefore disappear early when switching apps. |
| Processed and outgoing IDs | Local `seen_ids` and `own_ids` tables | Kept to recognise repeat events and identify the sender’s own messages. These tables contain no message text. |
| Live messages at the relay | Relay memory, according to the shared protocol | The relay is expected to delete each message at its deadline. The client cannot inspect the host’s memory, logs or backups. |

A classmate can take a screenshot or copy text before it disappears. Anyone with the relay address can join under a display name. A powered-off phone cannot run cleanup at the moment of expiry, so the app clears old accepted messages when it starts. Deleting a database row shows that the app no longer returns that row. It does not prove that every copy of the text has been erased from the device.

## Testing with a classmate

The app has been run on a Vivo phone and in Chrome. The earlier development relay was used for local testing. On the hosted v1 relay, a Chrome join attempt reported `ERR_CONNECTION_RESET`. A successful two-way exchange with an independent class app is still to be confirmed.

Use a quiet room and distinct test phrases so it is easy to tell which message each result belongs to.

| Test | Steps | What to check |
| --- | --- | --- |
| Send both ways | Join from two phones with different display names. Send “Lab 3?” from one and “Yes, second floor” from the other. | Both phones show both messages once. Compare the countdown for the same message on both screens. |
| Check deletion | Send one message. Open **About this room** to read the accepted-row count. Close the sheet, wait for expiry, then open it again. | With no other messages arriving, the bubble disappears and the count returns to zero. Reopening the sheet gives a fresh count. |
| Reconnect in time | On the sender, tap the Wi-Fi button, send a message, then reconnect after about 5 seconds. | If the relay accepts it before the queue deadline, the recipient sees one copy. |
| Reconnect too late | Repeat with a new message and stay offline for more than 30 seconds. | The waiting entry disappears. The recipient never sees that message after reconnection. |
| Check for replay | Let a live message expire. Disconnect and rejoin the room. | The expired message stays gone. |
| Check a repeated ID | In an isolated client test, feed the receiver the same valid incoming message twice using the same ID. | The receiver shows one bubble and stores one accepted row. Sending the same words twice through the composer creates two different IDs and tests a different case. |

To run the Join screen widget test from the project folder:

```powershell
flutter test test/widget_test.dart
```

This test checks that entering only spaces shows the name validation error. The two-device checks above exercise the network and storage behaviour separately.

## Troubleshooting

| What you see | What to try |
| --- | --- |
| **Relay unavailable** | Check internet access on the affected device. Ask a classmate using the same address to join at the same time. Give the host about a minute to wake, then retry. If only one phone fails, try that phone on another network. |
| **Protocol mismatch** | Compare the address and protocol version with the class specification. The older local server in `tool/dev_relay.dart` speaks a different protocol. |
| Phone missing from `flutter devices` | Unlock it, reconnect with a USB data cable and accept the debugging prompt. Run `flutter devices` again. |
| A classmate cannot open your `localhost` link | Have them launch their own compatible client or install the Android APK. Both clients then join the hosted class room. |
| A message disappears after switching apps | This version clears accepted messages when hidden. Keep both apps visible when comparing live countdowns. |

## Files worth reading

| File | What to look for |
| --- | --- |
| `lib/main.dart` | Join and chat screens, the relay address, WebSocket events, reconnect logic and countdowns. |
| `lib/local_store.dart` | The database tables, queue limit, duplicate checks and deletion of message rows. |
| `test/widget_test.dart` | The Join screen’s display-name validation test. |
| `tool/dev_relay.dart` | The earlier local relay used during development. |
| `pubspec.yaml` | Flutter dependencies used by the project. |

## Credits

Sawaira Waheed proposed the shared relay protocol used as the compatibility target. Google Stitch was used for interface design. OpenAI ChatGPT/Codex assisted with implementation, debugging and documentation.

The app uses Flutter for the interface, a WebSocket channel for communication, UUIDs for message and device IDs, and SQLite for local storage. [Flutter](https://flutter.dev/) · [web_socket_channel](https://pub.dev/packages/web_socket_channel) · [uuid](https://pub.dev/packages/uuid) · [sqflite](https://pub.dev/packages/sqflite) · [sqflite_common_ffi_web](https://pub.dev/packages/sqflite_common_ffi_web)
