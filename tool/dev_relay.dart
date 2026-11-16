import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

final clients = <WebSocket, Map<String, String>>{};
final records = <String, Map<String, dynamic>>{};

final clock = Stopwatch()..start();
final startedAt = DateTime.now().millisecondsSinceEpoch;

// A different identifier each time this server starts.
final epoch = List.generate(
  16,
  (_) => Random.secure().nextInt(256).toRadixString(16).padLeft(2, '0'),
).join();

int serverNow() => startedAt + clock.elapsedMilliseconds;

void reply(WebSocket socket, Map<String, dynamic> event) {
  if (socket.readyState == WebSocket.open) {
    socket.add(jsonEncode(event));
  }
}

void broadcast(Map<String, dynamic> event) {
  for (final socket in clients.keys.toList()) {
    reply(socket, event);
  }
}

void fail(WebSocket socket, String code, String message) {
  reply(socket, {
    'type': 'error',
    'code': code,
    'message': message,
  });
}

bool expire(String id) {
  final record = records[id];
  if (record == null) return true;

  if (serverNow() < (record['expires_at'] as int)) {
    return false;
  }

  // Keep non-content metadata for duplicate detection.
  // Remove the message body from the relay's records.
  if (record.containsKey('text')) {
    record.remove('text');
    record.remove('receipts');

    broadcast({
      'type': 'expired',
      'id': id,
      'expires_at': record['expires_at'],
    });
  }

  return true;
}

void confirmAcceptance(
  WebSocket socket,
  String id,
  Map<String, dynamic> record,
) {
  reply(socket, {
    'type': 'accepted',
    'id': id,
    'sent_at': record['sent_at'],
    'expires_at': record['expires_at'],
    'relay_epoch': epoch,
  });
}

void handle(WebSocket socket, dynamic raw) {
  try {
    if (raw is! String || utf8.encode(raw).length > 8192) {
      fail(socket, 'bad_payload', 'Invalid or oversized message.');
      return;
    }

    final decoded = jsonDecode(raw);
    if (decoded is! Map<String, dynamic>) {
      fail(socket, 'bad_payload', 'Expected a JSON object.');
      return;
    }

    final event = decoded;
    final type = event['type'];

    if (type == 'join') {
      if (event['version'] != 'dev-v1') {
        fail(socket, 'unsupported_version', 'Expected dev-v1.');
        return;
      }

      final name = event['display_name'];
      final deviceId = event['device_id'];

      if (name is! String ||
          name.trim().isEmpty ||
          name.trim().length > 24 ||
          deviceId is! String ||
          deviceId.isEmpty ||
          deviceId.length > 128) {
        fail(socket, 'bad_payload', 'Invalid name or device ID.');
        return;
      }

      if (clients.containsKey(socket)) {
        fail(socket, 'already_joined', 'This connection already joined.');
        return;
      }

      clients[socket] = {
        'device_id': deviceId,
        'display_name': name.trim(),
      };

      reply(socket, {
        'type': 'welcome',
        'version': 'dev-v1',
        'relay_epoch': epoch,
        'server_now': serverNow(),
      });
      return;
    }

    final sender = clients[socket];
    if (sender == null) {
      fail(socket, 'not_joined', 'Join before sending events.');
      return;
    }

    if (type == 'ping') {
      reply(socket, {
        'type': 'pong',
        'nonce': event['nonce'],
        'server_now': serverNow(),
      });
      return;
    }

    final id = event['id'];
    if (id is! String || id.isEmpty || id.length > 128) {
      fail(socket, 'bad_payload', 'A valid message ID is required.');
      return;
    }

    if (type == 'send') {
      final text = event['text'];
      if (text is! String ||
          text.trim().isEmpty ||
          text.length > 500) {
        fail(socket, 'bad_payload', 'Text must be 1–500 characters.');
        return;
      }

      final existing = records[id];

      if (existing != null) {
        if (existing['owner'] != sender['device_id']) {
          fail(socket, 'id_conflict', 'Message ID already belongs to a device.');
          return;
        }

        expire(id);

        // A retry gets the original timestamps.
        // It is NEVER broadcast again.
        confirmAcceptance(socket, id, existing);
        return;
      }

      if (records.length >= 10000) {
        fail(socket, 'capacity', 'Development relay capacity reached.');
        return;
      }

      final sentAt = serverNow();
      final record = <String, dynamic>{
        'owner': sender['device_id'],
        'text': text.trim(),
        'sent_at': sentAt,
        'expires_at': sentAt + 30000,
        'receipts': <String>{},
      };

      records[id] = record;
      confirmAcceptance(socket, id, record);

      broadcast({
        'type': 'message',
        'id': id,
        'sender': sender,
        'text': record['text'],
        'sent_at': record['sent_at'],
        'expires_at': record['expires_at'],
        'relay_epoch': epoch,
      });

      // Capture only the ID, not the message body.
      Timer(const Duration(seconds: 30), () => expire(id));
      return;
    }

    if (type == 'status') {
      final record = records[id];

      if (record != null && record['owner'] != sender['device_id']) {
        fail(socket, 'id_conflict', 'Cannot query another sender’s message.');
        return;
      }

      final result = record == null
          ? 'unknown'
          : expire(id)
              ? 'expired'
              : 'accepted';

      reply(socket, {
        'type': 'status_result',
        'id': id,
        'result': result,
        'relay_epoch': epoch,
        if (record != null) 'sent_at': record['sent_at'],
        if (record != null) 'expires_at': record['expires_at'],
      });
      return;
    }

    if (type == 'delivered') {
      final record = records[id];
      if (record == null || expire(id)) return;

      final recipientId = sender['device_id']!;
      if (record['owner'] == recipientId) return;

      final receipts = record['receipts'] as Set<String>;
      if (!receipts.add(recipientId)) return;

      for (final entry in clients.entries) {
        if (entry.value['device_id'] == record['owner']) {
          reply(entry.key, {
            'type': 'receipt',
            'id': id,
            'recipient_device_id': recipientId,
          });
        }
      }
      return;
    }

    fail(socket, 'bad_payload', 'Unknown event type.');
  } catch (_) {
    // Do not print message bodies or raw incoming frames.
    fail(socket, 'bad_payload', 'Could not process this event.');
  }
}

Future<void> main() async {
  // Laptop-only access for initial Chrome development.
  final server = await HttpServer.bind(
    InternetAddress.anyIPv4,
    8081,
  );

  print('Vanish development relay is running.');
  print('Address: ws://127.0.0.1:8081');
  print('Protocol: dev-v1');
  print('Keep this terminal open. Press Ctrl+C to stop.');

  await for (final request in server) {
    if (!WebSocketTransformer.isUpgradeRequest(request)) {
      request.response
        ..statusCode = HttpStatus.ok
        ..write('Vanish development relay is running.');
      await request.response.close();
      continue;
    }

    try {
      final socket = await WebSocketTransformer.upgrade(request);
      socket.pingInterval = const Duration(seconds: 20);

      socket.listen(
        (data) => handle(socket, data),
        onDone: () => clients.remove(socket),
        onError: (Object error) => clients.remove(socket),
        cancelOnError: true,
      );
    } catch (_) {
      // Ignore a failed connection upgrade without logging content.
    }
  }
}