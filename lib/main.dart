import 'dart:async';
import 'dart:convert';

import 'package:flutter/material.dart';
import 'package:uuid/uuid.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'local_store.dart';

const primaryColor = Color(0xFF4F46E5);
const backgroundColor = Color(0xFFF6F8FC);
const headingColor = Color(0xFF14213D);
const secondaryColor = Color(0xFF526176);

// Sawaira's proposed protocol v1. Use only with the class-approved relay.
const relayAddress = 'wss://vanish-relay-sawaira.bonto.page/ws';
// This is a local namespace, NOT a server epoch (v1 has no epoch field).
const protocolScope = 'sawaira-v1';

void main() {
  WidgetsFlutterBinding.ensureInitialized();
  runApp(const VanishApp());
}

class VanishApp extends StatelessWidget {
  const VanishApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      debugShowCheckedModeBanner: false,
      title: 'Vanish',
      theme: ThemeData(
        useMaterial3: true,
        scaffoldBackgroundColor: backgroundColor,
        colorScheme: ColorScheme.fromSeed(
          seedColor: primaryColor,
          primary: primaryColor,
        ),
        inputDecorationTheme: InputDecorationTheme(
          filled: true,
          fillColor: Colors.white,
          contentPadding: const EdgeInsets.all(14),
          border: OutlineInputBorder(borderRadius: BorderRadius.circular(12)),
          enabledBorder: OutlineInputBorder(
            borderRadius: BorderRadius.circular(12),
            borderSide: const BorderSide(color: Color(0xFFDCE2EF)),
          ),
        ),
        filledButtonTheme: FilledButtonThemeData(
          style: FilledButton.styleFrom(
            minimumSize: const Size(48, 52),
            shape: RoundedRectangleBorder(
              borderRadius: BorderRadius.circular(12),
            ),
          ),
        ),
      ),
      home: const VanishHome(),
    );
  }
}

class LiveMessage {
  final String id;
  final String senderId;
  final String senderName;
  final String text;
  final int deadline;
  int? deliveryCount;

  LiveMessage({
    required this.id,
    required this.senderId,
    required this.senderName,
    required this.text,
    required this.deadline,
  });
}

class VanishHome extends StatefulWidget {
  final LocalStore? store;
const VanishHome({super.key, this.store});

  @override
  State<VanishHome> createState() => _VanishHomeState();
}

class _VanishHomeState extends State<VanishHome> with WidgetsBindingObserver {
  late final LocalStore _store = widget.store ?? LocalStore();
  final _uuid = const Uuid();
  final _elapsed = Stopwatch()..start();

  final _name = TextEditingController();
  final _draft = TextEditingController();
  final _formKey = GlobalKey<FormState>();

  final _messages = <LiveMessage>[];
  final _pendingDeadlines = <String, int>{};
  bool _purging = false;
  bool _retryEnabled = false;

  List<Map<String, Object?>> _outbox = [];

  WebSocketChannel? _channel;
  StreamSubscription<dynamic>? _subscription;

  Timer? _ticker;
  Timer? _heartbeat;
  Timer? _connectTimeout;
  Timer? _retry;
  Timer? _flightTimeout;

  // Serialises database operations and received events.
  Future<void> _work = Future<void>.value();

  bool _disposed = false;
  bool _ready = false;
  bool _loadingFailed = false;
  bool _joined = false;
  bool _hasJoinedBefore = false;
  bool _connecting = false;
  bool _connected = false;
  bool _clockReady = false;
  bool _foreground = true;
  bool _manualOffline = false;
  bool _saving = false;
  bool _expanded = false;

  String _deviceId = '';
  String _displayName = '';
  String _notice = '';

  String? _flightId;

  int _generation = 0;
  int _serverSample = 0;
  int _sampleTick = 0;

  Timer? _pongDeadline;
  int? _earlyDeliveryCount;
  int _retrySeconds = 1;

  int get _tick => _elapsed.elapsedMilliseconds;

  int get _serverNow => _serverSample + (_tick - _sampleTick);

  bool get _canDrain =>
      _connected && _clockReady && _foreground && !_manualOffline;

  void _redraw() {
    if (mounted && !_disposed) setState(() {});
  }

  // All callers share one ordered work queue.
  Future<void> _serial(Future<void> Function() action) {
    final next = _work.then((_) async {
      if (!_disposed) await action();
    });

    // Keep the queue usable even if one task fails.
    _work = next.catchError((Object error, StackTrace stack) {});

    return next;
  }

  void _background(Future<void> Function() action) {
    unawaited(
      _serial(action).catchError((Object error, StackTrace stack) {
        if (!_disposed) {
          _notice = 'An operation failed. Please reconnect and try again.';
          _redraw();
        }
      }),
    );
  }

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    _initialise();
  }

  Future<void> _initialise() async {
    try {
      await _store.open();
      final profile = await _store.loadProfile();
      final uncertain = await _store.discardUncertain();
      await _purgeWaiting();

      if (!mounted) return;

      _deviceId = profile['device_id'] as String;
      _displayName = profile['display_name'] as String;
      _name.text = _displayName;
      _hasJoinedBefore = profile['has_joined'] == 1;
      if (uncertain > 0) _notice = 'Delivery outcome unknown; not resent.';
      _ready = true;

      _ticker = Timer.periodic(const Duration(milliseconds: 250), (_) {
        if (!_foreground) return;
        if (!_purging && _outbox.any(_waitingExpired)) {
          _purging = true;
          _background(() async {
            try {
              await _purgeWaiting();
            } finally {
              _purging = false;
            }
          });
        }
        if (_messages.isEmpty) return;

        // Hide expired content immediately, then clean storage.
        final expired = _messages
            .where((m) => m.deadline <= _tick)
            .map((m) => m.id)
            .toList();

        _messages.removeWhere((m) => m.deadline <= _tick);
        _redraw();

        if (expired.isNotEmpty) {
          _background(() async {
            for (final id in expired) {
              await _store.removeExpiredMessage(id);
            }
          });
        }
      });

      _redraw();
    } catch (_) {
      _loadingFailed = true;
      _notice =
          'Database could not open. Check the web setup files '
          'and restart Flutter.';
      _redraw();
    }
  }

  @override
  void dispose() {
    _disposed = true;
    _generation++;

    WidgetsBinding.instance.removeObserver(this);

    _ticker?.cancel();
    _heartbeat?.cancel();
    _pongDeadline?.cancel();
    _connectTimeout?.cancel();
    _retry?.cancel();
    _flightTimeout?.cancel();
    _subscription?.cancel();
    _channel?.sink.close();

    // Let any current database transaction finish before closing.
    unawaited(_work.then((_) => _store.close()));

    _name.dispose();
    _draft.dispose();

    super.dispose();
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == AppLifecycleState.hidden ||
        state == AppLifecycleState.paused) {
      _foreground = false;
      _clockReady = false;
      _messages.clear();
      _redraw();

      if (_ready) {
        _background(() => _store.clearAcceptedMessages());
      }
    }

    // A visible but unfocused window is inactive, not hidden.
    // Only resynchronise after a real hidden/paused transition.
    // Ordinary focus changes must not invalidate a valid clock.
    if ((state == AppLifecycleState.resumed ||
            state == AppLifecycleState.inactive) &&
        !_foreground) {
      _foreground = true;
      _clockReady = false;
      // v1 pong carries no clock. Rejoin to obtain a fresh welcome time.
      if (!_manualOffline && (_joined || _retryEnabled)) {
        _lost(_generation, 'Refreshing relay time.', retry: false);
        _connect();
      }

      _redraw();
    }
  }

  void _write(Map<String, dynamic> event) {
    final channel = _channel;
    if (channel == null) {
      throw StateError('No active connection.');
    }
    channel.sink.add(jsonEncode(event));
  }

  Future<void> _join({bool offline = false}) async {
    if (_connecting || _saving) return;
    if (!_formKey.currentState!.validate()) return;

    _saving = true;
    _redraw();

    try {
      final displayName = _name.text.trim();

      await _serial(() => _store.saveName(displayName));

      if (!mounted) return;

      _displayName = displayName;
      FocusScope.of(context).unfocus();

      if (offline) {
        _manualOffline = true;
        _joined = true;
        _notice = 'Offline mode. New messages will wait on this device.';
      } else {
        _manualOffline = false;
        _retryEnabled = true;
        await _connect();
      }
    } catch (_) {
      _notice = 'Could not save your name. Please try again.';
    } finally {
      _saving = false;
      _redraw();
    }
  }

  Future<void> _connect() async {
    if (_disposed || !_foreground || _connecting || _manualOffline) return;

    _retry?.cancel();

    final generation = ++_generation;

    _connecting = true;
    _connected = false;
    _clockReady = false;

    _notice = '';
    _redraw();

    _connectTimeout?.cancel();
    _connectTimeout = Timer(
      const Duration(seconds: 75),
      () => _lost(generation, 'Connection timed out.'),
    );

    try {
      await _subscription?.cancel();
      await _channel?.sink.close();

      if (_disposed || generation != _generation) return;

      final channel = WebSocketChannel.connect(Uri.parse(relayAddress));

      _channel = channel;
      _subscription = channel.stream.listen(
        (raw) {
          final receivedTick = _tick;
          _background(() async {
            if (generation == _generation) {
              await _handle(raw, receivedTick);
            }
          });
        },
        onDone: () =>
            _lost(generation, 'Connection lost. Messages will wait here.'),
        onError: (Object error) => _lost(
          generation,
          'Relay unavailable. Check that the server is running.',
        ),
        cancelOnError: true,
      );

      await channel.ready;
      if (_disposed || generation != _generation) return;
      _write({
        'type': 'join',
        'protocol': 1,
        'device_id': _deviceId,
        'name': _displayName,
      });
    } catch (_) {
      _lost(generation, 'Could not connect to the relay.');
    }
  }

  void _lost(int generation, String reason, {bool retry = true}) {
    if (_disposed || generation != _generation) return;

    _generation++;

    _connectTimeout?.cancel();
    _heartbeat?.cancel();
    _pongDeadline?.cancel();
    _flightTimeout?.cancel();
    _subscription?.cancel();
    _channel?.sink.close();
    _channel = null;

    _connected = false;
    _connecting = false;
    _clockReady = false;
    _flightId = null;
    _earlyDeliveryCount = null;

    _notice = reason;
    _redraw();

    if (_ready) {
      _background(() async {
        final uncertain = await _store.discardUncertain();
        if (uncertain > 0) _notice = 'Delivery outcome unknown; not resent.';
        await _reloadOutbox();
        _redraw();
      });
    }

    if (retry && _retryEnabled && !_manualOffline) {
      _retry?.cancel();

      final delay = _retrySeconds;
      _retrySeconds = (_retrySeconds * 2).clamp(1, 8).toInt();

      _retry = Timer(Duration(seconds: delay), () {
        if (!_disposed && !_manualOffline) _connect();
      });
    }
  }

  void _armPongDeadline() {
    _pongDeadline?.cancel();
    final generation = _generation;
    _pongDeadline = Timer(const Duration(seconds: 30), () {
      _lost(generation, 'Heartbeat timed out. Reconnecting...');
    });
  }

  void _sendPing() {
    if (!_connected) return;

    try {
      _write({'type': 'ping'});
    } catch (_) {
      _lost(_generation, 'Connection lost.');
    }
  }

  Future<void> _handle(dynamic raw, int receivedTick) async {
    Map<String, dynamic> event;

    try {
      if (raw is! String) return;
      final decoded = jsonDecode(raw);
      if (decoded is! Map<String, dynamic>) return;
      event = decoded;
    } catch (_) {
      _notice = 'An invalid relay event was ignored.';
      _redraw();
      return;
    }

    switch (event['type']) {
      case 'welcome':
        if (event['protocol'] != 1 || event['server_time'] is! int) {
          _lost(_generation, 'Protocol mismatch. Expected protocol 1.', retry: false);
          return;
        }
        if (!_foreground) return;
        // Equivalent to server_time - phone_time offset at welcome, but
        // advanced using a monotonic stopwatch so wall-clock edits cannot
        // change an active countdown. Network delay is not measurable here.
        _serverSample = event['server_time'] as int;
        _sampleTick = receivedTick;
        _clockReady = true;
        _connected = true;
        _connecting = false;
        _joined = true;
        _retryEnabled = true;
        _retrySeconds = 1;
        _armPongDeadline();
        _connectTimeout?.cancel();
        _heartbeat?.cancel();
        _heartbeat = Timer.periodic(const Duration(seconds: 15), (_) => _sendPing());
        if (!_hasJoinedBefore) {
          await _store.markJoined();
          _hasJoinedBefore = true;
        }
        await _advanceQueue();
        break;

      case 'pong':
        if (_connected) _armPongDeadline();
        break;

      case 'message':
        if (_connected) await _handleMessage(event);
        break;

      case 'delivered':
        final id = event['id'];
        final count = event['count'];
        if (id is String && count is int && count >= 0) {
          if (id == _flightId) _earlyDeliveryCount = count;
          for (final message in _messages) {
            if (message.id == id && message.senderId == _deviceId) {
              message.deliveryCount = count;
            }
          }
        }
        break;

      case 'expired':
        final id = event['id'];

        if (id is String) {
          _messages.removeWhere((message) => message.id == id);
          await _store.rememberId(id, protocolScope, 0);
          await _store.removeExpiredMessage(id);
          _finishFlight(id);
          await _reloadOutbox();
          await _advanceQueue();
        }
        break;

      case 'error':
        final code = event['code'];
        final message = event['message'] is String
            ? event['message'] as String
            : 'The relay rejected a request.';

        if (code == 'not_joined') {
          _lost(_generation, 'Session ended. Rejoining the class...');
          return;
        }
        final rejected = _flightId;
        if (code == 'bad_message' && rejected != null) {
          // There is exactly one outstanding send on this connection.
          await _store.discardOutboxItem(rejected);
          _finishFlight(rejected);
          _notice = message;
          await _reloadOutbox();
          await _advanceQueue();
          break;
        }
        // Configuration errors require user action before retrying.
        _retryEnabled = false;
        _manualOffline = _joined;
        _lost(
          _generation,
          '$message Check the relay before reconnecting.',
          retry: false,
        );

        if (code == 'bad_protocol') {
          _notice = 'Protocol mismatch. Expected protocol 1.';
        }
        break;
    }

    _redraw();
  }

  Future<void> _reloadOutbox() async {
    _outbox = await _store.loadOutbox();
    final ids = _outbox.map((row) => row['id'] as String).toSet();
    _pendingDeadlines.removeWhere((id, _) => !ids.contains(id));
    final wallNow = DateTime.now().millisecondsSinceEpoch;
    for (final row in _outbox) {
      final id = row['id'] as String;
      final age = wallNow - (row['created_at_local'] as int);
      // Across process restarts only wall time is available offline.
      // Reject future-dated rows on rollback rather than extend their TTL.
      _pendingDeadlines.putIfAbsent(id,
        () => _tick + (age < 0 ? 0 : (30000 - age).clamp(0, 30000).toInt()));
    }
  }

  bool _waitingExpired(Map<String, Object?> row) =>
      row['state'] == 'waiting' && (_pendingDeadlines[row['id']] ?? 0) <= _tick;

  Future<void> _purgeWaiting() async {
    await _reloadOutbox();
    final expired = _outbox.where(_waitingExpired).toList();
    for (final row in expired) {
      await _store.cancelWaiting(row['id'] as String);
    }
    if (expired.isNotEmpty) {
      _notice = 'Unsent messages older than 30 seconds were dropped.';
      await _reloadOutbox();
      _redraw();
    }
  }

  void _finishFlight(String id) {
    if (_flightId == id) {
      _flightId = null;
      _flightTimeout?.cancel();
    }
  }

  void _watchFlight(String id) {
    _flightTimeout?.cancel();
    final generation = _generation;

    _flightTimeout = Timer(const Duration(seconds: 30), () {
      if (_flightId == id && generation == _generation) {
        _lost(
          generation,
          'Delivery outcome unknown; not resent.',
        );
      }
    });
  }

  // The proposal has no acceptance-status query. One attempted send is
  // persisted before writing, and is NEVER automatically sent a second time.
  Future<void> _advanceQueue() async {
    if (!_canDrain || _flightId != null) return;
    final generation = _generation;
    await _purgeWaiting();
    while (_outbox.isNotEmpty && _canDrain && generation == _generation) {
      final item = _outbox.first;
      final id = item['id'] as String;
      if (item['state'] == 'in_flight') {
        await _store.discardOutboxItem(id);
        _notice = 'Delivery outcome unknown; not resent.';
        await _reloadOutbox();
        continue;
      }
      if ((_pendingDeadlines[id] ?? 0) <= _tick) {
        await _purgeWaiting();
        continue;
      }
      final claimed = await _store.markInFlight(id, protocolScope);
      if (!claimed) {
        await _reloadOutbox();
        continue;
      }
      if (!_canDrain || generation != _generation) {
        await _store.markWaiting(id);
        await _reloadOutbox();
        return;
      }
      // Recheck after the database await, immediately before transmission.
      if ((_pendingDeadlines[id] ?? 0) <= _tick) {
        await _store.discardOutboxItem(id);
        await _reloadOutbox();
        continue;
      }
      _earlyDeliveryCount = null;
      _flightId = id;
      try {
        _write({'type': 'message', 'id': id, 'sender': _displayName,
          'text': item['text']});
        _watchFlight(id);
      } catch (_) {
        _lost(_generation, 'Delivery outcome unknown; not resent.');
      }
      await _reloadOutbox();
      return;
    }
  }

  bool _validTimes(Map<String, dynamic> event) {
    return event['sent_at'] is int &&
        event['expires_at'] is int &&
        (event['expires_at'] as int) - (event['sent_at'] as int) == 30000;
  }

  Future<void> _handleMessage(Map<String, dynamic> event) async {
    final id = event['id'];
    final sender = event['sender'];
    final text = event['text'];
    if (id is! String || id.isEmpty || sender is! String ||
        sender.trim().isEmpty || text is! String || text.isEmpty ||
        text.runes.length > 500 || !_validTimes(event)) return;
    // Names are not unique. Match our own sends by their durable UUIDs.
    final own = await _store.isOwnId(id);
    await _recordMessage(
      id: id, senderId: own ? _deviceId : '', senderName: sender,
      text: text, sentAt: event['sent_at'] as int,
      expiresAt: event['expires_at'] as int,
    );
    if (own) {
      if (id == _flightId && _earlyDeliveryCount != null) {
        for (final message in _messages) {
          if (message.id == id) message.deliveryCount = _earlyDeliveryCount;
        }
        _earlyDeliveryCount = null;
      }
      await _store.discardOutboxItem(id);
      _finishFlight(id);
      await _reloadOutbox();
      await _advanceQueue();
    }
    // v1 sends delivered/count FROM the relay. No client delivered event.
  }

  Future<bool> _recordMessage({
    required String id,
    required String senderId,
    required String senderName,
    required String text,
    required int sentAt,
    required int expiresAt,
  }) async {
    // Do not confuse missing clock synchronisation with an expired
    // message. In particular, do not delete an outgoing queued body.
    if (!_foreground || !_clockReady) return false;
    if (_messages.any((message) => message.id == id)) return false;
    if (await _store.hasSeen(id)) return false;
    if (!_foreground || !_clockReady) return false;

    const epoch = protocolScope;
    final remaining = expiresAt - _serverNow;

    // Only a trusted deadline (or the explicit capacity limit) drops
    // content here. Tombstones continue to prevent expired replay.
    if (remaining <= 0 || _messages.length >= 500) {
      await _store.db.transaction((transaction) async {
        await transaction.rawInsert(
          'INSERT OR IGNORE INTO seen_ids '
          '(id, relay_epoch, expires_at) VALUES (?, ?, ?)',
          [id, epoch, expiresAt],
        );

        await transaction.delete('outbox', where: 'id = ?', whereArgs: [id]);
      });

      if (senderId == _deviceId) {
        _notice = remaining <= 0
            ? 'The relay says this message has already expired.'
            : 'Live message limit reached; message not displayed.';
      }
      return false;
    }

    final generation = _generation;
    final deadline = _tick + remaining.clamp(0, 30000).toInt();

    await _store.saveAcceptedMessage(
      id: id,
      senderId: senderId,
      senderName: senderName,
      text: text,
      sentAt: sentAt,
      expiresAt: expiresAt,
      epoch: epoch,
    );

    // Storage writes can finish after expiry or a lifecycle change.
    if (!_foreground || generation != _generation || deadline <= _tick) {
      await _store.removeExpiredMessage(id);
      return false;
    }

    _messages.add(
      LiveMessage(
        id: id,
        senderId: senderId,
        senderName: senderName,
        text: text,
        deadline: deadline,
      ),
    );

    _redraw();
    return true;
  }

  Future<void> _queueMessage() async {
    final text = _draft.text.trim();

    if (_saving || text.isEmpty || text.runes.length > 500) return;

    _saving = true;
    _redraw();
    final id = _uuid.v4();
    final createdAt = DateTime.now().millisecondsSinceEpoch;
    final queueDeadline = _tick + 30000;

    try {
      await _serial(() async {
        await _purgeWaiting();
        await _store.enqueue(id: id, text: text, createdAt: createdAt);
        _pendingDeadlines[id] = queueDeadline;
        await _reloadOutbox();
      });

      // Clear only after the local write succeeds.
      _draft.clear();
      _notice = '';

      _background(() => _advanceQueue());
    } on StateError catch (error) {
      _notice = error.message == 'queue_full'
          ? 'Your pending queue is full. Cancel a waiting message.'
          : "Couldn't save your message. It hasn't been queued.";
    } catch (_) {
      _notice = "Couldn't save your message. It hasn't been queued.";
    } finally {
      _saving = false;
      _redraw();
    }
  }

  Future<void> _cancelWaiting(String id) async {
    try {
      await _serial(() async {
        await _store.cancelWaiting(id);
        await _reloadOutbox();
      });

      _redraw();
    } catch (_) {
      _notice = 'Could not cancel this item. Please try again.';
      _redraw();
    }
  }

  void _toggleTestConnection() {
    if (_manualOffline || (!_connected && !_connecting)) {
      _manualOffline = false;
      _retryEnabled = true;
      _connect();
    } else {
      _manualOffline = true;
      _retry?.cancel();

      _lost(
        _generation,
        'Offline test mode. New messages will wait on this device.',
        retry: false,
      );
    }
  }

  Future<void> _showInfo() async {
    int? storedCount;

    try {
      await _serial(() async {
        final rows = await _store.db.rawQuery(
          'SELECT COUNT(*) AS total FROM messages',
        );
        storedCount = rows.first['total'] as int;
      });
    } catch (_) {
      // The information sheet can still open.
    }

    if (!mounted) return;

    showModalBottomSheet<void>(
      context: context,
      showDragHandle: true,
      isScrollControlled: true,
      constraints: const BoxConstraints(maxWidth: 480),
      builder: (context) => SafeArea(
        child: SingleChildScrollView(
          padding: const EdgeInsets.fromLTRB(24, 0, 24, 24),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            mainAxisSize: MainAxisSize.min,
            children: [
              const Text(
                'About this room',
                style: TextStyle(
                  fontSize: 22,
                  fontWeight: FontWeight.bold,
                  color: headingColor,
                ),
              ),
              const SizedBox(height: 20),
              const Text(
                '• Messages expire 30 seconds after the relay '
                'accepts them.\n\n'
                '• Offline messages wait in local storage. '
                'They are dropped 30 seconds after Send if still unsent.\n\n'
                '• Screenshots and copies are possible.\n\n'
                '• This prototype clears accepted messages when hidden '
                'or restarted. Unsent messages have a 30-second queue limit.\n\n'
                '• Cleanup cannot execute while the app is killed '
                'or the phone is off.',
                style: TextStyle(fontSize: 15, height: 1.5),
              ),
              const Divider(height: 32),
              const Text(
                'Development check — snapshot',
                style: TextStyle(fontWeight: FontWeight.bold),
              ),
              const SizedBox(height: 8),
              Text(
                'Accepted database rows: ${storedCount ?? 'Unavailable'}\n'
                'Pending messages: ${_outbox.length}',
              ),
              const SizedBox(height: 8),
              const Text(
                'Row counts do not prove that all file bytes '
                'have been erased. Android file inspection is still needed.',
                style: TextStyle(fontSize: 12, color: secondaryColor),
              ),
              const SizedBox(height: 20),
              SizedBox(
                width: double.infinity,
                child: FilledButton(
                  onPressed: () => Navigator.pop(context),
                  child: const Text('Got it'),
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _noticeWidget() {
    if (_notice.isEmpty) return const SizedBox.shrink();

    return Container(
      width: double.infinity,
      padding: const EdgeInsets.fromLTRB(12, 6, 0, 6),
      color: const Color(0xFFFFF4DB),
      child: Row(
        children: [
          const Icon(Icons.info_outline, size: 18, color: Color(0xFF855100)),
          const SizedBox(width: 8),
          Expanded(
            child: Text(
              _notice,
              style: const TextStyle(fontSize: 13, color: Color(0xFF704300)),
            ),
          ),
          IconButton(
            tooltip: 'Dismiss',
            onPressed: () {
              _notice = '';
              _redraw();
            },
            icon: const Icon(Icons.close, size: 18),
          ),
        ],
      ),
    );
  }

  Widget _joinScreen() {
    return Scaffold(
      body: SafeArea(
        child: Center(
          child: SingleChildScrollView(
            padding: const EdgeInsets.all(24),
            child: Form(
              key: _formKey,
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Container(
                    width: 56,
                    height: 56,
                    decoration: BoxDecoration(
                      color: primaryColor,
                      borderRadius: BorderRadius.circular(16),
                    ),
                    child: const Icon(
                      Icons.timer_outlined,
                      color: Colors.white,
                      size: 32,
                    ),
                  ),
                  const SizedBox(height: 24),
                  const Text(
                    'Vanish',
                    style: TextStyle(
                      fontSize: 32,
                      fontWeight: FontWeight.bold,
                      color: headingColor,
                    ),
                  ),
                  const SizedBox(height: 8),
                  const Text(
                    'A quick conversation with your class.',
                    style: TextStyle(fontSize: 16, color: secondaryColor),
                  ),
                  const SizedBox(height: 24),
                  Container(
                    padding: const EdgeInsets.all(16),
                    decoration: BoxDecoration(
                      color: Colors.white,
                      borderRadius: BorderRadius.circular(12),
                    ),
                    child: const Text(
                      'Messages expire 30 seconds after the relay '
                      'accepts them.\n\n'
                      'Screenshots and copies are still possible.',
                      style: TextStyle(color: headingColor, height: 1.5),
                    ),
                  ),
                  const SizedBox(height: 24),
                  TextFormField(
                    controller: _name,
                    enabled: !_connecting && !_saving,
                    maxLength: 30,
                    textInputAction: TextInputAction.go,
                    decoration: const InputDecoration(
                      labelText: 'Display name',
                      hintText: 'e.g. Ali',
                    ),
                    validator: (value) {
                      final name = value?.trim() ?? '';
                      if (name.isEmpty || name.runes.length > 30) {
                        return 'Enter a name between 1 and 30 characters.';
                      }
                      return null;
                    },
                    onFieldSubmitted: (_) => _join(),
                  ),
                  const SizedBox(height: 12),
                  _noticeWidget(),
                  const SizedBox(height: 20),
                  SizedBox(
                    width: double.infinity,
                    child: FilledButton(
                      onPressed: _connecting || _saving ? null : _join,
                      child: Text(
                        _connecting || _saving ? 'Joining…' : 'Join class room',
                      ),
                    ),
                  ),
                  if (_hasJoinedBefore) ...[
                    const SizedBox(height: 8),
                    SizedBox(
                      width: double.infinity,
                      child: TextButton(
                        onPressed: _connecting || _saving
                            ? null
                            : () => _join(offline: true),
                        child: const Text('Open offline'),
                      ),
                    ),
                  ],
                  const SizedBox(height: 12),
                  const Center(
                    child: Text(
                      'Shared relay · protocol 1',
                      style: TextStyle(fontSize: 12, color: secondaryColor),
                    ),
                  ),
                ],
              ),
            ),
          ),
        ),
      ),
    );
  }

  Widget _messageBubble(LiveMessage message) {
    final remaining = message.deadline - _tick;
    if (remaining <= 0) return const SizedBox.shrink();

    final own = message.senderId == _deviceId;
    final seconds = (remaining / 1000).ceil();

    return Align(
      alignment: own ? Alignment.centerRight : Alignment.centerLeft,
      child: Padding(
        padding: const EdgeInsets.only(bottom: 18),
        child: ConstrainedBox(
          constraints: BoxConstraints(
            maxWidth: MediaQuery.sizeOf(context).width.clamp(0, 480) * 0.78,
          ),
          child: Column(
            crossAxisAlignment: own
                ? CrossAxisAlignment.end
                : CrossAxisAlignment.start,
            children: [
              Text(
                own ? 'You' : message.senderName,
                style: const TextStyle(fontSize: 12, color: secondaryColor),
              ),
              const SizedBox(height: 5),
              Container(
                padding: const EdgeInsets.symmetric(
                  horizontal: 14,
                  vertical: 11,
                ),
                decoration: BoxDecoration(
                  color: own ? const Color(0xFFE5E7FF) : Colors.white,
                  borderRadius: BorderRadius.circular(16),
                ),
                child: Text(
                  message.text,
                  style: const TextStyle(
                    fontSize: 16,
                    height: 1.4,
                    color: headingColor,
                  ),
                ),
              ),
              const SizedBox(height: 5),
              Text(
                '${own ? (message.deliveryCount == null ? 'Sent · ' : 'Relay reports ${message.deliveryCount} deliveries · ') : ''}'
                '${seconds}s left',
                style: TextStyle(
                  fontSize: 12,
                  color: seconds <= 5 ? const Color(0xFF945800) : primaryColor,
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }

  Widget _pendingSection() {
    if (_outbox.isEmpty) return const SizedBox.shrink();

    final visible = _expanded ? _outbox : _outbox.take(2).toList();

    final maxHeight = MediaQuery.sizeOf(context).height * 0.28;

    return Container(
      padding: const EdgeInsets.fromLTRB(12, 4, 12, 8),
      decoration: const BoxDecoration(
        color: backgroundColor,
        border: Border(top: BorderSide(color: Color(0xFFDCE2EF))),
      ),
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          Row(
            children: [
              Expanded(
                child: Text(
                  'Pending · ${_outbox.length}',
                  style: const TextStyle(
                    fontWeight: FontWeight.w600,
                    color: headingColor,
                  ),
                ),
              ),
              if (_outbox.length > 2)
                TextButton(
                  onPressed: () {
                    _expanded = !_expanded;
                    _redraw();
                  },
                  child: Text(
                    _expanded ? 'Show less' : 'Show all (${_outbox.length})',
                  ),
                ),
            ],
          ),
          ConstrainedBox(
            constraints: BoxConstraints(maxHeight: maxHeight),
            child: ListView.builder(
              shrinkWrap: true,
              itemCount: visible.length,
              itemBuilder: (context, index) {
                final item = visible[index];
                final waiting = item['state'] == 'waiting';

                return Card(
                  elevation: 0,
                  color: Colors.white,
                  margin: const EdgeInsets.only(bottom: 6),
                  child: Padding(
                    padding: const EdgeInsets.fromLTRB(12, 8, 4, 8),
                    child: Row(
                      children: [
                        Expanded(
                          child: Column(
                            crossAxisAlignment: CrossAxisAlignment.start,
                            children: [
                              Text(
                                item['text'] as String,
                                maxLines: 2,
                                overflow: TextOverflow.ellipsis,
                              ),
                              const SizedBox(height: 5),
                              Text(
                                waiting
                                    ? 'Waiting — drops after 30 seconds'
                                    : 'Sending — awaiting relay echo…',
                                style: const TextStyle(
                                  fontSize: 12,
                                  color: secondaryColor,
                                ),
                              ),
                            ],
                          ),
                        ),
                        if (waiting)
                          TextButton(
                            onPressed: () =>
                                _cancelWaiting(item['id'] as String),
                            child: const Text('Cancel'),
                          )
                        else
                          const Padding(
                            padding: EdgeInsets.all(12),
                            child: Icon(Icons.schedule, color: primaryColor),
                          ),
                      ],
                    ),
                  ),
                );
              },
            ),
          ),
        ],
      ),
    );
  }

  Widget _chatScreen() {
    final status = _manualOffline
        ? 'Offline'
        : _connecting
        ? 'Reconnecting…'
        : !_connected
        ? 'Offline'
        : !_clockReady
        ? 'Checking…'
        : 'Connected';

    final queueFull = _outbox.length >= 50;

    return Scaffold(
      appBar: AppBar(
        backgroundColor: Colors.white,
        surfaceTintColor: Colors.transparent,
        title: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            const Text(
              'Class Room',
              style: TextStyle(
                fontSize: 20,
                fontWeight: FontWeight.bold,
                color: headingColor,
              ),
            ),
            Text(
              "You're $_displayName · $status",
              style: const TextStyle(fontSize: 12, color: secondaryColor),
            ),
          ],
        ),
        actions: [
          IconButton(
            tooltip: _manualOffline || !_connected
                ? 'Reconnect'
                : 'Go offline — test',
            onPressed: _connecting ? null : _toggleTestConnection,
            icon: Icon(
              _manualOffline || !_connected ? Icons.wifi_off : Icons.wifi,
            ),
          ),
          IconButton(
            tooltip: 'About this room',
            onPressed: _showInfo,
            icon: const Icon(Icons.info_outline),
          ),
        ],
      ),
      body: SafeArea(
        child: Column(
          children: [
            _noticeWidget(),
            Expanded(
              child: _messages.isEmpty
                  ? Center(
                      child: Padding(
                        padding: const EdgeInsets.all(24),
                        child: Column(
                          mainAxisSize: MainAxisSize.min,
                          children: [
                            const Icon(
                              Icons.chat_bubble_outline,
                              size: 42,
                              color: primaryColor,
                            ),
                            const SizedBox(height: 14),
                            Text(
                              _connected && !_clockReady
                                  ? 'Checking message expiry…'
                                  : 'No messages yet',
                              textAlign: TextAlign.center,
                              style: const TextStyle(
                                fontSize: 20,
                                fontWeight: FontWeight.w600,
                                color: headingColor,
                              ),
                            ),
                            const SizedBox(height: 8),
                            const Text(
                              'Messages appear here while they are live.',
                              textAlign: TextAlign.center,
                              style: TextStyle(color: secondaryColor),
                            ),
                          ],
                        ),
                      ),
                    )
                  : ListView.builder(
                      reverse: true,
                      padding: const EdgeInsets.all(16),
                      itemCount: _messages.length,
                      itemBuilder: (context, index) => _messageBubble(
                        _messages[_messages.length - 1 - index],
                      ),
                    ),
            ),
            _pendingSection(),
            Container(
              padding: const EdgeInsets.all(12),
              color: Colors.white,
              child: Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  if (queueFull)
                    const Padding(
                      padding: EdgeInsets.only(bottom: 8),
                      child: Text(
                        'Queue full: cancel a waiting message '
                        'or wait for delivery.',
                        style: TextStyle(
                          color: Color(0xFF945800),
                          fontSize: 13,
                        ),
                      ),
                    ),
                  Row(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      Expanded(
                        child: TextField(
                          controller: _draft,
                          readOnly: _saving,
                          minLines: 1,
                          maxLines: 4,
                          maxLength: 500,
                          onChanged: (_) => _redraw(),
                          decoration: InputDecoration(
                            hintText: _canDrain
                                ? 'Message the class…'
                                : 'Queue a message…',
                            counterText: _draft.text.isEmpty
                                ? ''
                                : '${_draft.text.runes.length}/500',
                          ),
                        ),
                      ),
                      const SizedBox(width: 8),
                      Padding(
                        padding: const EdgeInsets.only(top: 4),
                        child: IconButton.filled(
                          tooltip: _canDrain
                              ? 'Send message'
                              : 'Save to offline queue',
                          constraints: const BoxConstraints(
                            minHeight: 48,
                            minWidth: 48,
                          ),
                          onPressed:
                              _saving || queueFull || _draft.text.trim().isEmpty
                              ? null
                              : _queueMessage,
                          icon: Icon(
                            _saving ? Icons.hourglass_top : Icons.arrow_upward,
                          ),
                        ),
                      ),
                    ],
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    Widget screen;

    if (!_ready) {
      screen = Scaffold(
        body: Center(
          child: Padding(
            padding: const EdgeInsets.all(24),
            child: _loadingFailed
                ? Text(_notice, textAlign: TextAlign.center)
                : const Column(
                    mainAxisSize: MainAxisSize.min,
                    children: [
                      CircularProgressIndicator(),
                      SizedBox(height: 16),
                      Text('Opening local storage…'),
                    ],
                  ),
          ),
        ),
      );
    } else {
      screen = _joined ? _chatScreen() : _joinScreen();
    }

    return ColoredBox(
      color: const Color(0xFFE8ECF4),
      child: Center(
        child: ConstrainedBox(
          constraints: const BoxConstraints(maxWidth: 480),
          child: screen,
        ),
      ),
    );
  }
}
