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

// Chrome and relay must currently run on the same laptop.
const relayAddress = 'ws://10.30.28.169:8081';

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
  final Set<String> recipients = {};

  LiveMessage({
    required this.id,
    required this.senderId,
    required this.senderName,
    required this.text,
    required this.deadline,
  });
}

class VanishHome extends StatefulWidget {
  const VanishHome({super.key});

  @override
  State<VanishHome> createState() => _VanishHomeState();
}

class _VanishHomeState extends State<VanishHome> with WidgetsBindingObserver {
  final _store = LocalStore();
  final _uuid = const Uuid();
  final _elapsed = Stopwatch()..start();

  final _name = TextEditingController();
  final _draft = TextEditingController();
  final _formKey = GlobalKey<FormState>();

  final _messages = <LiveMessage>[];
  final _pings = <String, int>{};

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
  String? _epoch;
  String? _flightId;

  int _generation = 0;
  int _serverSample = 0;
  int _sampleTick = 0;
  int _bestRtt = 1 << 30;
  int _lastPong = 0;
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
      final pending = await _store.loadOutbox();

      if (!mounted) return;

      _deviceId = profile['device_id'] as String;
      _displayName = profile['display_name'] as String;
      _name.text = _displayName;
      _hasJoinedBefore = profile['has_joined'] == 1;
      _outbox = pending;
      _ready = true;

      _ticker = Timer.periodic(const Duration(milliseconds: 250), (_) {
        if (!_foreground || _messages.isEmpty) return;

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
      _bestRtt = 1 << 30;
      _pings.clear();

      if (_connected) {
        _sendPing();
        _sendPing();
        _sendPing();
      } else if (_joined && !_manualOffline && !_connecting) {
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
    if (_disposed || _connecting || _manualOffline) return;

    _retry?.cancel();

    final generation = ++_generation;

    _connecting = true;
    _connected = false;
    _clockReady = false;
    _bestRtt = 1 << 30;
    _pings.clear();
    _notice = '';
    _redraw();

    _connectTimeout?.cancel();
    _connectTimeout = Timer(
      const Duration(seconds: 10),
      () => _lost(generation, 'Connection timed out.'),
    );

    try {
      await _subscription?.cancel();
      await _channel?.sink.close();

      if (_disposed || generation != _generation) return;

      final channel = WebSocketChannel.connect(Uri.parse(relayAddress));

      _channel = channel;
      await channel.ready;

      if (_disposed || generation != _generation) {
        await channel.sink.close();
        return;
      }

      _subscription = channel.stream.listen(
        (raw) {
          _background(() async {
            if (generation == _generation) {
              await _handle(raw);
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

      _write({
        'type': 'join',
        'version': 'dev-v1',
        'device_id': _deviceId,
        'display_name': _displayName,
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
    _flightTimeout?.cancel();
    _subscription?.cancel();
    _channel?.sink.close();
    _channel = null;

    _connected = false;
    _connecting = false;
    _clockReady = false;
    _flightId = null;
    _messages.clear();
    _pings.clear();
    _notice = reason;
    _redraw();

    if (_ready) {
      _background(() => _store.clearAcceptedMessages());
    }

    if (retry && _joined && !_manualOffline) {
      _retry?.cancel();

      final delay = _retrySeconds;
      _retrySeconds = (_retrySeconds * 2).clamp(1, 10).toInt();

      _retry = Timer(Duration(seconds: delay), () {
        if (!_disposed && !_manualOffline) _connect();
      });
    }
  }

  void _sendPing() {
    if (!_connected) return;

    final nonce = _uuid.v4();
    _pings.removeWhere((_, value) => _tick - value > 30000);
    _pings[nonce] = _tick;

    try {
      _write({'type': 'ping', 'nonce': nonce});
    } catch (_) {
      _lost(_generation, 'Connection lost.');
    }
  }

  Future<void> _handle(dynamic raw) async {
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
        if (event['version'] != 'dev-v1' || event['relay_epoch'] is! String) {
          _lost(
            _generation,
            'Protocol mismatch. Expected dev-v1.',
            retry: false,
          );
          return;
        }

        _epoch = event['relay_epoch'] as String;
        _connected = true;
        _lastPong = _tick;

        await _store.removeOldSeenIds(_epoch!);

        _sendPing();
        _sendPing();
        _sendPing();

        _heartbeat?.cancel();
        _heartbeat = Timer.periodic(const Duration(seconds: 10), (_) {
          if (_tick - _lastPong > 25000) {
            _lost(_generation, 'Connection timed out.');
          } else {
            _sendPing();
          }
        });
        break;

      case 'pong':
        final sentTick = _pings.remove(event['nonce']);
        final serverTime = event['server_now'];

        if (sentTick == null || serverTime is! int) return;

        final received = _tick;
        final rtt = received - sentTick;
        _lastPong = received;

        if (!_clockReady || rtt <= _bestRtt) {
          _bestRtt = rtt;
          _serverSample = serverTime + rtt ~/ 2;
          _sampleTick = received;
        }

        final becameClockReady = !_clockReady && _foreground;
        _clockReady = _foreground;
        if (becameClockReady && _flightId != null) {
          // An acceptance may have arrived during clock synchronisation.
          // Reconcile its existing ID instead of deleting or resending it.
          _flightTimeout?.cancel();
          _flightId = null;
        }
        _connecting = false;
        _joined = true;
        _retrySeconds = 1;
        _connectTimeout?.cancel();

        if (!_hasJoinedBefore) {
          await _store.markJoined();
          _hasJoinedBefore = true;
        }

        await _advanceQueue();
        break;

      case 'accepted':
        await _handleAccepted(event);
        break;

      case 'message':
        await _handleMessage(event);
        break;

      case 'status_result':
        await _handleStatus(event);
        break;

      case 'receipt':
        final id = event['id'];
        final recipient = event['recipient_device_id'];

        if (id is String && recipient is String) {
          for (final message in _messages) {
            if (message.id == id && message.senderId == _deviceId) {
              message.recipients.add(recipient);
            }
          }
        }
        break;

      case 'expired':
        final id = event['id'];

        if (id is String) {
          _messages.removeWhere((message) => message.id == id);
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

        // Stop automatic retries for protocol/server rejections.
        // Preserve in-flight data until its outcome can be checked.
        _manualOffline = _joined;
        _lost(
          _generation,
          '$message Check the relay before reconnecting.',
          retry: false,
        );

        if (code == 'unsupported_version') {
          _notice = 'Protocol mismatch. Expected dev-v1.';
        }
        break;
    }

    _redraw();
  }

  Future<void> _reloadOutbox() async {
    _outbox = await _store.loadOutbox();
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

    _flightTimeout = Timer(const Duration(seconds: 12), () {
      if (_flightId == id && generation == _generation) {
        _lost(
          generation,
          'Confirmation timed out. Delivery will be checked '
          'before retrying.',
        );
      }
    });
  }

  // Starts at most one send/status operation at a time.
  Future<void> _advanceQueue() async {
    if (!_canDrain || _flightId != null || _epoch == null) return;

    final generation = _generation;
    await _reloadOutbox();

    while (_outbox.isNotEmpty && _canDrain && generation == _generation) {
      final item = _outbox.first;
      final id = item['id'] as String;

      if (item['state'] == 'in_flight') {
        if (item['attempted_epoch'] != _epoch) {
          // An in-memory relay restart loses acceptance history.
          // Do not blindly resend an uncertain message.
          await _store.discardOutboxItem(id);
          _notice = 'Delivery outcome unknown; not resent.';
          await _reloadOutbox();
          continue;
        }

        _flightId = id;

        try {
          _write({'type': 'status', 'id': id});
          _watchFlight(id);
        } catch (_) {
          _lost(_generation, 'Connection lost.');
        }

        return;
      }

      final claimed = await _store.markInFlight(id, _epoch!);
      await _reloadOutbox();

      if (!claimed) continue;

      if (!_canDrain || generation != _generation) {
        // It was marked in-flight but not transmitted.
        // Restore Waiting so reconnect can handle it safely.
        await _store.markWaiting(id);
        await _reloadOutbox();
        return;
      }

      _flightId = id;

      try {
        _write({'type': 'send', 'id': id, 'text': item['text']});
        _watchFlight(id);
      } catch (_) {
        _lost(_generation, 'Connection lost.');
      }

      return;
    }
  }

  bool _validTimes(Map<String, dynamic> event) {
    return event['sent_at'] is int &&
        event['expires_at'] is int &&
        (event['expires_at'] as int) - (event['sent_at'] as int) == 30000;
  }

  Future<void> _handleAccepted(Map<String, dynamic> event) async {
    final id = event['id'];

    if (id is! String ||
        event['relay_epoch'] != _epoch ||
        !_validTimes(event)) {
      return;
    }

    // Unknown time is NOT proof of expiry. Keep the durable in-flight
    // body until a pong lets us ask status for the same message ID.
    if (!_foreground || !_clockReady) {
      if (_foreground) {
        _notice = 'Checking message time before displaying your send…';
        _sendPing();
      }
      return;
    }

    await _reloadOutbox();

    final matching = _outbox.where((row) => row['id'] == id);

    if (matching.isNotEmpty) {
      final pending = matching.first;

      await _recordMessage(
        id: id,
        senderId: _deviceId,
        senderName: _displayName,
        text: pending['text'] as String,
        sentAt: event['sent_at'] as int,
        expiresAt: event['expires_at'] as int,
      );
    }

    // A lifecycle change can happen during a database await.
    if (!_foreground || !_clockReady) return;

    _finishFlight(id);
    await _reloadOutbox();
    _redraw();
    await _advanceQueue();
  }

  Future<void> _handleStatus(Map<String, dynamic> event) async {
    final id = event['id'];

    if (id is! String || id != _flightId || event['relay_epoch'] != _epoch) {
      return;
    }

    switch (event['result']) {
      case 'accepted':
        await _handleAccepted(event);
        return;

      case 'expired':
        await _store.removeExpiredMessage(id);
        _messages.removeWhere((message) => message.id == id);
        break;

      case 'unknown':
        // Same relay epoch confirms it has never accepted this ID.
        // Retry using the SAME message ID.
        await _store.markWaiting(id);
        break;

      default:
        return;
    }

    _finishFlight(id);
    await _reloadOutbox();
    await _advanceQueue();
  }

  Future<void> _handleMessage(Map<String, dynamic> event) async {
    final sender = event['sender'];

    if (event['id'] is! String ||
        event['text'] is! String ||
        sender is! Map ||
        sender['device_id'] is! String ||
        sender['display_name'] is! String ||
        event['relay_epoch'] != _epoch ||
        !_validTimes(event)) {
      return;
    }

    final text = event['text'] as String;
    if (text.isEmpty || text.length > 500) return;

    final id = event['id'] as String;
    final senderId = sender['device_id'] as String;

    final displayed = await _recordMessage(
      id: id,
      senderId: senderId,
      senderName: sender['display_name'] as String,
      text: text,
      sentAt: event['sent_at'] as int,
      expiresAt: event['expires_at'] as int,
    );

    if (displayed && senderId != _deviceId && _connected) {
      _write({'type': 'delivered', 'id': id});
    }
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

    final epoch = _epoch!;
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

    if (_saving || text.isEmpty || text.length > 500) return;

    _saving = true;
    _redraw();

    try {
      await _serial(() async {
        await _store.enqueue(id: _uuid.v4(), text: text);
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
                '• Messages expire 30 seconds after the test relay '
                'accepts them.\n\n'
                '• Offline messages wait in local storage. '
                'Their countdown has not started.\n\n'
                '• Screenshots and copies are possible.\n\n'
                '• This prototype clears accepted messages when hidden '
                'or restarted. Unsent messages remain queued.\n\n'
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
                      'Messages expire 30 seconds after the test relay '
                      'accepts them.\n\n'
                      'Screenshots and copies are still possible.',
                      style: TextStyle(color: headingColor, height: 1.5),
                    ),
                  ),
                  const SizedBox(height: 24),
                  TextFormField(
                    controller: _name,
                    enabled: !_connecting && !_saving,
                    maxLength: 24,
                    textInputAction: TextInputAction.go,
                    decoration: const InputDecoration(
                      labelText: 'Display name',
                      hintText: 'e.g. Ali',
                    ),
                    validator: (value) {
                      final name = value?.trim() ?? '';
                      if (name.isEmpty || name.length > 24) {
                        return 'Enter a name between 1 and 24 characters.';
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
                      'Local development relay · dev-v1',
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
                '${own ? (message.recipients.isEmpty ? 'Sent · ' : 'Delivered to ${message.recipients.length} devices · ') : ''}'
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
                                    ? 'Waiting — timer has not started'
                                    : 'Checking delivery…',
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
                                : '${_draft.text.length}/500',
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
