import 'package:flutter/foundation.dart';
import 'package:sqflite/sqflite.dart';
import 'package:sqflite_common_ffi_web/sqflite_ffi_web.dart';
import 'package:uuid/uuid.dart';

class LocalStore {
  Database? _database;

  Database get db {
    final database = _database;
    if (database == null) {
      throw StateError('Database has not been opened.');
    }
    return database;
  }

  Future<void> open() async {
    if (_database != null) return;

    // Chrome needs the web database implementation.
    // Android uses the normal SQLite implementation.
    if (kIsWeb) {
      databaseFactory = databaseFactoryFfiWeb;
    }

    final folder = await getDatabasesPath();
    final path = kIsWeb ? 'vanish.db' : '$folder/vanish.db';

    _database = await openDatabase(
      path,
      version: 2,
      onUpgrade: (database, oldVersion, newVersion) async {
        // Pending messages in v1 of this database were addressed to the old
        // local relay. They must never be forwarded to the shared class room.
        if (oldVersion < 2) {
          await database.delete('outbox');
          await database.execute('CREATE TABLE IF NOT EXISTS own_ids (id TEXT PRIMARY KEY)');
        }
      },
      onConfigure: (database) async {
        await database.execute('PRAGMA secure_delete = ON');

        // Keep Android storage inspection simple.
        // Browser storage is managed by the web implementation.
        if (!kIsWeb) {
          await database.rawQuery('PRAGMA journal_mode = DELETE');
        }
      },
      onCreate: (database, version) async {
        await database.execute('''
          CREATE TABLE profile (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            device_id TEXT NOT NULL,
            display_name TEXT NOT NULL,
            has_joined INTEGER NOT NULL DEFAULT 0
          )
        ''');

        await database.execute('''
          CREATE TABLE outbox (
            id TEXT PRIMARY KEY,
            text TEXT NOT NULL,
            state TEXT NOT NULL,
            attempted_epoch TEXT,
            created_at_local INTEGER NOT NULL
          )
        ''');

        await database.execute('''
          CREATE TABLE messages (
            id TEXT PRIMARY KEY,
            sender_id TEXT NOT NULL,
            sender_name TEXT NOT NULL,
            text TEXT NOT NULL,
            sent_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            relay_epoch TEXT NOT NULL
          )
        ''');

        await database.execute('CREATE TABLE own_ids (id TEXT PRIMARY KEY)');
        // This table never stores message text.
        await database.execute('''
          CREATE TABLE seen_ids (
            id TEXT PRIMARY KEY,
            relay_epoch TEXT NOT NULL,
            expires_at INTEGER NOT NULL
          )
        ''');
      },
    );

    await db.insert(
      'profile',
      {
        'id': 1,
        'device_id': const Uuid().v4(),
        'display_name': '',
        'has_joined': 0,
      },
      conflictAlgorithm: ConflictAlgorithm.ignore,
    );

    // Conservative cold-start rule:
    // do not restore previously accepted message bodies.
    // Waiting rows retain a 30-second TTL; uncertain attempts are discarded by the client.
    await db.delete('messages');
  }

  Future<Map<String, Object?>> loadProfile() async {
    final rows = await db.query(
      'profile',
      where: 'id = ?',
      whereArgs: [1],
    );

    return rows.single;
  }

  Future<void> saveName(String name) async {
    await db.update(
      'profile',
      {'display_name': name.trim()},
      where: 'id = ?',
      whereArgs: [1],
    );
  }

  Future<void> markJoined() async {
    await db.update(
      'profile',
      {'has_joined': 1},
      where: 'id = ?',
      whereArgs: [1],
    );
  }

  Future<void> enqueue({
    required String id,
    required String text,
    required int createdAt,
  }) async {
    final trimmed = text.trim();

    if (trimmed.isEmpty || text.runes.length > 500) {
      throw ArgumentError('Message must be 1–500 characters.');
    }

    await db.transaction((transaction) async {
      final count = Sqflite.firstIntValue(
            await transaction.rawQuery(
              'SELECT COUNT(*) FROM outbox',
            ),
          ) ??
          0;

      if (count >= 50) {
        throw StateError('queue_full');
      }

      await transaction.insert('own_ids', {'id': id},
        conflictAlgorithm: ConflictAlgorithm.ignore);
      await transaction.insert('outbox', {
        'id': id,
        'text': trimmed,
        'state': 'waiting',
        'attempted_epoch': null,
        'created_at_local': createdAt,
      });
    });
  }

  Future<List<Map<String, Object?>>> loadOutbox() {
    return db.query(
      'outbox',
      orderBy: 'created_at_local ASC, rowid ASC',
    );
  }

  Future<bool> markInFlight(String id, String epoch) async {
    final changed = await db.update(
      'outbox',
      {
        'state': 'in_flight',
        'attempted_epoch': epoch,
      },
      where: 'id = ? AND state = ?',
      whereArgs: [id, 'waiting'],
    );

    return changed == 1;
  }

  Future<void> markWaiting(String id) async {
    await db.update(
      'outbox',
      {
        'state': 'waiting',
        'attempted_epoch': null,
      },
      where: 'id = ?',
      whereArgs: [id],
    );
  }

  Future<bool> cancelWaiting(String id) async {
    // An in-flight message cannot be cancelled safely.
    final deleted = await db.delete(
      'outbox',
      where: 'id = ? AND state = ?',
      whereArgs: [id, 'waiting'],
    );

    return deleted == 1;
  }

  Future<void> discardOutboxItem(String id) async {
    await db.delete(
      'outbox',
      where: 'id = ?',
      whereArgs: [id],
    );
  }

  Future<bool> hasSeen(String id) async {
    final rows = await db.query(
      'seen_ids',
      columns: ['id'],
      where: 'id = ?',
      whereArgs: [id],
      limit: 1,
    );

    return rows.isNotEmpty;
  }

  Future<void> saveAcceptedMessage({
    required String id,
    required String senderId,
    required String senderName,
    required String text,
    required int sentAt,
    required int expiresAt,
    required String epoch,
  }) async {
    await db.transaction((transaction) async {
      await transaction.insert(
        'messages',
        {
          'id': id,
          'sender_id': senderId,
          'sender_name': senderName,
          'text': text,
          'sent_at': sentAt,
          'expires_at': expiresAt,
          'relay_epoch': epoch,
        },
        conflictAlgorithm: ConflictAlgorithm.ignore,
      );

      await transaction.insert(
        'seen_ids',
        {
          'id': id,
          'relay_epoch': epoch,
          'expires_at': expiresAt,
        },
        conflictAlgorithm: ConflictAlgorithm.ignore,
      );

      // Acceptance removes the queued copy in the same transaction.
      await transaction.delete(
        'outbox',
        where: 'id = ?',
        whereArgs: [id],
      );
    });
  }

  Future<void> removeExpiredMessage(String id) async {
    await db.transaction((transaction) async {
      await transaction.delete(
        'messages',
        where: 'id = ?',
        whereArgs: [id],
      );

      await transaction.delete(
        'outbox',
        where: 'id = ?',
        whereArgs: [id],
      );
    });

    // Keep seen_ids metadata to prevent duplicate display.
  }

  Future<void> clearAcceptedMessages() async {
    await db.delete('messages');
  }

  Future<int> discardUncertain() => db.delete('outbox',
    where: 'state = ?', whereArgs: ['in_flight']);

  Future<bool> isOwnId(String id) async {
    final rows = await db.query('own_ids', columns: ['id'],
      where: 'id = ?', whereArgs: [id], limit: 1);
    return rows.isNotEmpty;
  }

  Future<void> rememberId(String id, String scope, int expiresAt) async {
    await db.insert('seen_ids', {'id': id, 'relay_epoch': scope,
      'expires_at': expiresAt}, conflictAlgorithm: ConflictAlgorithm.ignore);
  }

  Future<void> close() async {
    await _database?.close();
    _database = null;
  }
}