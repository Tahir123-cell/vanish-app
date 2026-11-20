import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:vanish_app/local_store.dart';
import 'package:vanish_app/main.dart';

class _TestStore extends Fake implements LocalStore {
  @override
  Future<void> open() async {}

  @override
  Future<Map<String, Object?>> loadProfile() async => {
    'device_id': 'test-device',
    'display_name': '',
    'has_joined': 0,
  };

  @override
  Future<int> discardUncertain() async => 0;

  @override
  Future<List<Map<String, Object?>>> loadOutbox() async => [];

  @override
  Future<void> close() async {}
}

void main() {
  testWidgets('Join rejects a name containing only spaces', (tester) async {
    await tester.pumpWidget(
      MaterialApp(home: VanishHome(store: _TestStore())),
    );

    try {
      await tester.pump();

      final field = find.byType(TextFormField);
      final join = find.widgetWithText(FilledButton, 'Join class room');

      expect(field, findsOneWidget);
      await tester.enterText(field, '   ');
      await tester.ensureVisible(join);
      await tester.tap(join);
      await tester.pump();

      expect(
        find.text('Enter a name between 1 and 30 characters.'),
        findsOneWidget,
      );
    } finally {
      await tester.pumpWidget(const SizedBox.shrink());
      await tester.pump();
    }
  });
}