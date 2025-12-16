import 'dart:async';
import 'dart:isolate';

import 'package:dive_computer/framework/dive_computer_ffi.dart';
import 'package:dive_computer/types/computer.dart';

enum DiveComputerMethod {
  supportedComputers,
  download,
  bufferData,
}

typedef IsolateMessage = (int id, DiveComputerMethod method, List<Object?> args);
typedef IsolateResponse = (int id, Object? response);

class DiveComputerWorker {
  final SendPort _commands;
  final ReceivePort _responses;
  final Map<int, Completer<Object?>> _activeRequests = {};
  int _idCounter = 0;
  bool _closed = false;

  Future<List<Computer>> get supportedComputers async {
    return _exec(DiveComputerMethod.supportedComputers, []);
  }

  Future download(
    Computer computer,
    ComputerTransport transport, {
    String? lastFingerprint,
  }) async {
    return await _exec(DiveComputerMethod.download, [computer, transport, lastFingerprint]);
  }

  Future<T> _exec<T>(DiveComputerMethod method, List<Object?> args) async {
    if (_closed) throw StateError('Closed');
    final completer = Completer<T>.sync();
    final id = _idCounter++;
    _activeRequests[id] = completer;
    _commands.send((id, method, args));
    return await completer.future;
  }

  static Future<DiveComputerWorker> spawn() async {
    // Create a receive port and add its initial message handler.
    final initPort = RawReceivePort();
    final connection = Completer<(ReceivePort, SendPort)>.sync();
    initPort.handler = (initialMessage) {
      final commandPort = initialMessage as SendPort;
      connection.complete((
        ReceivePort.fromRawReceivePort(initPort),
        commandPort,
      ));
    };

    // Spawn the isolate.
    try {
      await Isolate.spawn(_startRemoteIsolate, (initPort.sendPort));
    } on Object {
      initPort.close();
      rethrow;
    }
    final (ReceivePort receivePort, SendPort sendPort) = await connection.future;
    return DiveComputerWorker._(receivePort, sendPort);
  }

  DiveComputerWorker._(this._responses, this._commands) {
    _responses.listen(_handleResponsesFromIsolate);
  }

  void _handleResponsesFromIsolate(dynamic message) {
    final (int id, Object? response) = message as IsolateResponse;
    final completer = _activeRequests.remove(id)!;
    if (response is RemoteError) {
      completer.completeError(response);
    } else {
      completer.complete(response);
    }
    if (_closed && _activeRequests.isEmpty) _responses.close();
  }

  static void _handleCommandsToIsolate(
    ReceivePort receivePort,
    SendPort sendPort,
  ) {
    receivePort.listen((message) {
      if (message == 'shutdown') {
        receivePort.close();
        return;
      }
      final (int id, DiveComputerMethod method, List<Object?> args) = message as IsolateMessage;
      try {
        switch (method) {
          case DiveComputerMethod.supportedComputers:
            final computers = DiveComputerFfi.supportedComputers;
            sendPort.send((id, computers));
          case DiveComputerMethod.download:
            final computer = args[0] as Computer;
            final transport = args[1] as ComputerTransport;
            final lastFingerprint = args[2] as String?;
            DiveComputerFfi.download(computer, transport, lastFingerprint: lastFingerprint); // long running
            sendPort.send((id, null));
          case DiveComputerMethod.bufferData:
            break;
        }
      } catch (e) {
        sendPort.send((id, RemoteError(e.toString(), '')));
      }
    });
  }

  static void _startRemoteIsolate(SendPort sendPort) {
    final receivePort = ReceivePort();
    sendPort.send(receivePort.sendPort);

    DiveComputerFfi.initialize();
    DiveComputerFfi.openConnection();

    _handleCommandsToIsolate(receivePort, sendPort);
  }

  void close() {
    if (!_closed) {
      _closed = true;
      _commands.send('shutdown');
      if (_activeRequests.isEmpty) _responses.close();
    }
  }
}
