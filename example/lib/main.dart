import 'package:flutter/material.dart';
import 'package:dive_computer/dive_computer.dart';

void main() {
  runApp(const MyApp());
}

class MyApp extends StatefulWidget {
  const MyApp({super.key});

  @override
  State<MyApp> createState() => _MyAppState();
}

class _MyAppState extends State<MyApp> {
  List<Computer> supportedComputers = [];
  DiveComputerWorker? dc = null;

  @override
  void initState() {
    super.initState();
    setSupportedComputers();
  }

  Future setSupportedComputers() async {
    dc = await DiveComputerWorker.spawn();
    final comps = await dc!.supportedComputers;
    setState(() {
      supportedComputers = comps;
    });
  }

  @override
  void dispose() {
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      home: Scaffold(
        appBar: AppBar(
          title: const Text('libdivecomputer ffi example'),
        ),
        body: Container(
          padding: const EdgeInsets.all(10),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              const Text('Supported dive computers:',
                  style: TextStyle(
                    fontWeight: FontWeight.bold,
                  )),
              const SizedBox(height: 10),
              Expanded(
                child: ListView.builder(
                  itemCount: supportedComputers.length,
                  itemBuilder: (context, index) {
                    final computer = supportedComputers[index];
                    return GestureDetector(
                      onTap: () async {
                        final dives = await dc!.download(
                          computer,
                          computer.transports.first,
                          lastFingerprint: "exampleFingerprint",
                        );
                        // ignore: use_build_context_synchronously
                        ScaffoldMessenger.of(context).showSnackBar(
                          SnackBar(
                            content: Text('Downloaded ${dives.length} dives'),
                          ),
                        );
                      },
                      child: Text(computer.toString()),
                    );
                  },
                ),
              ),
            ],
          ),
        ),
      ),
    );
  }
}
