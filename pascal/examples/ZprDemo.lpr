program ZprDemo;

// Every capability zpr has, in the order you would meet them.
//
// ══ WHAT THIS IS FOR ════════════════════════════════════════════════════════
//
// Two things at once: something to run to see the library work, and the answer
// to "how do I use the new streaming/server/transcoder calls", which have no
// high-level wrapper classes yet and are reached through the raw zpr_* entry
// points below.
//
// ⚠ IT DEGRADES RATHER THAN FAILING. Every network section is guarded: with no
// daemon on the other end each one says so and the program continues. The JSON
// and protobuf sections need nothing but the library and always run, so a bad
// build is distinguishable from a missing daemon.
//
// Usage:
//   ZprDemo [grpcEndpoint] [descriptorSet.binpb]
//
//   ZprDemo                                          JSON + protobuf only
//   ZprDemo http://127.0.0.1:9491 descriptors.binpb  everything
//
// Build (FPC):
//   fpc -Fu../ -o./ZprDemo ZprDemo.lpr
// with libzpr.dylib/.so beside the executable.

{$MODE OBJFPC}{$H+}

uses
  SysUtils, Zpr;

var
  GEndpoint: UTF8String = '';
  GDescriptors: string = '';

procedure Head(const ATitle: string);
begin
  WriteLn;
  WriteLn('== ', ATitle, ' ', StringOfChar('=', 60 - Length(ATitle)));
end;

// ── 1. JSON ────────────────────────────────────────────────────────────────
// A DOM, not a serializer: build a tree, hand it over, read one back.
procedure DemoJson;
var
  Root, Legs, Leg: TZprJson;
begin
  Head('JSON');
  Root := TZprJson.NewObject;
  try
    Root.SetField('symbol', TZprJson.NewString('NIFTY'));
    Root.SetField('lots', TZprJson.NewFloat(2));
    Legs := TZprJson.NewArray;
    Leg := TZprJson.NewObject;
    Leg.SetField('strike', TZprJson.NewFloat(24000));
    Leg.SetField('call', TZprJson.NewBool(True));
    // ⚠ OWNERSHIP TRANSFERS ON Push/SetField. The child belongs to the parent
    // afterwards; freeing it yourself is a double free.
    Legs.Push(Leg);
    Root.SetField('legs', Legs);
    WriteLn('  built : ', Root.ToJson(False));
  finally
    Root.Free;
  end;

  Root := TZprJson.Parse('{"ok":true,"filled":75,"avg":128.4}');
  try
    WriteLn('  parsed: filled=', Root['filled'].AsFloat:0:0,
            '  avg=', Root['avg'].AsFloat:0:2,
            '  ok=', Root['ok'].AsBoolean);
  finally
    Root.Free;
  end;
end;

// ── 2. HTTP ────────────────────────────────────────────────────────────────
procedure DemoHttp;
var
  Status: Word;
  Headers: UTF8String;
  Body: TBytes;
begin
  Head('HTTP client');
  try
    Body := TZprHttpClient.Request('GET', 'https://example.com', '', nil, 8000, Status, Headers);
    WriteLn('  GET example.com -> ', Status, ', ', Length(Body), ' bytes');
  except
    on E: Exception do
      WriteLn('  skipped (no network): ', E.Message);
  end;
end;

// ── 3. protobuf ⇄ JSON, from a descriptor set ──────────────────────────────
// No generated code: the FileDescriptorSet is read at runtime and any message
// in it can be converted by name.
function DemoProtobuf: TZprProtobufPool;
begin
  Result := nil;
  Head('protobuf <-> JSON');
  if GDescriptors = '' then
  begin
    WriteLn('  skipped (no descriptor set given)');
    Exit;
  end;
  Result := TZprProtobufPool.LoadFromFile(GDescriptors);
  WriteLn('  loaded ', GDescriptors);
  // Round-tripping an empty message proves the pool resolved the TYPE, which is
  // the part that fails when a descriptor set is stale.
  WriteLn('  Empty {} -> ', Length(Result.JsonToBinary('sapphire.v1.Empty', '{}')), ' bytes');
end;

// ── 4. a unary gRPC call ───────────────────────────────────────────────────
procedure DemoUnary(APool: TZprProtobufPool);
var
  Req, Reply: TBytes;
  Status: Integer;
begin
  Head('gRPC unary');
  if (GEndpoint = '') or (APool = nil) then
  begin
    WriteLn('  skipped (needs an endpoint and a descriptor set)');
    Exit;
  end;
  try
    Req := APool.JsonToBinary('sapphire.v1.Empty', '{}');
    Reply := TZprGrpcClient.Call(GEndpoint, '/sapphire.v1.HealthService/Ping', Req, 5000, Status);
    if Status = 0 then
      WriteLn('  Ping -> ', APool.BinaryToJson('sapphire.v1.Pong', Reply))
    else
      WriteLn('  Ping refused, grpc-status=', Status, ' ', LastError);
  except
    on E: Exception do WriteLn('  skipped: ', E.Message);
  end;
end;

// ── 5. server-streaming, buffered, polled ──────────────────────────────────
// THE SHAPE A GUI SHOULD USE. `Buffer` puts a ring behind the stream so reads
// stop blocking; a TTimer calling ReadInto is then a complete feed loop, with
// no worker thread and nothing entering the host from a foreign thread.
procedure DemoStreaming(APool: TZprProtobufPool);
var
  Stream: PGrpcClientStream;
  Req: TBytes;
  Status: Integer;
  Buf: array[0..65535] of Byte;
  Got: NativeUInt;
  Rc, Reads, Spins: Integer;
  Depth: NativeUInt;
  Dropped: UInt64;
begin
  Head('gRPC server-streaming (ring-buffered, non-blocking)');
  if (GEndpoint = '') or (APool = nil) then
  begin
    WriteLn('  skipped (needs an endpoint and a descriptor set)');
    Exit;
  end;
  Req := APool.JsonToBinary('sapphire.v1.Empty', '{}');
  if zpr_grpc_client_stream_open(PAnsiChar(GEndpoint),
       '/sapphire.v1.MarketDataService/StreamSnapshots', nil,
       PByte(Req), Length(Req), 5000, Stream, Status) <> 0 then
  begin
    WriteLn('  could not open: ', LastError);
    Exit;
  end;
  try
    // 256 messages of slack. Full means the OLDEST is discarded — right for
    // marks, wrong for anything whose successor does not carry what it said.
    zpr_grpc_client_stream_buffer(Stream, 256);
    Reads := 0;
    for Spins := 1 to 40 do
    begin
      Rc := zpr_grpc_client_stream_read_into(Stream, @Buf[0], Length(Buf), Got);
      if Rc = 1 then
      begin
        Inc(Reads);
        if Reads = 1 then
          WriteLn('  first message: ', Got, ' bytes');
      end
      else if Rc < 0 then
      begin
        WriteLn('  stream error: ', LastError);
        Break;
      end;
      Sleep(50); // a TTimer interval, in a console program
    end;
    zpr_grpc_client_stream_stats(Stream, Depth, Dropped);
    WriteLn('  read ', Reads, ' message(s); ring depth ', Depth, ', dropped ', Dropped);
    if Dropped > 0 then
      WriteLn('  ^ the host fell behind. This number is the only sign of it.');
  finally
    zpr_grpc_client_stream_cancel(Stream);
  end;
end;

// ── 6. bidirectional streaming ─────────────────────────────────────────────
// Client-streaming is this with one read at the end.
procedure DemoBidi;
var
  Bidi: PGrpcBidiStream;
  Status: Integer;
  Payload: TBytes;
begin
  Head('gRPC bidirectional');
  if GEndpoint = '' then
  begin
    WriteLn('  skipped (no endpoint)');
    Exit;
  end;
  if zpr_grpc_bidi_open(PAnsiChar(GEndpoint), '/sapphire.v1.HealthService/Ping',
       nil, 8, 5000, Bidi, Status) <> 0 then
  begin
    WriteLn('  could not open (expected — Ping is not bidirectional): grpc-status=', Status);
    Exit;
  end;
  try
    SetLength(Payload, 0);
    zpr_grpc_bidi_send(Bidi, PByte(Payload), 0);
    // ⚠ WITHOUT THIS A CLIENT-STREAMING SERVER NEVER ANSWERS: it is waiting for
    // the end of the request stream to compute its single reply.
    zpr_grpc_bidi_close_send(Bidi);
    WriteLn('  sent one message and half-closed');
  finally
    zpr_grpc_bidi_cancel(Bidi);
  end;
end;

// ── 7. a gRPC SERVER the host polls ────────────────────────────────────────
// Nothing calls into this program. It accepts, answers by id, and completes —
// the loop below is what a TTimer would do, and it never leaves the main thread.
procedure DemoQueuedServer;
const
  Bind = '127.0.0.1:50077';
var
  Handle: PGrpcServerHandle;
  Queue: PCallQueue;
  CallId: UInt64;
  Method: array[0..255] of Byte;
  MethodLen, Got: NativeUInt;
  Buf: array[0..4095] of Byte;
  Rc, Spins: Integer;
  Pending, Live: NativeUInt;
  Reply: TBytes;
begin
  Head('gRPC server (queued — no callback, no worker thread)');
  if zpr_grpc_server_start_queued(Bind, Handle, Queue) <> 0 then
  begin
    WriteLn('  could not bind ', Bind, ': ', LastError);
    Exit;
  end;
  try
    WriteLn('  listening on ', Bind, ' — try: grpcurl -plaintext ', Bind, ' any.Service/Method');
    for Spins := 1 to 30 do
    begin
      Rc := zpr_grpc_accept(Queue, CallId, @Method[0], Length(Method), MethodLen);
      if Rc = 1 then
      begin
        WriteLn('  accepted #', CallId, ' ', PAnsiChar(@Method[0]));
        // Drain the request, answer once, and ALWAYS complete.
        repeat
          Rc := zpr_grpc_call_read_into(Queue, CallId, @Buf[0], Length(Buf), Got);
          if Rc = 1 then WriteLn('    request message: ', Got, ' bytes');
        until Rc <> 1;
        SetLength(Reply, 0);
        zpr_grpc_call_write(Queue, CallId, PByte(Reply), 0);
        // ⚠ AN ID NEVER COMPLETED LEAVES THE CLIENT WAITING ON TRAILERS THAT
        // NEVER COME — the call hangs rather than failing.
        zpr_grpc_call_complete(Queue, CallId, 0, nil);
        WriteLn('    completed OK');
      end;
      Sleep(100);
    end;
    zpr_grpc_queue_stats(Queue, Pending, Live);
    WriteLn('  queue: ', Pending, ' pending, ', Live, ' live (live should be 0)');
  finally
    zpr_grpc_server_stop(Handle);
    zpr_grpc_queue_free(Queue);
  end;
end;

// ── 8. the transcoder ──────────────────────────────────────────────────────
// Puts REST/JSON in front of a gRPC-only daemon, routed entirely by the
// descriptor set — no route table, nothing here knows a method name.
procedure DemoTranscoder(APool: TZprProtobufPool);
const
  Bind = '127.0.0.1:8098';
var
  Handle: PTranscodeHandle;
begin
  Head('JSON -> gRPC transcoder');
  if (GEndpoint = '') or (APool = nil) then
  begin
    WriteLn('  skipped (needs an endpoint and a descriptor set)');
    Exit;
  end;
  if zpr_transcode_start(Bind, PAnsiChar(GEndpoint), APool.Handle, 5000, Handle) <> 0 then
  begin
    WriteLn('  could not start: ', LastError);
    Exit;
  end;
  try
    WriteLn('  transcoding http://', Bind, ' -> ', GEndpoint);
    WriteLn('  try: curl -X POST http://', Bind, '/sapphire.v1.HealthService/Ping \');
    WriteLn('            -H ''Content-Type: application/json'' -d ''{}''');
    Sleep(15000);
  finally
    zpr_transcode_stop(Handle);
  end;
end;

var
  Pool: TZprProtobufPool;
begin
  if ParamCount >= 1 then GEndpoint := UTF8String(ParamStr(1));
  if ParamCount >= 2 then GDescriptors := ParamStr(2);

  Load;
  WriteLn('zpr ', Version);

  Pool := nil;
  try
    DemoJson;
    DemoHttp;
    Pool := DemoProtobuf;
    DemoUnary(Pool);
    DemoStreaming(Pool);
    DemoBidi;
    DemoQueuedServer;
    DemoTranscoder(Pool);
  finally
    Pool.Free;
    Unload;
  end;

  WriteLn;
  WriteLn('done.');
end.
