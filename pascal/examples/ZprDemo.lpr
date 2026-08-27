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
  Stream: TZprGrpcClientStream;
  Req: TBytes;
  Buf: array[0..65535] of Byte;
  Got: NativeUInt;
  Reads, Spins: Integer;
begin
  Head('gRPC server-streaming (ring-buffered, non-blocking)');
  if (GEndpoint = '') or (APool = nil) then
  begin
    WriteLn('  skipped (needs an endpoint and a descriptor set)');
    Exit;
  end;
  try
    Req := APool.JsonToBinary('sapphire.v1.Empty', '{}');
    Stream := TZprGrpcClient.OpenStream(GEndpoint,
      '/sapphire.v1.MarketDataService/StreamSnapshots', Req, 5000);
  except
    on E: Exception do
    begin
      WriteLn('  could not open: ', E.Message);
      Exit;
    end;
  end;
  try
    // 256 messages of slack. Full discards the OLDEST — right for marks, wrong
    // for anything whose successor does not carry what it said.
    Stream.Buffer(256);
    Reads := 0;
    for Spins := 1 to 40 do
    begin
      case Stream.ReadInto(@Buf[0], Length(Buf), Got) of
        zrMessage:
          begin
            Inc(Reads);
            if Reads = 1 then
              WriteLn('  first message: ', Got, ' bytes');
          end;
        zrError:
          begin
            WriteLn('  stream error: ', LastError);
            Break;
          end;
      end;
      Sleep(50); // a TTimer interval, in a console program
    end;
    WriteLn('  read ', Reads, ' message(s); ring depth ', Stream.Depth,
            ', dropped ', Stream.Dropped);
    if Stream.Dropped > 0 then
      WriteLn('  ^ this program fell behind. That counter is the only sign of it.');
  finally
    Stream.Free;
  end;
end;

// ── 6. bidirectional streaming ─────────────────────────────────────────────
// Client-streaming is this with one read at the end.
procedure DemoBidi;
var
  Bidi: TZprGrpcBidiStream;
  Payload: TBytes;
begin
  Head('gRPC bidirectional');
  if GEndpoint = '' then
  begin
    WriteLn('  skipped (no endpoint)');
    Exit;
  end;
  try
    Bidi := TZprGrpcBidiStream.Open(GEndpoint, '/sapphire.v1.HealthService/Ping');
  except
    on E: Exception do
    begin
      WriteLn('  refused, as expected — Ping is not bidirectional: ', E.Message);
      Exit;
    end;
  end;
  try
    SetLength(Payload, 0);
    Bidi.Send(Payload);
    // ⚠ WITHOUT THIS A CLIENT-STREAMING SERVER NEVER ANSWERS: it waits for the
    // end of the request stream before computing its single reply.
    Bidi.CloseSend;
    WriteLn('  sent one message and half-closed');
  finally
    Bidi.Free;
  end;
end;

// ── 7. a gRPC SERVER the host polls ────────────────────────────────────────
// Nothing calls into this program. It accepts, answers by id, and completes —
// the loop below is what a TTimer would do, and it never leaves the main thread.
procedure DemoQueuedServer;
const
  Bind = '127.0.0.1:50077';
var
  Server: TZprGrpcQueuedServer;
  Call: TZprGrpcCall;
  Req, Reply: TBytes;
  Spins: Integer;
  Res: TZprReadResult;
begin
  Head('gRPC server (queued — no callback, no worker thread)');
  try
    Server := TZprGrpcQueuedServer.Start(Bind);
  except
    on E: Exception do
    begin
      WriteLn('  could not bind ', Bind, ': ', E.Message);
      Exit;
    end;
  end;
  try
    WriteLn('  listening on ', Bind, ' — try: grpcurl -plaintext ', Bind, ' any.Service/Method');
    for Spins := 1 to 30 do
    begin
      Call := Server.Accept;
      if Call <> nil then
        try
          WriteLn('  accepted #', Call.Id, ' ', string(Call.MethodPath));
          repeat
            Res := Call.Read(Req);
            if Res = zrMessage then
              WriteLn('    request message: ', Length(Req), ' bytes');
          until Res <> zrMessage;
          SetLength(Reply, 0);
          Call.Write(Reply);
          Call.Complete(0);
          WriteLn('    completed OK');
        finally
          // Freeing completes the call if we did not — a call left incomplete
          // HANGS its client rather than failing it.
          Call.Free;
        end;
      Sleep(100);
    end;
    WriteLn('  queue: ', Server.Pending, ' pending, ', Server.Live, ' live (live should be 0)');
  finally
    Server.Free;
  end;
end;

// ── 8. the transcoder ──────────────────────────────────────────────────────
// Puts REST/JSON in front of a gRPC-only daemon, routed entirely by the
// descriptor set — no route table, nothing here knows a method name.
procedure DemoTranscoder(APool: TZprProtobufPool);
const
  Bind = '127.0.0.1:8098';
var
  Transcoder: TZprTranscoder;
begin
  Head('JSON -> gRPC transcoder');
  if (GEndpoint = '') or (APool = nil) then
  begin
    WriteLn('  skipped (needs an endpoint and a descriptor set)');
    Exit;
  end;
  try
    Transcoder := TZprTranscoder.Start(Bind, GEndpoint, APool);
  except
    on E: Exception do
    begin
      WriteLn('  could not start: ', E.Message);
      Exit;
    end;
  end;
  try
    WriteLn('  transcoding http://', Bind, ' -> ', GEndpoint);
    WriteLn('  try: curl -X POST http://', Bind, '/sapphire.v1.HealthService/Ping');
    WriteLn('       -H ''Content-Type: application/json'' -d ''{}'');
    Sleep(15000);
  finally
    Transcoder.Free;
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
