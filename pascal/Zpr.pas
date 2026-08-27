// Zpr.pas — an Object Pascal wrapper around zpr (Zero Portable Runtime), a
// C-ABI shim that gives Object Pascal (Lazarus/FPC and Delphi alike) three
// capabilities it has no native equivalent for: JSON, an HTTP client, and a
// generic gRPC/protobuf client + server.
//
// This unit compiles unmodified under both FPC ({$MODE DELPHI}) and Delphi.
// It talks to zpr as a dynamically loaded shared library (zpr.dll /
// libzpr.so / libzpr.dylib) resolved at runtime via LoadLibrary/dlopen —
// never statically linked — which is what keeps one unit source portable
// across both compilers and all three platforms without touching a linker.
//
// Quick start:
//
//   uses Zpr;
//
//   Load; // looks next to the running executable by default
//   try
//     var J := TZprJson.NewObject;
//     try
//       J.SetField('hello', TZprJson.NewString('world'));
//       WriteLn(J.ToJson);
//     finally
//       J.Free;
//     end;
//   finally
//     Unload;
//   end;
unit Zpr;

{$IFDEF FPC}
  {$MODE DELPHI}
  {$H+}
  {$MODESWITCH ADVANCEDRECORDS}
{$ENDIF}

interface

uses
  SysUtils, Classes
  {$IFDEF FPC}
  , dynlibs
  {$ELSE}
    {$IFDEF MSWINDOWS}
    , Winapi.Windows
    {$ELSE}
    , Posix.Dlfcn
    {$ENDIF}
  {$ENDIF}
  ;

// ---------------------------------------------------------------------
// Result codes, mirroring the constants in zpr.h. Left un-prefixed like
// the rest of the interface, but matched verbatim to the C ABI names
// since these mirror the wire contract, not a type.
// ---------------------------------------------------------------------

const
  ZPR_OK = 0;
  ZPR_ERR = -1;

  GRPC_CALL_OK = 0;
  GRPC_CALL_STATUS_ERR = 1;
  GRPC_CALL_TRANSPORT_ERR = -1;

  zkNull = 0;
  zkBool = 1;
  zkNumber = 2;
  zkString = 3;
  zkArray = 4;
  zkObject = 5;

type
  EError = class(Exception);
  ELoadError = class(EError);

  // Opaque handles. Each is just a Pointer at the ABI level; distinct types
  // exist only so the Pascal compiler catches mixing them up.
  PJsonValue = Pointer;
  PDescriptorPool = Pointer;
  PGrpcServerHandle = Pointer;
  PGrpcStream = Pointer;
  PCallQueue = Pointer;
  PGrpcBidiStream = Pointer;
  PTranscodeHandle = Pointer;
  PGrpcClientStream = Pointer;

  // Mirrors the C ZprBuffer struct exactly: a Rust-owned byte buffer.
  // Application code normally never sees this directly — the wrapper
  // classes below convert it to/from TBytes and free it immediately.
  TZprBuffer = record
    Data: PByte;
    Len: NativeUInt;
    Cap: NativeUInt;
  end;

  // Callback signature for zpr_grpc_server_start. Application code
  // registers a TZprGrpcCallEvent (a method) instead of this directly — see
  // TZprGrpcServer — this is the raw C-ABI shape underneath it.
  TZprGrpcHandlerProc = procedure(MethodPath: PAnsiChar; Stream: PGrpcStream;
    UserData: Pointer; out OutGrpcStatus: Int32; out OutMessage: PByte;
    out OutMessageLen: NativeUInt); cdecl;

// ---------------------------------------------------------------------
// Library lifecycle
// ---------------------------------------------------------------------

// Loads zpr. With ALibraryPath = '' (the default), looks for the
// platform-default file name (zpr.dll / libzpr.so / libzpr.dylib) next
// to the running executable, then falls back to the platform's normal
// shared-library search path. Raises ELoadError on failure. Calling this
// again while already loaded is a no-op.
procedure Load(const ALibraryPath: string = '');

// Unloads zpr. Safe to call even if not loaded.
procedure Unload;

function IsLoaded: Boolean;

function Version: UTF8String;

// The last error message set on the calling thread by the most recent
// zpr call, or '' if none. Wrapper methods below already fold this
// into the EError they raise — call this directly only if you're calling
// the raw zpr_* procedural variables yourself.
function LastError: UTF8String;

// ---------------------------------------------------------------------
// JSON
// ---------------------------------------------------------------------

type
  // Wraps a JsonValue handle. Every instance owns its handle and frees it
  // on destruction, EXCEPT after being handed to Push/SetField, which
  // transfer ownership into the parent value (matching the C API, where
  // the value is consumed) — do not use or free an instance after that.
  TZprJson = class
  private
    FHandle: PJsonValue;
    FOwnsHandle: Boolean;
    constructor CreateFromHandle(AHandle: PJsonValue; AOwnsHandle: Boolean);
    function GetKind: Integer;
    function GetAsBoolean: Boolean;
    function GetAsFloat: Double;
    function GetAsString: UTF8String;
    function GetArrayLength: NativeInt;
    function GetItem(AIndex: NativeUInt): TZprJson;
    function GetField(const AKey: UTF8String): TZprJson;
  public
    destructor Destroy; override;

    class function Parse(const AJson: UTF8String): TZprJson;
    class function NewNull: TZprJson;
    class function NewBool(AValue: Boolean): TZprJson;
    class function NewFloat(AValue: Double): TZprJson;
    class function NewString(const AValue: UTF8String): TZprJson;
    class function NewArray: TZprJson;
    class function NewObject: TZprJson;

    procedure Push(AValue: TZprJson);
    procedure SetField(const AKey: UTF8String; AValue: TZprJson);

    function Keys: TArray<UTF8String>;
    function ToJson(APretty: Boolean = False): UTF8String;

    property Kind: Integer read GetKind;
    property AsBoolean: Boolean read GetAsBoolean;
    property AsFloat: Double read GetAsFloat;
    property AsString: UTF8String read GetAsString;
    property ArrayLength: NativeInt read GetArrayLength;
    // Both return a new TZprJson the caller owns and must Free.
    property Item[AIndex: NativeUInt]: TZprJson read GetItem;
    property Field[const AKey: UTF8String]: TZprJson read GetField; default;
  end;

// ---------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------

type
  TZprHttpClient = class
  public
    // Blocks the calling thread until the request completes. AHeadersJson
    // is an optional JSON object of request headers (pass '' for none).
    // Raises EError on a transport-level failure (a non-2xx HTTP status is
    // NOT an error here — check AStatus yourself).
    class function Request(const AMethod, AUrl: UTF8String; const AHeadersJson: UTF8String;
      const ABody: TBytes; ATimeoutMs: Cardinal; out AStatus: Word;
      out AResponseHeadersJson: UTF8String): TBytes;

    // Overrides HTTP_PROXY/HTTPS_PROXY/ALL_PROXY/NO_PROXY detection. Must be
    // called before the first Request.
    class procedure SetProxy(const AProxyUrl: UTF8String);
    class procedure DisableProxy;
  end;

// ---------------------------------------------------------------------
// Protobuf <-> JSON, driven by a runtime-loaded descriptor pool
// ---------------------------------------------------------------------

type
  // Wraps a compiled FileDescriptorSet (the output of
  // `protoc --descriptor_set_out=out.bin --include_imports your.proto`),
  // letting Pascal build/read arbitrary protobuf messages by full type
  // name without any generated bindings.
  TZprProtobufPool = class
  private
    FHandle: PDescriptorPool;
  public
    destructor Destroy; override;

    /// The raw pool, for the transcoder — which routes by descriptor and so
    /// needs the pool itself rather than a converted message.
    property Handle: PDescriptorPool read FHandle;

    class function LoadFromBytes(const ADescriptorSet: TBytes): TZprProtobufPool;
    class function LoadFromFile(const AFileName: string): TZprProtobufPool;

    function JsonToBinary(const AMessageType, AJson: UTF8String): TBytes;
    function BinaryToJson(const AMessageType: UTF8String; const AData: TBytes): UTF8String;

    // The same conversions with NO TEXT in the middle — use these on a hot path.
    //
    // ⚠ BinaryToJson RENDERS A STRING, and a caller who then wants to read
    // fields hands it to TZprJson.Parse, which parses it straight back into the
    // tree it was just serialised from. Two full passes to arrive where the
    // first one already was. These build the value directly; on a per-tick path
    // that is most of the cost gone, and the result is an ordinary TZprJson.
    function BinaryToJsonValue(const AMessageType: UTF8String; const AData: TBytes): TZprJson;
    function JsonValueToBinary(const AMessageType: UTF8String; AValue: TZprJson): TBytes;
  end;

// ---------------------------------------------------------------------
// gRPC client — generic pass-through, no per-RPC codegen. Build/read
// message bytes with TZprProtobufPool above.
// ---------------------------------------------------------------------

type
  // Non-owning wrapper around a server-streaming call opened by
  // TZprGrpcClient.OpenStream. A unary call is just the degenerate case of
  // this with exactly one read.
  // What a non-blocking read found.
  TZprReadResult = (
    zrNone,        // nothing waiting. The ORDINARY answer when polling, not an error.
    zrMessage,     // a message was delivered
    zrClientDone,  // the far end finished sending
    zrShortBuffer, // the buffer was too small; the message is HELD, retry bigger
    zrError        // see LastError
  );

  TZprGrpcClientStream = class
  private
    FHandle: PGrpcClientStream;
    FBuffered: Boolean;
    function GetDepth: NativeUInt;
    function GetDropped: UInt64;
  public
    constructor Create(AHandle: PGrpcClientStream);
    destructor Destroy; override;

    // Blocks until the next message arrives. Returns False once the server
    // has finished cleanly (not an error — the stream just ended).
    //
    // ⚠ AFTER Buffer THIS NO LONGER BLOCKS, and False then means "nothing right
    // now" as well as "finished" — which is why polling code should use
    // ReadInto, whose result tells the two apart.
    function Read(out AData: TBytes): Boolean;

    // Reads into memory YOU own — no allocation, nothing to free. The shape a
    // polling loop should use.
    function ReadInto(ABuffer: PByte; ACapacity: NativeUInt; out ALength: NativeUInt): TZprReadResult;

    // Switches to a bounded ring so reads stop blocking: a timer on the main
    // thread becomes a complete feed loop, with no worker thread and nothing
    // entering your program from a thread it did not create.
    //
    // ⚠ LOSSY, AND THAT IS THE TRADE. A full ring discards its OLDEST message.
    // Right for marks and depth, where a stale tick is worthless. WRONG for
    // order events: a dropped fill is a position you never hear about, and the
    // next message does not carry what it said. Leave those unbuffered and read
    // them on a worker thread.
    procedure Buffer(ACapacity: NativeUInt);

    property IsBuffered: Boolean read FBuffered;
    // How many messages are waiting.
    property Depth: NativeUInt read GetDepth;
    // How many were DISCARDED because you did not keep up. ⚠ WATCH THIS. The
    // messages you do receive look perfectly healthy either way; this counter
    // is the only sign that anything went missing.
    property Dropped: UInt64 read GetDropped;
  end;

  // Both halves of a bidirectional call — and of a client-streaming one, which
  // is the same thing with a single read at the end.
  TZprGrpcBidiStream = class
  private
    FHandle: PGrpcBidiStream;
    FSendClosed: Boolean;
  public
    destructor Destroy; override;

    // Opens the call. Nothing is sent until Send.
    class function Open(const AEndpoint, AMethodPath: UTF8String; ASendCapacity: NativeUInt = 8;
      ATimeoutMs: Cardinal = 5000; const AMetadataJson: UTF8String = ''): TZprGrpcBidiStream;

    // Queues one request message. False means the send window is full — retry,
    // it is not a failure.
    function Send(const AData: TBytes): Boolean;

    // Says no more requests are coming, leaving the response half open.
    //
    // ⚠ A CLIENT-STREAMING SERVER DOES NOT ANSWER UNTIL YOU CALL THIS. It is
    // waiting for the end of your request stream to compute its one reply, so a
    // program that only ever sends waits forever for a response it never asked
    // to be produced.
    procedure CloseSend;

    function ReadInto(ABuffer: PByte; ACapacity: NativeUInt; out ALength: NativeUInt): TZprReadResult;
    procedure Buffer(ACapacity: NativeUInt);

    property SendClosed: Boolean read FSendClosed;
  end;

  // One inbound RPC, handed over by TZprGrpcQueuedServer.Accept. You own it and
  // must free it; freeing completes the call if you did not.
  TZprGrpcCall = class
  private
    FQueue: PCallQueue;
    FId: UInt64;
    FMethodPath: UTF8String;
    FCompleted: Boolean;
  public
    constructor Create(AQueue: PCallQueue; AId: UInt64; const AMethodPath: UTF8String);

    // ⚠ COMPLETES THE CALL IF YOU FORGOT, with UNAVAILABLE. An RPC that is never
    // completed holds its channels open and leaves the caller waiting on
    // trailers that never arrive — the request HANGS rather than failing, which
    // is the worst of the two. Relying on this is still a bug; it just is not
    // a hung client as well.
    destructor Destroy; override;

    // Next request message. zrNone means "not yet" — poll again.
    function Read(out AData: TBytes): TZprReadResult;

    // One response message. A unary reply is exactly one of these.
    function Write(const AData: TBytes): Boolean;

    // Ends the call. 0 is OK; anything else is a google.rpc.Code.
    procedure Complete(AStatus: Integer = 0; const AMessage: UTF8String = '');

    property Id: UInt64 read FId;
    property MethodPath: UTF8String read FMethodPath;
    property IsCompleted: Boolean read FCompleted;
  end;

  // A gRPC server that QUEUES calls instead of calling you back.
  //
  // ⚠ THIS IS THE ONE A GUI SHOULD HOST. Nothing here ever enters your program
  // from a thread it did not create: you Accept, answer, and Complete, all on
  // whichever thread you like. TZprGrpcServer — the callback one — runs handlers
  // on worker threads concurrently, which neither the LCL nor the VCL survives
  // being touched from. Use that one only in a headless program.
  TZprGrpcQueuedServer = class
  private
    FHandle: PGrpcServerHandle;
    FQueue: PCallQueue;
    function GetPending: NativeUInt;
    function GetLive: NativeUInt;
  public
    destructor Destroy; override;

    class function Start(const ABindAddr: UTF8String): TZprGrpcQueuedServer;

    // The next waiting RPC, or nil when none is. Never blocks — call it from a
    // timer. You own what it returns.
    function Accept: TZprGrpcCall;

    procedure Stop;

    // The two limits that stop a bug in YOUR code becoming an outage.
    // AMaxPending: waiting RPCs before new ones are refused RESOURCE_EXHAUSTED.
    // ADeadlineMs: how long an accepted call may go uncompleted before the
    // server answers it DEADLINE_EXCEEDED and forgets it; 0 disables that.
    // Defaults are 1024 and 30000.
    procedure Configure(AMaxPending: UInt64 = 1024; ADeadlineMs: UInt64 = 30000);

    // Lifetime totals. ⚠ Refused AND Reaped ARE THE TWO THAT MEAN SOMETHING IS
    // WRONG: the first says you are not accepting fast enough, the second that
    // you are accepting calls and not completing them.
    procedure Counters(out AAccepted, ACompleted, ARefused, AReaped: UInt64);

    property Pending: NativeUInt read GetPending;
    // Accepted and not yet completed. ⚠ A NUMBER THAT ONLY GROWS IS A LEAK —
    // calls you accepted and never completed.
    property Live: NativeUInt read GetLive;
  end;

  // REST/JSON in front of a gRPC-only daemon, routed entirely by a descriptor
  // set — no route table, and nothing here knows a method name.
  TZprTranscoder = class
  private
    FHandle: PTranscodeHandle;
  public
    destructor Destroy; override;

    // Serves POST /package.Service/Method with a JSON body on ABindAddr,
    // forwarding to AUpstream (an h2c gRPC endpoint). APool must outlive
    // nothing — it is copied.
    class function Start(const ABindAddr, AUpstream: UTF8String; APool: TZprProtobufPool;
      ATimeoutMs: Cardinal = 5000): TZprTranscoder;

    procedure Stop;
  end;

  TZprGrpcClient = class
  public
    // Makes one unary call. AEndpoint is a URI, e.g. "http://127.0.0.1:50051"
    // (or "https://..." for TLS). AMethodPath is the full gRPC path, e.g.
    // "/pkg.Service/Method". AMetadataJson is an optional (pass '' for none)
    // JSON object of gRPC metadata/headers to send with the call — build it
    // once wherever your auth headers already live and pass it on every
    // call. Raises EError only for a transport-level failure (never
    // reaching a gRPC status); a non-OK gRPC status is reported via
    // AGrpcStatus instead (0 = OK), since that's an ordinary RPC-level
    // outcome the caller should handle, not an exception.
    class function Call(const AEndpoint, AMethodPath: UTF8String; const ARequest: TBytes;
      ATimeoutMs: Cardinal; out AGrpcStatus: Integer; const AMetadataJson: UTF8String = ''): TBytes;

    // Opens a server-streaming call, sending ARequest as the single
    // initiating message. ATimeoutMs bounds only opening the call, never
    // the stream's lifetime. Raises EError for a transport-level failure or
    // a non-OK gRPC status (unlike Call, there is no partial-success
    // shape to hand back before any message has arrived).
    class function OpenStream(const AEndpoint, AMethodPath: UTF8String; const ARequest: TBytes;
      ATimeoutMs: Cardinal; const AMetadataJson: UTF8String = ''): TZprGrpcClientStream;

    class procedure SetProxy(const AProxyUrl: UTF8String);
    class procedure DisableProxy;
  end;

// ---------------------------------------------------------------------
// gRPC server — one registered handler serves every method, unary or
// streaming alike; unary is just "read one message, write one message".
// ---------------------------------------------------------------------

type
  // Non-owning wrapper around one in-flight RPC's stream. Only valid for the
  // duration of the TZprGrpcCallEvent invocation that receives it.
  TZprGrpcStream = class
  private
    FHandle: PGrpcStream;
  public
    constructor Create(AHandle: PGrpcStream);

    // Blocks until the next inbound message arrives. Returns False once the
    // client has finished sending (a unary call sees exactly one message,
    // then False).
    function Read(out AData: TBytes): Boolean;

    // Sends one outbound message. AData is copied immediately.
    procedure Write(const AData: TBytes);
  end;

  TZprGrpcCallEvent = procedure(Sender: TObject; const AMethodPath: UTF8String;
    AStream: TZprGrpcStream; out AGrpcStatus: Integer; out AMessage: UTF8String) of object;

  TZprGrpcServer = class
  private
    FHandle: PGrpcServerHandle;
    FOnCall: TZprGrpcCallEvent;
    function GetIsRunning: Boolean;
  public
    destructor Destroy; override;

    // Binds and starts serving immediately on a background thread; returns
    // once the socket is bound. ABindAddr is e.g. "127.0.0.1:50052".
    class function Start(const ABindAddr: UTF8String; AOnCall: TZprGrpcCallEvent): TZprGrpcServer;

    // Stops accepting connections and joins the server thread. Safe to call
    // more than once.
    procedure Stop;

    property IsRunning: Boolean read GetIsRunning;
  end;

implementation

// =======================================================================
// Platform shim: dynamic library loading. This is the ONLY section that
// differs between FPC and Delphi, and between Windows and POSIX — every
// wrapper class above this line is plain, portable Object Pascal.
// =======================================================================

{$IF DEFINED(MSWINDOWS) OR DEFINED(WIN32) OR DEFINED(WIN64)}
  {$DEFINE ZPR_WINDOWS}
{$IFEND}
{$IFNDEF ZPR_WINDOWS}
  {$DEFINE ZPR_POSIX}
{$ENDIF}
{$IF DEFINED(DARWIN) OR DEFINED(MACOS) OR DEFINED(MACOS64) OR DEFINED(OSX)}
  {$DEFINE ZPR_DARWIN}
{$IFEND}

type
  {$IFDEF FPC}
  TZprLibHandle = dynlibs.TLibHandle;
  {$ELSE}
    {$IFDEF ZPR_WINDOWS}
  TZprLibHandle = HMODULE;
    {$ELSE}
  TZprLibHandle = Pointer;
    {$ENDIF}
  {$ENDIF}

const
  {$IFDEF FPC}
  InvalidLibHandle = dynlibs.NilHandle;
  {$ELSE}
  InvalidLibHandle = TZprLibHandle(0);
  {$ENDIF}

function DefaultLibraryFileName: string;
begin
  {$IFDEF ZPR_WINDOWS}
  Result := 'zpr.dll';
  {$ELSE}
    {$IFDEF ZPR_DARWIN}
  Result := 'libzpr.dylib';
    {$ELSE}
  Result := 'libzpr.so';
    {$ENDIF}
  {$ENDIF}
end;

function DoLoadLibrary(const APath: string): TZprLibHandle;
begin
  {$IFDEF FPC}
  Result := dynlibs.LoadLibrary(APath);
  {$ELSE}
    {$IFDEF ZPR_WINDOWS}
  Result := Winapi.Windows.LoadLibrary(PChar(APath));
    {$ELSE}
  Result := Posix.Dlfcn.dlopen(MarshaledAString(UTF8String(APath)), RTLD_NOW);
    {$ENDIF}
  {$ENDIF}
end;

function DoGetProcAddress(ALib: TZprLibHandle; const AName: AnsiString): Pointer;
begin
  {$IFDEF FPC}
  Result := dynlibs.GetProcedureAddress(ALib, AName);
  {$ELSE}
    {$IFDEF ZPR_WINDOWS}
  Result := Winapi.Windows.GetProcAddress(ALib, PAnsiChar(AName));
    {$ELSE}
  Result := Posix.Dlfcn.dlsym(ALib, MarshaledAString(AName));
    {$ENDIF}
  {$ENDIF}
end;

procedure DoFreeLibrary(ALib: TZprLibHandle);
begin
  if ALib = InvalidLibHandle then
    Exit;
  {$IFDEF FPC}
  dynlibs.FreeLibrary(ALib);
  {$ELSE}
    {$IFDEF ZPR_WINDOWS}
  Winapi.Windows.FreeLibrary(ALib);
    {$ELSE}
  Posix.Dlfcn.dlclose(ALib);
    {$ENDIF}
  {$ENDIF}
end;

// =======================================================================
// Raw C-ABI procedural types and the resolved function pointers. Names
// match zpr.h exactly.
// =======================================================================

type
  Tzpr_version = function: PAnsiChar; cdecl;
  Tzpr_buffer_free = procedure(Buf: TZprBuffer); cdecl;
  Tzpr_alloc = function(Len: NativeUInt): PByte; cdecl;
  Tzpr_string_free = procedure(S: PAnsiChar); cdecl;
  Tzpr_last_error = function: PAnsiChar; cdecl;

  Tzpr_grpc_set_proxy = function(ProxyUrl: PAnsiChar): Int32; cdecl;
  Tzpr_grpc_call = function(Endpoint, MethodPath, MetadataJson: PAnsiChar; Request: PByte;
    RequestLen: NativeUInt; TimeoutMs: UInt32; out OutResponse: TZprBuffer;
    out OutGrpcStatus: Int32): Int32; cdecl;

  Tzpr_grpc_client_stream_open = function(Endpoint, MethodPath, MetadataJson: PAnsiChar; Request: PByte;
    RequestLen: NativeUInt; TimeoutMs: UInt32; out OutHandle: PGrpcClientStream;
    out OutGrpcStatus: Int32): Int32; cdecl;
  Tzpr_grpc_client_stream_read = function(Stream: PGrpcClientStream; out Output: TZprBuffer): Int32; cdecl;
  Tzpr_grpc_client_stream_cancel = procedure(Stream: PGrpcClientStream); cdecl;
  Tzpr_grpc_client_stream_read_into = function(Stream: PGrpcClientStream; Output: PByte;
    OutCap: NativeUInt; out OutLen: NativeUInt): Int32; cdecl;
  Tzpr_grpc_client_stream_buffer = function(Stream: PGrpcClientStream; Capacity: NativeUInt): Int32; cdecl;
  Tzpr_grpc_client_stream_stats = function(Stream: PGrpcClientStream; out OutDepth: NativeUInt;
    out OutDropped: UInt64): Int32; cdecl;
  Tzpr_grpc_bidi_open = function(Endpoint, MethodPath, MetadataJson: PAnsiChar; SendCapacity: NativeUInt;
    TimeoutMs: UInt32; out OutHandle: PGrpcBidiStream; out OutGrpcStatus: Int32): Int32; cdecl;
  Tzpr_grpc_bidi_send = function(Stream: PGrpcBidiStream; Data: PByte; Len: NativeUInt): Int32; cdecl;
  Tzpr_grpc_bidi_close_send = function(Stream: PGrpcBidiStream): Int32; cdecl;
  Tzpr_grpc_bidi_read_into = function(Stream: PGrpcBidiStream; Output: PByte; OutCap: NativeUInt;
    out OutLen: NativeUInt): Int32; cdecl;
  Tzpr_grpc_bidi_buffer = function(Stream: PGrpcBidiStream; Capacity: NativeUInt): Int32; cdecl;
  Tzpr_grpc_bidi_cancel = procedure(Stream: PGrpcBidiStream); cdecl;
  Tzpr_transcode_start = function(BindAddr, Upstream: PAnsiChar; Pool: PDescriptorPool;
    TimeoutMs: UInt32; out OutHandle: PTranscodeHandle): Int32; cdecl;
  Tzpr_transcode_stop = function(Handle: PTranscodeHandle): Int32; cdecl;

  Tzpr_protobuf_pool_new = function(DescriptorSet: PByte; Len: NativeUInt): PDescriptorPool; cdecl;
  Tzpr_protobuf_pool_free = procedure(Handle: PDescriptorPool); cdecl;
  Tzpr_protobuf_json_to_binary = function(Pool: PDescriptorPool; MessageType, Json: PAnsiChar;
    out Output: TZprBuffer): Int32; cdecl;
  Tzpr_protobuf_binary_to_json = function(Pool: PDescriptorPool; MessageType: PAnsiChar;
    Data: PByte; DataLen: NativeUInt): PAnsiChar; cdecl;
  Tzpr_protobuf_binary_to_json_value = function(Pool: PDescriptorPool; MessageType: PAnsiChar;
    Data: PByte; DataLen: NativeUInt): PJsonValue; cdecl;
  Tzpr_protobuf_json_value_to_binary = function(Pool: PDescriptorPool; MessageType: PAnsiChar;
    Value: PJsonValue; out Output: TZprBuffer): Int32; cdecl;

  Tzpr_grpc_stream_read = function(Stream: PGrpcStream; out Output: TZprBuffer): Int32; cdecl;
  Tzpr_grpc_stream_write = function(Stream: PGrpcStream; Data: PByte; Len: NativeUInt): Int32; cdecl;

  Tzpr_grpc_server_start = function(BindAddr: PAnsiChar; Handler: TZprGrpcHandlerProc;
    UserData: Pointer; out OutHandle: PGrpcServerHandle): Int32; cdecl;
  Tzpr_grpc_server_stop = function(Handle: PGrpcServerHandle): Int32; cdecl;
  Tzpr_grpc_server_start_queued = function(BindAddr: PAnsiChar; out OutHandle: PGrpcServerHandle;
    out OutQueue: PCallQueue): Int32; cdecl;
  Tzpr_grpc_accept = function(Queue: PCallQueue; out OutCallId: UInt64; OutMethod: PByte;
    OutMethodCap: NativeUInt; out OutMethodLen: NativeUInt): Int32; cdecl;
  Tzpr_grpc_call_read_into = function(Queue: PCallQueue; CallId: UInt64; Output: PByte;
    OutCap: NativeUInt; out OutLen: NativeUInt): Int32; cdecl;
  Tzpr_grpc_call_write = function(Queue: PCallQueue; CallId: UInt64; Data: PByte; Len: NativeUInt): Int32; cdecl;
  Tzpr_grpc_call_complete = function(Queue: PCallQueue; CallId: UInt64; GrpcStatus: Int32;
    Message: PAnsiChar): Int32; cdecl;
  Tzpr_grpc_queue_stats = function(Queue: PCallQueue; out OutPending: NativeUInt;
    out OutLive: NativeUInt): Int32; cdecl;
  Tzpr_grpc_queue_free = procedure(Queue: PCallQueue); cdecl;
  Tzpr_grpc_queue_configure = function(Queue: PCallQueue; MaxPending, DeadlineMs: UInt64): Int32; cdecl;
  Tzpr_grpc_queue_counters = function(Queue: PCallQueue; out OutAccepted, OutCompleted,
    OutRefused, OutReaped: UInt64): Int32; cdecl;

  Tzpr_http_set_proxy = function(ProxyUrl: PAnsiChar): Int32; cdecl;
  Tzpr_http_request = function(Method, Url, HeadersJson: PAnsiChar; Body: PByte;
    BodyLen: NativeUInt; TimeoutMs: UInt32; out OutStatus: UInt16;
    out OutHeadersJson: PAnsiChar; out OutBody: TZprBuffer): Int32; cdecl;

  Tzpr_json_parse = function(Text: PAnsiChar): PJsonValue; cdecl;
  Tzpr_json_free = procedure(Handle: PJsonValue); cdecl;
  Tzpr_json_stringify = function(Handle: PJsonValue; Pretty: Int32): PAnsiChar; cdecl;
  Tzpr_json_kind = function(Handle: PJsonValue): Int32; cdecl;
  Tzpr_json_as_bool = function(Handle: PJsonValue; out Value: Byte): Int32; cdecl;
  Tzpr_json_as_f64 = function(Handle: PJsonValue; out Value: Double): Int32; cdecl;
  Tzpr_json_as_string = function(Handle: PJsonValue): PAnsiChar; cdecl;
  Tzpr_json_array_len = function(Handle: PJsonValue): NativeInt; cdecl;
  Tzpr_json_array_get = function(Handle: PJsonValue; Index: NativeUInt): PJsonValue; cdecl;
  Tzpr_json_object_get = function(Handle: PJsonValue; Key: PAnsiChar): PJsonValue; cdecl;
  Tzpr_json_object_keys = function(Handle: PJsonValue): PAnsiChar; cdecl;
  Tzpr_json_new_null = function: PJsonValue; cdecl;
  Tzpr_json_new_bool = function(B: Byte): PJsonValue; cdecl;
  Tzpr_json_new_f64 = function(N: Double): PJsonValue; cdecl;
  Tzpr_json_new_string = function(S: PAnsiChar): PJsonValue; cdecl;
  Tzpr_json_new_array = function: PJsonValue; cdecl;
  Tzpr_json_new_object = function: PJsonValue; cdecl;
  Tzpr_json_array_push = function(Arr, Value: PJsonValue): Int32; cdecl;
  Tzpr_json_object_set = function(Obj: PJsonValue; Key: PAnsiChar; Value: PJsonValue): Int32; cdecl;

var
  GLib: TZprLibHandle = InvalidLibHandle;

{$IFDEF ZPR_STATIC}
// ── STATICALLY LINKED ─────────────────────────────────────────────────────────
//
// ⚠ iOS IS WHY THIS MODE EXISTS. The dynamic path below is the portable one and
// stays the default — one unit, no linker configuration, three desktop
// platforms. It cannot work on iOS: an app may only load code inside its own
// bundle, a bare .so is not that, and the store refuses builds that try. Android
// is friendlier but still wants the .so packaged per-ABI into the APK.
//
// So on those two the library is linked, not loaded, and the symbols below are
// resolved by the LINKER instead of by `Load`. Everything above and below this
// block is unchanged, and every call site reads identically — which is the point
// of doing it here rather than in the call sites.
//
// Build the archive with the target's own toolchain, e.g.
//   cargo build --release --target aarch64-apple-ios          → libzpr.a
//   cargo build --release --target aarch64-linux-android      → libzpr.a
// then pass it to the Pascal linker. `crate-type` already emits `staticlib`.
//
// ⚠ A STATIC BUILD DRAGS IN THE WHOLE RUNTIME. tokio, rustls and hyper are all
// in that archive; expect a much larger binary than the dylib, and strip it.
{$IFDEF FPC}
  // FPC: the archive is named to the linker, and the symbols are bare.
  {$LINKLIB zpr}
function zpr_version: PAnsiChar; cdecl; external name 'zpr_version';
procedure zpr_buffer_free(Buf: TZprBuffer); cdecl; external name 'zpr_buffer_free';
function zpr_alloc(Len: NativeUInt): PByte; cdecl; external name 'zpr_alloc';
procedure zpr_string_free(S: PAnsiChar); cdecl; external name 'zpr_string_free';
function zpr_last_error: PAnsiChar; cdecl; external name 'zpr_last_error';
function zpr_grpc_set_proxy(ProxyUrl: PAnsiChar): Int32; cdecl; external name 'zpr_grpc_set_proxy';
function zpr_grpc_call(Endpoint, MethodPath, MetadataJson: PAnsiChar; Request: PByte;
    RequestLen: NativeUInt; TimeoutMs: UInt32; out OutResponse: TZprBuffer;
    out OutGrpcStatus: Int32): Int32; cdecl; external name 'zpr_grpc_call';
function zpr_grpc_client_stream_open(Endpoint, MethodPath, MetadataJson: PAnsiChar; Request: PByte;
    RequestLen: NativeUInt; TimeoutMs: UInt32; out OutHandle: PGrpcClientStream;
    out OutGrpcStatus: Int32): Int32; cdecl; external name 'zpr_grpc_client_stream_open';
function zpr_grpc_client_stream_read(Stream: PGrpcClientStream; out Output: TZprBuffer): Int32; cdecl; external name 'zpr_grpc_client_stream_read';
procedure zpr_grpc_client_stream_cancel(Stream: PGrpcClientStream); cdecl; external name 'zpr_grpc_client_stream_cancel';
function zpr_grpc_client_stream_read_into(Stream: PGrpcClientStream; Output: PByte;
    OutCap: NativeUInt; out OutLen: NativeUInt): Int32; cdecl; external name 'zpr_grpc_client_stream_read_into';
function zpr_grpc_client_stream_buffer(Stream: PGrpcClientStream; Capacity: NativeUInt): Int32; cdecl; external name 'zpr_grpc_client_stream_buffer';
function zpr_grpc_client_stream_stats(Stream: PGrpcClientStream; out OutDepth: NativeUInt;
    out OutDropped: UInt64): Int32; cdecl; external name 'zpr_grpc_client_stream_stats';
function zpr_grpc_bidi_open(Endpoint, MethodPath, MetadataJson: PAnsiChar; SendCapacity: NativeUInt;
    TimeoutMs: UInt32; out OutHandle: PGrpcBidiStream; out OutGrpcStatus: Int32): Int32; cdecl; external name 'zpr_grpc_bidi_open';
function zpr_grpc_bidi_send(Stream: PGrpcBidiStream; Data: PByte; Len: NativeUInt): Int32; cdecl; external name 'zpr_grpc_bidi_send';
function zpr_grpc_bidi_close_send(Stream: PGrpcBidiStream): Int32; cdecl; external name 'zpr_grpc_bidi_close_send';
function zpr_grpc_bidi_read_into(Stream: PGrpcBidiStream; Output: PByte; OutCap: NativeUInt;
    out OutLen: NativeUInt): Int32; cdecl; external name 'zpr_grpc_bidi_read_into';
function zpr_grpc_bidi_buffer(Stream: PGrpcBidiStream; Capacity: NativeUInt): Int32; cdecl; external name 'zpr_grpc_bidi_buffer';
procedure zpr_grpc_bidi_cancel(Stream: PGrpcBidiStream); cdecl; external name 'zpr_grpc_bidi_cancel';
function zpr_transcode_start(BindAddr, Upstream: PAnsiChar; Pool: PDescriptorPool;
    TimeoutMs: UInt32; out OutHandle: PTranscodeHandle): Int32; cdecl; external name 'zpr_transcode_start';
function zpr_transcode_stop(Handle: PTranscodeHandle): Int32; cdecl; external name 'zpr_transcode_stop';
function zpr_protobuf_pool_new(DescriptorSet: PByte; Len: NativeUInt): PDescriptorPool; cdecl; external name 'zpr_protobuf_pool_new';
procedure zpr_protobuf_pool_free(Handle: PDescriptorPool); cdecl; external name 'zpr_protobuf_pool_free';
function zpr_protobuf_json_to_binary(Pool: PDescriptorPool; MessageType, Json: PAnsiChar;
    out Output: TZprBuffer): Int32; cdecl; external name 'zpr_protobuf_json_to_binary';
function zpr_protobuf_binary_to_json(Pool: PDescriptorPool; MessageType: PAnsiChar;
    Data: PByte; DataLen: NativeUInt): PAnsiChar; cdecl; external name 'zpr_protobuf_binary_to_json';
function zpr_protobuf_binary_to_json_value(Pool: PDescriptorPool; MessageType: PAnsiChar;
    Data: PByte; DataLen: NativeUInt): PJsonValue; cdecl; external name 'zpr_protobuf_binary_to_json_value';
function zpr_protobuf_json_value_to_binary(Pool: PDescriptorPool; MessageType: PAnsiChar;
    Value: PJsonValue; out Output: TZprBuffer): Int32; cdecl; external name 'zpr_protobuf_json_value_to_binary';
function zpr_grpc_stream_read(Stream: PGrpcStream; out Output: TZprBuffer): Int32; cdecl; external name 'zpr_grpc_stream_read';
function zpr_grpc_stream_write(Stream: PGrpcStream; Data: PByte; Len: NativeUInt): Int32; cdecl; external name 'zpr_grpc_stream_write';
function zpr_grpc_server_start(BindAddr: PAnsiChar; Handler: TZprGrpcHandlerProc;
    UserData: Pointer; out OutHandle: PGrpcServerHandle): Int32; cdecl; external name 'zpr_grpc_server_start';
function zpr_grpc_server_stop(Handle: PGrpcServerHandle): Int32; cdecl; external name 'zpr_grpc_server_stop';
function zpr_grpc_server_start_queued(BindAddr: PAnsiChar; out OutHandle: PGrpcServerHandle;
    out OutQueue: PCallQueue): Int32; cdecl; external name 'zpr_grpc_server_start_queued';
function zpr_grpc_accept(Queue: PCallQueue; out OutCallId: UInt64; OutMethod: PByte;
    OutMethodCap: NativeUInt; out OutMethodLen: NativeUInt): Int32; cdecl; external name 'zpr_grpc_accept';
function zpr_grpc_call_read_into(Queue: PCallQueue; CallId: UInt64; Output: PByte;
    OutCap: NativeUInt; out OutLen: NativeUInt): Int32; cdecl; external name 'zpr_grpc_call_read_into';
function zpr_grpc_call_write(Queue: PCallQueue; CallId: UInt64; Data: PByte; Len: NativeUInt): Int32; cdecl; external name 'zpr_grpc_call_write';
function zpr_grpc_call_complete(Queue: PCallQueue; CallId: UInt64; GrpcStatus: Int32;
    Message: PAnsiChar): Int32; cdecl; external name 'zpr_grpc_call_complete';
function zpr_grpc_queue_stats(Queue: PCallQueue; out OutPending: NativeUInt;
    out OutLive: NativeUInt): Int32; cdecl; external name 'zpr_grpc_queue_stats';
procedure zpr_grpc_queue_free(Queue: PCallQueue); cdecl; external name 'zpr_grpc_queue_free';
function zpr_grpc_queue_configure(Queue: PCallQueue; MaxPending, DeadlineMs: UInt64): Int32; cdecl; external name 'zpr_grpc_queue_configure';
function zpr_grpc_queue_counters(Queue: PCallQueue; out OutAccepted, OutCompleted,
    OutRefused, OutReaped: UInt64): Int32; cdecl; external name 'zpr_grpc_queue_counters';
function zpr_http_set_proxy(ProxyUrl: PAnsiChar): Int32; cdecl; external name 'zpr_http_set_proxy';
function zpr_http_request(Method, Url, HeadersJson: PAnsiChar; Body: PByte;
    BodyLen: NativeUInt; TimeoutMs: UInt32; out OutStatus: UInt16;
    out OutHeadersJson: PAnsiChar; out OutBody: TZprBuffer): Int32; cdecl; external name 'zpr_http_request';
function zpr_json_parse(Text: PAnsiChar): PJsonValue; cdecl; external name 'zpr_json_parse';
procedure zpr_json_free(Handle: PJsonValue); cdecl; external name 'zpr_json_free';
function zpr_json_stringify(Handle: PJsonValue; Pretty: Int32): PAnsiChar; cdecl; external name 'zpr_json_stringify';
function zpr_json_kind(Handle: PJsonValue): Int32; cdecl; external name 'zpr_json_kind';
function zpr_json_as_bool(Handle: PJsonValue; out Value: Byte): Int32; cdecl; external name 'zpr_json_as_bool';
function zpr_json_as_f64(Handle: PJsonValue; out Value: Double): Int32; cdecl; external name 'zpr_json_as_f64';
function zpr_json_as_string(Handle: PJsonValue): PAnsiChar; cdecl; external name 'zpr_json_as_string';
function zpr_json_array_len(Handle: PJsonValue): NativeInt; cdecl; external name 'zpr_json_array_len';
function zpr_json_array_get(Handle: PJsonValue; Index: NativeUInt): PJsonValue; cdecl; external name 'zpr_json_array_get';
function zpr_json_object_get(Handle: PJsonValue; Key: PAnsiChar): PJsonValue; cdecl; external name 'zpr_json_object_get';
function zpr_json_object_keys(Handle: PJsonValue): PAnsiChar; cdecl; external name 'zpr_json_object_keys';
function zpr_json_new_null: PJsonValue; cdecl; external name 'zpr_json_new_null';
function zpr_json_new_bool(B: Byte): PJsonValue; cdecl; external name 'zpr_json_new_bool';
function zpr_json_new_f64(N: Double): PJsonValue; cdecl; external name 'zpr_json_new_f64';
function zpr_json_new_string(S: PAnsiChar): PJsonValue; cdecl; external name 'zpr_json_new_string';
function zpr_json_new_array: PJsonValue; cdecl; external name 'zpr_json_new_array';
function zpr_json_new_object: PJsonValue; cdecl; external name 'zpr_json_new_object';
function zpr_json_array_push(Arr, Value: PJsonValue): Int32; cdecl; external name 'zpr_json_array_push';
function zpr_json_object_set(Obj: PJsonValue; Key: PAnsiChar; Value: PJsonValue): Int32; cdecl; external name 'zpr_json_object_set';
{$ELSE}
  // Delphi names the library in the declaration itself. iOS links the static
  // archive; Android loads a .so that the Deployment Manager must place in the
  // APK for EVERY abi the app ships (armeabi-v7a and arm64-v8a are separate
  // builds of this crate, not one fat file).
const
  {$IFDEF IOS}
  ZPR_LIB = 'libzpr.a';
  {$ELSE}
  ZPR_LIB = 'libzpr.so';
  {$ENDIF}

function zpr_version: PAnsiChar; cdecl; external ZPR_LIB name 'zpr_version';
procedure zpr_buffer_free(Buf: TZprBuffer); cdecl; external ZPR_LIB name 'zpr_buffer_free';
function zpr_alloc(Len: NativeUInt): PByte; cdecl; external ZPR_LIB name 'zpr_alloc';
procedure zpr_string_free(S: PAnsiChar); cdecl; external ZPR_LIB name 'zpr_string_free';
function zpr_last_error: PAnsiChar; cdecl; external ZPR_LIB name 'zpr_last_error';
function zpr_grpc_set_proxy(ProxyUrl: PAnsiChar): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_set_proxy';
function zpr_grpc_call(Endpoint, MethodPath, MetadataJson: PAnsiChar; Request: PByte;
    RequestLen: NativeUInt; TimeoutMs: UInt32; out OutResponse: TZprBuffer;
    out OutGrpcStatus: Int32): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_call';
function zpr_grpc_client_stream_open(Endpoint, MethodPath, MetadataJson: PAnsiChar; Request: PByte;
    RequestLen: NativeUInt; TimeoutMs: UInt32; out OutHandle: PGrpcClientStream;
    out OutGrpcStatus: Int32): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_client_stream_open';
function zpr_grpc_client_stream_read(Stream: PGrpcClientStream; out Output: TZprBuffer): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_client_stream_read';
procedure zpr_grpc_client_stream_cancel(Stream: PGrpcClientStream); cdecl; external ZPR_LIB name 'zpr_grpc_client_stream_cancel';
function zpr_grpc_client_stream_read_into(Stream: PGrpcClientStream; Output: PByte;
    OutCap: NativeUInt; out OutLen: NativeUInt): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_client_stream_read_into';
function zpr_grpc_client_stream_buffer(Stream: PGrpcClientStream; Capacity: NativeUInt): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_client_stream_buffer';
function zpr_grpc_client_stream_stats(Stream: PGrpcClientStream; out OutDepth: NativeUInt;
    out OutDropped: UInt64): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_client_stream_stats';
function zpr_grpc_bidi_open(Endpoint, MethodPath, MetadataJson: PAnsiChar; SendCapacity: NativeUInt;
    TimeoutMs: UInt32; out OutHandle: PGrpcBidiStream; out OutGrpcStatus: Int32): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_bidi_open';
function zpr_grpc_bidi_send(Stream: PGrpcBidiStream; Data: PByte; Len: NativeUInt): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_bidi_send';
function zpr_grpc_bidi_close_send(Stream: PGrpcBidiStream): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_bidi_close_send';
function zpr_grpc_bidi_read_into(Stream: PGrpcBidiStream; Output: PByte; OutCap: NativeUInt;
    out OutLen: NativeUInt): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_bidi_read_into';
function zpr_grpc_bidi_buffer(Stream: PGrpcBidiStream; Capacity: NativeUInt): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_bidi_buffer';
procedure zpr_grpc_bidi_cancel(Stream: PGrpcBidiStream); cdecl; external ZPR_LIB name 'zpr_grpc_bidi_cancel';
function zpr_transcode_start(BindAddr, Upstream: PAnsiChar; Pool: PDescriptorPool;
    TimeoutMs: UInt32; out OutHandle: PTranscodeHandle): Int32; cdecl; external ZPR_LIB name 'zpr_transcode_start';
function zpr_transcode_stop(Handle: PTranscodeHandle): Int32; cdecl; external ZPR_LIB name 'zpr_transcode_stop';
function zpr_protobuf_pool_new(DescriptorSet: PByte; Len: NativeUInt): PDescriptorPool; cdecl; external ZPR_LIB name 'zpr_protobuf_pool_new';
procedure zpr_protobuf_pool_free(Handle: PDescriptorPool); cdecl; external ZPR_LIB name 'zpr_protobuf_pool_free';
function zpr_protobuf_json_to_binary(Pool: PDescriptorPool; MessageType, Json: PAnsiChar;
    out Output: TZprBuffer): Int32; cdecl; external ZPR_LIB name 'zpr_protobuf_json_to_binary';
function zpr_protobuf_binary_to_json(Pool: PDescriptorPool; MessageType: PAnsiChar;
    Data: PByte; DataLen: NativeUInt): PAnsiChar; cdecl; external ZPR_LIB name 'zpr_protobuf_binary_to_json';
function zpr_protobuf_binary_to_json_value(Pool: PDescriptorPool; MessageType: PAnsiChar;
    Data: PByte; DataLen: NativeUInt): PJsonValue; cdecl; external ZPR_LIB name 'zpr_protobuf_binary_to_json_value';
function zpr_protobuf_json_value_to_binary(Pool: PDescriptorPool; MessageType: PAnsiChar;
    Value: PJsonValue; out Output: TZprBuffer): Int32; cdecl; external ZPR_LIB name 'zpr_protobuf_json_value_to_binary';
function zpr_grpc_stream_read(Stream: PGrpcStream; out Output: TZprBuffer): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_stream_read';
function zpr_grpc_stream_write(Stream: PGrpcStream; Data: PByte; Len: NativeUInt): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_stream_write';
function zpr_grpc_server_start(BindAddr: PAnsiChar; Handler: TZprGrpcHandlerProc;
    UserData: Pointer; out OutHandle: PGrpcServerHandle): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_server_start';
function zpr_grpc_server_stop(Handle: PGrpcServerHandle): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_server_stop';
function zpr_grpc_server_start_queued(BindAddr: PAnsiChar; out OutHandle: PGrpcServerHandle;
    out OutQueue: PCallQueue): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_server_start_queued';
function zpr_grpc_accept(Queue: PCallQueue; out OutCallId: UInt64; OutMethod: PByte;
    OutMethodCap: NativeUInt; out OutMethodLen: NativeUInt): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_accept';
function zpr_grpc_call_read_into(Queue: PCallQueue; CallId: UInt64; Output: PByte;
    OutCap: NativeUInt; out OutLen: NativeUInt): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_call_read_into';
function zpr_grpc_call_write(Queue: PCallQueue; CallId: UInt64; Data: PByte; Len: NativeUInt): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_call_write';
function zpr_grpc_call_complete(Queue: PCallQueue; CallId: UInt64; GrpcStatus: Int32;
    Message: PAnsiChar): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_call_complete';
function zpr_grpc_queue_stats(Queue: PCallQueue; out OutPending: NativeUInt;
    out OutLive: NativeUInt): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_queue_stats';
procedure zpr_grpc_queue_free(Queue: PCallQueue); cdecl; external ZPR_LIB name 'zpr_grpc_queue_free';
function zpr_grpc_queue_configure(Queue: PCallQueue; MaxPending, DeadlineMs: UInt64): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_queue_configure';
function zpr_grpc_queue_counters(Queue: PCallQueue; out OutAccepted, OutCompleted,
    OutRefused, OutReaped: UInt64): Int32; cdecl; external ZPR_LIB name 'zpr_grpc_queue_counters';
function zpr_http_set_proxy(ProxyUrl: PAnsiChar): Int32; cdecl; external ZPR_LIB name 'zpr_http_set_proxy';
function zpr_http_request(Method, Url, HeadersJson: PAnsiChar; Body: PByte;
    BodyLen: NativeUInt; TimeoutMs: UInt32; out OutStatus: UInt16;
    out OutHeadersJson: PAnsiChar; out OutBody: TZprBuffer): Int32; cdecl; external ZPR_LIB name 'zpr_http_request';
function zpr_json_parse(Text: PAnsiChar): PJsonValue; cdecl; external ZPR_LIB name 'zpr_json_parse';
procedure zpr_json_free(Handle: PJsonValue); cdecl; external ZPR_LIB name 'zpr_json_free';
function zpr_json_stringify(Handle: PJsonValue; Pretty: Int32): PAnsiChar; cdecl; external ZPR_LIB name 'zpr_json_stringify';
function zpr_json_kind(Handle: PJsonValue): Int32; cdecl; external ZPR_LIB name 'zpr_json_kind';
function zpr_json_as_bool(Handle: PJsonValue; out Value: Byte): Int32; cdecl; external ZPR_LIB name 'zpr_json_as_bool';
function zpr_json_as_f64(Handle: PJsonValue; out Value: Double): Int32; cdecl; external ZPR_LIB name 'zpr_json_as_f64';
function zpr_json_as_string(Handle: PJsonValue): PAnsiChar; cdecl; external ZPR_LIB name 'zpr_json_as_string';
function zpr_json_array_len(Handle: PJsonValue): NativeInt; cdecl; external ZPR_LIB name 'zpr_json_array_len';
function zpr_json_array_get(Handle: PJsonValue; Index: NativeUInt): PJsonValue; cdecl; external ZPR_LIB name 'zpr_json_array_get';
function zpr_json_object_get(Handle: PJsonValue; Key: PAnsiChar): PJsonValue; cdecl; external ZPR_LIB name 'zpr_json_object_get';
function zpr_json_object_keys(Handle: PJsonValue): PAnsiChar; cdecl; external ZPR_LIB name 'zpr_json_object_keys';
function zpr_json_new_null: PJsonValue; cdecl; external ZPR_LIB name 'zpr_json_new_null';
function zpr_json_new_bool(B: Byte): PJsonValue; cdecl; external ZPR_LIB name 'zpr_json_new_bool';
function zpr_json_new_f64(N: Double): PJsonValue; cdecl; external ZPR_LIB name 'zpr_json_new_f64';
function zpr_json_new_string(S: PAnsiChar): PJsonValue; cdecl; external ZPR_LIB name 'zpr_json_new_string';
function zpr_json_new_array: PJsonValue; cdecl; external ZPR_LIB name 'zpr_json_new_array';
function zpr_json_new_object: PJsonValue; cdecl; external ZPR_LIB name 'zpr_json_new_object';
function zpr_json_array_push(Arr, Value: PJsonValue): Int32; cdecl; external ZPR_LIB name 'zpr_json_array_push';
function zpr_json_object_set(Obj: PJsonValue; Key: PAnsiChar; Value: PJsonValue): Int32; cdecl; external ZPR_LIB name 'zpr_json_object_set';
{$ENDIF}
{$ELSE}
  zpr_version: Tzpr_version;
  zpr_buffer_free: Tzpr_buffer_free;
  zpr_alloc: Tzpr_alloc;
  zpr_string_free: Tzpr_string_free;
  zpr_last_error: Tzpr_last_error;

  zpr_grpc_set_proxy: Tzpr_grpc_set_proxy;
  zpr_grpc_call: Tzpr_grpc_call;
  zpr_grpc_client_stream_open: Tzpr_grpc_client_stream_open;
  zpr_grpc_client_stream_read: Tzpr_grpc_client_stream_read;
  zpr_grpc_client_stream_cancel: Tzpr_grpc_client_stream_cancel;
  zpr_grpc_client_stream_read_into: Tzpr_grpc_client_stream_read_into;
  zpr_grpc_client_stream_buffer: Tzpr_grpc_client_stream_buffer;
  zpr_grpc_client_stream_stats: Tzpr_grpc_client_stream_stats;
  zpr_grpc_bidi_open: Tzpr_grpc_bidi_open;
  zpr_grpc_bidi_send: Tzpr_grpc_bidi_send;
  zpr_grpc_bidi_close_send: Tzpr_grpc_bidi_close_send;
  zpr_grpc_bidi_read_into: Tzpr_grpc_bidi_read_into;
  zpr_grpc_bidi_buffer: Tzpr_grpc_bidi_buffer;
  zpr_grpc_bidi_cancel: Tzpr_grpc_bidi_cancel;
  zpr_transcode_start: Tzpr_transcode_start;
  zpr_transcode_stop: Tzpr_transcode_stop;

  zpr_protobuf_pool_new: Tzpr_protobuf_pool_new;
  zpr_protobuf_pool_free: Tzpr_protobuf_pool_free;
  zpr_protobuf_json_to_binary: Tzpr_protobuf_json_to_binary;
  zpr_protobuf_binary_to_json: Tzpr_protobuf_binary_to_json;
  zpr_protobuf_binary_to_json_value: Tzpr_protobuf_binary_to_json_value;
  zpr_protobuf_json_value_to_binary: Tzpr_protobuf_json_value_to_binary;

  zpr_grpc_stream_read: Tzpr_grpc_stream_read;
  zpr_grpc_stream_write: Tzpr_grpc_stream_write;

  zpr_grpc_server_start: Tzpr_grpc_server_start;
  zpr_grpc_server_stop: Tzpr_grpc_server_stop;
  zpr_grpc_server_start_queued: Tzpr_grpc_server_start_queued;
  zpr_grpc_accept: Tzpr_grpc_accept;
  zpr_grpc_call_read_into: Tzpr_grpc_call_read_into;
  zpr_grpc_call_write: Tzpr_grpc_call_write;
  zpr_grpc_call_complete: Tzpr_grpc_call_complete;
  zpr_grpc_queue_stats: Tzpr_grpc_queue_stats;
  zpr_grpc_queue_free: Tzpr_grpc_queue_free;
  zpr_grpc_queue_configure: Tzpr_grpc_queue_configure;
  zpr_grpc_queue_counters: Tzpr_grpc_queue_counters;

  zpr_http_set_proxy: Tzpr_http_set_proxy;
  zpr_http_request: Tzpr_http_request;

  zpr_json_parse: Tzpr_json_parse;
  zpr_json_free: Tzpr_json_free;
  zpr_json_stringify: Tzpr_json_stringify;
  zpr_json_kind: Tzpr_json_kind;
  zpr_json_as_bool: Tzpr_json_as_bool;
  zpr_json_as_f64: Tzpr_json_as_f64;
  zpr_json_as_string: Tzpr_json_as_string;
  zpr_json_array_len: Tzpr_json_array_len;
  zpr_json_array_get: Tzpr_json_array_get;
  zpr_json_object_get: Tzpr_json_object_get;
  zpr_json_object_keys: Tzpr_json_object_keys;
  zpr_json_new_null: Tzpr_json_new_null;
  zpr_json_new_bool: Tzpr_json_new_bool;
  zpr_json_new_f64: Tzpr_json_new_f64;
  zpr_json_new_string: Tzpr_json_new_string;
  zpr_json_new_array: Tzpr_json_new_array;
  zpr_json_new_object: Tzpr_json_new_object;
  zpr_json_array_push: Tzpr_json_array_push;
  zpr_json_object_set: Tzpr_json_object_set;
{$ENDIF}

  // A permanently-allocated single NUL byte: a guaranteed-valid,
  // guaranteed-non-nil pointer to hand zpr for an empty string,
  // sidestepping the fact that casting a genuinely empty AnsiString/
  // UTF8String to PAnsiChar is not reliably non-nil across compiler
  // versions (an unassigned/empty string's internal pointer is nil).
  EmptyStringByte: Byte = 0;

function Utf8Ptr(const S: UTF8String): PAnsiChar;
begin
  if Length(S) = 0 then
    Result := PAnsiChar(@EmptyStringByte)
  else
    Result := PAnsiChar(S);
end;

/// For arguments the library treats as OPTIONAL, where NULL means "not given".
///
/// ⚠ AN EMPTY STRING IS NOT THE SAME AS ABSENT HERE. `Utf8Ptr('')` hands over a
/// pointer to an empty byte, which the Rust side reads as a present-but-empty
/// value and then fails to parse — metadata JSON of "" is invalid JSON, not
/// "no metadata". Optional arguments must be nil.
function Utf8PtrOrNil(const S: UTF8String): PAnsiChar;
begin
  if Length(S) = 0 then
    Result := nil
  else
    Result := PAnsiChar(S);
end;

{$IFNDEF ZPR_STATIC}
function Resolve(const AName: AnsiString): Pointer;
begin
  Result := DoGetProcAddress(GLib, AName);
  if Result = nil then
    raise ELoadError.CreateFmt('zpr: missing symbol "%s" (library version mismatch?)', [AName]);
end;

{$ENDIF}

{$IFDEF ZPR_STATIC}
/// Linked, not loaded: there is nothing to open and nothing to resolve. Kept so
/// a program written against the dynamic build compiles unchanged against this
/// one — the call is simply a no-op.
procedure Load(const ALibraryPath: string);
begin
end;
{$ELSE}
procedure Load(const ALibraryPath: string);
var
  Path: string;
begin
  if GLib <> InvalidLibHandle then
    Exit;

  if ALibraryPath <> '' then
    Path := ALibraryPath
  else
    Path := ExtractFilePath(ParamStr(0)) + DefaultLibraryFileName;

  GLib := DoLoadLibrary(Path);
  if GLib = InvalidLibHandle then
    GLib := DoLoadLibrary(DefaultLibraryFileName); // fall back to the OS search path
  if GLib = InvalidLibHandle then
    raise ELoadError.CreateFmt('zpr: could not load "%s"', [Path]);

  try
    zpr_version := Tzpr_version(Resolve('zpr_version'));
    zpr_buffer_free := Tzpr_buffer_free(Resolve('zpr_buffer_free'));
    zpr_alloc := Tzpr_alloc(Resolve('zpr_alloc'));
    zpr_string_free := Tzpr_string_free(Resolve('zpr_string_free'));
    zpr_last_error := Tzpr_last_error(Resolve('zpr_last_error'));

    zpr_grpc_set_proxy := Tzpr_grpc_set_proxy(Resolve('zpr_grpc_set_proxy'));
    zpr_grpc_call := Tzpr_grpc_call(Resolve('zpr_grpc_call'));
    zpr_grpc_client_stream_open := Tzpr_grpc_client_stream_open(Resolve('zpr_grpc_client_stream_open'));
    zpr_grpc_client_stream_read := Tzpr_grpc_client_stream_read(Resolve('zpr_grpc_client_stream_read'));
    zpr_grpc_client_stream_cancel := Tzpr_grpc_client_stream_cancel(Resolve('zpr_grpc_client_stream_cancel'));
    zpr_grpc_client_stream_read_into := Tzpr_grpc_client_stream_read_into(Resolve('zpr_grpc_client_stream_read_into'));
    zpr_grpc_client_stream_buffer := Tzpr_grpc_client_stream_buffer(Resolve('zpr_grpc_client_stream_buffer'));
    zpr_grpc_client_stream_stats := Tzpr_grpc_client_stream_stats(Resolve('zpr_grpc_client_stream_stats'));
    zpr_grpc_bidi_open := Tzpr_grpc_bidi_open(Resolve('zpr_grpc_bidi_open'));
    zpr_grpc_bidi_send := Tzpr_grpc_bidi_send(Resolve('zpr_grpc_bidi_send'));
    zpr_grpc_bidi_close_send := Tzpr_grpc_bidi_close_send(Resolve('zpr_grpc_bidi_close_send'));
    zpr_grpc_bidi_read_into := Tzpr_grpc_bidi_read_into(Resolve('zpr_grpc_bidi_read_into'));
    zpr_grpc_bidi_buffer := Tzpr_grpc_bidi_buffer(Resolve('zpr_grpc_bidi_buffer'));
    zpr_grpc_bidi_cancel := Tzpr_grpc_bidi_cancel(Resolve('zpr_grpc_bidi_cancel'));
    zpr_transcode_start := Tzpr_transcode_start(Resolve('zpr_transcode_start'));
    zpr_transcode_stop := Tzpr_transcode_stop(Resolve('zpr_transcode_stop'));

    zpr_protobuf_pool_new := Tzpr_protobuf_pool_new(Resolve('zpr_protobuf_pool_new'));
    zpr_protobuf_pool_free := Tzpr_protobuf_pool_free(Resolve('zpr_protobuf_pool_free'));
    zpr_protobuf_json_to_binary := Tzpr_protobuf_json_to_binary(Resolve('zpr_protobuf_json_to_binary'));
    zpr_protobuf_binary_to_json := Tzpr_protobuf_binary_to_json(Resolve('zpr_protobuf_binary_to_json'));
    zpr_protobuf_binary_to_json_value := Tzpr_protobuf_binary_to_json_value(Resolve('zpr_protobuf_binary_to_json_value'));
    zpr_protobuf_json_value_to_binary := Tzpr_protobuf_json_value_to_binary(Resolve('zpr_protobuf_json_value_to_binary'));

    zpr_grpc_stream_read := Tzpr_grpc_stream_read(Resolve('zpr_grpc_stream_read'));
    zpr_grpc_stream_write := Tzpr_grpc_stream_write(Resolve('zpr_grpc_stream_write'));

    zpr_grpc_server_start := Tzpr_grpc_server_start(Resolve('zpr_grpc_server_start'));
    zpr_grpc_server_stop := Tzpr_grpc_server_stop(Resolve('zpr_grpc_server_stop'));
    zpr_grpc_server_start_queued := Tzpr_grpc_server_start_queued(Resolve('zpr_grpc_server_start_queued'));
    zpr_grpc_accept := Tzpr_grpc_accept(Resolve('zpr_grpc_accept'));
    zpr_grpc_call_read_into := Tzpr_grpc_call_read_into(Resolve('zpr_grpc_call_read_into'));
    zpr_grpc_call_write := Tzpr_grpc_call_write(Resolve('zpr_grpc_call_write'));
    zpr_grpc_call_complete := Tzpr_grpc_call_complete(Resolve('zpr_grpc_call_complete'));
    zpr_grpc_queue_stats := Tzpr_grpc_queue_stats(Resolve('zpr_grpc_queue_stats'));
    zpr_grpc_queue_free := Tzpr_grpc_queue_free(Resolve('zpr_grpc_queue_free'));
    zpr_grpc_queue_configure := Tzpr_grpc_queue_configure(Resolve('zpr_grpc_queue_configure'));
    zpr_grpc_queue_counters := Tzpr_grpc_queue_counters(Resolve('zpr_grpc_queue_counters'));

    zpr_http_set_proxy := Tzpr_http_set_proxy(Resolve('zpr_http_set_proxy'));
    zpr_http_request := Tzpr_http_request(Resolve('zpr_http_request'));

    zpr_json_parse := Tzpr_json_parse(Resolve('zpr_json_parse'));
    zpr_json_free := Tzpr_json_free(Resolve('zpr_json_free'));
    zpr_json_stringify := Tzpr_json_stringify(Resolve('zpr_json_stringify'));
    zpr_json_kind := Tzpr_json_kind(Resolve('zpr_json_kind'));
    zpr_json_as_bool := Tzpr_json_as_bool(Resolve('zpr_json_as_bool'));
    zpr_json_as_f64 := Tzpr_json_as_f64(Resolve('zpr_json_as_f64'));
    zpr_json_as_string := Tzpr_json_as_string(Resolve('zpr_json_as_string'));
    zpr_json_array_len := Tzpr_json_array_len(Resolve('zpr_json_array_len'));
    zpr_json_array_get := Tzpr_json_array_get(Resolve('zpr_json_array_get'));
    zpr_json_object_get := Tzpr_json_object_get(Resolve('zpr_json_object_get'));
    zpr_json_object_keys := Tzpr_json_object_keys(Resolve('zpr_json_object_keys'));
    zpr_json_new_null := Tzpr_json_new_null(Resolve('zpr_json_new_null'));
    zpr_json_new_bool := Tzpr_json_new_bool(Resolve('zpr_json_new_bool'));
    zpr_json_new_f64 := Tzpr_json_new_f64(Resolve('zpr_json_new_f64'));
    zpr_json_new_string := Tzpr_json_new_string(Resolve('zpr_json_new_string'));
    zpr_json_new_array := Tzpr_json_new_array(Resolve('zpr_json_new_array'));
    zpr_json_new_object := Tzpr_json_new_object(Resolve('zpr_json_new_object'));
    zpr_json_array_push := Tzpr_json_array_push(Resolve('zpr_json_array_push'));
    zpr_json_object_set := Tzpr_json_object_set(Resolve('zpr_json_object_set'));
  except
    DoFreeLibrary(GLib);
    GLib := InvalidLibHandle;
    raise;
  end;
end;
{$ENDIF}

{$IFDEF ZPR_STATIC}
/// Nothing was opened, so nothing is closed. A linked library lives as long as
/// the process does.
procedure Unload;
begin
end;

/// Always true: the symbols were resolved by the linker before `main` ran.
function IsLoaded: Boolean;
begin
  Result := True;
end;
{$ELSE}
procedure Unload;
begin
  if GLib = InvalidLibHandle then
    Exit;
  DoFreeLibrary(GLib);
  GLib := InvalidLibHandle;
end;

function IsLoaded: Boolean;
begin
  Result := GLib <> InvalidLibHandle;
end;
{$ENDIF}

function Version: UTF8String;
begin
  Result := UTF8String(AnsiString(zpr_version));
end;

function LastError: UTF8String;
var
  P: PAnsiChar;
begin
  P := zpr_last_error();
  if P = nil then
    Result := ''
  else
    Result := UTF8String(AnsiString(P));
end;

procedure Check(AResult: Int32);
begin
  if AResult <> ZPR_OK then
    raise EError.Create(string(LastError));
end;

function BufferToBytes(const ABuf: TZprBuffer): TBytes;
begin
  SetLength(Result, ABuf.Len);
  if ABuf.Len > 0 then
    Move(ABuf.Data^, Result[0], ABuf.Len);
  zpr_buffer_free(ABuf);
end;

function TakeString(P: PAnsiChar): UTF8String;
begin
  if P = nil then
    Result := ''
  else
  begin
    Result := UTF8String(AnsiString(P));
    zpr_string_free(P);
  end;
end;

function BytesPtr(const B: TBytes): PByte;
begin
  if Length(B) = 0 then
    Result := nil
  else
    Result := PByte(@B[0]);
end;

// =======================================================================
// JSON
// =======================================================================

constructor TZprJson.CreateFromHandle(AHandle: PJsonValue; AOwnsHandle: Boolean);
begin
  inherited Create;
  FHandle := AHandle;
  FOwnsHandle := AOwnsHandle;
end;

destructor TZprJson.Destroy;
begin
  if FOwnsHandle and (FHandle <> nil) then
    zpr_json_free(FHandle);
  inherited Destroy;
end;

class function TZprJson.Parse(const AJson: UTF8String): TZprJson;
var
  H: PJsonValue;
begin
  H := zpr_json_parse(Utf8Ptr(AJson));
  if H = nil then
    raise EError.Create(string(LastError));
  Result := TZprJson.CreateFromHandle(H, True);
end;

class function TZprJson.NewNull: TZprJson;
begin
  Result := TZprJson.CreateFromHandle(zpr_json_new_null(), True);
end;

class function TZprJson.NewBool(AValue: Boolean): TZprJson;
begin
  Result := TZprJson.CreateFromHandle(zpr_json_new_bool(Ord(AValue)), True);
end;

class function TZprJson.NewFloat(AValue: Double): TZprJson;
begin
  Result := TZprJson.CreateFromHandle(zpr_json_new_f64(AValue), True);
end;

class function TZprJson.NewString(const AValue: UTF8String): TZprJson;
begin
  Result := TZprJson.CreateFromHandle(zpr_json_new_string(Utf8Ptr(AValue)), True);
end;

class function TZprJson.NewArray: TZprJson;
begin
  Result := TZprJson.CreateFromHandle(zpr_json_new_array(), True);
end;

class function TZprJson.NewObject: TZprJson;
begin
  Result := TZprJson.CreateFromHandle(zpr_json_new_object(), True);
end;

function TZprJson.GetKind: Integer;
begin
  Result := zpr_json_kind(FHandle);
end;

function TZprJson.GetAsBoolean: Boolean;
var
  V: Byte;
begin
  Check(zpr_json_as_bool(FHandle, V));
  Result := V <> 0;
end;

function TZprJson.GetAsFloat: Double;
var
  V: Double;
begin
  Check(zpr_json_as_f64(FHandle, V));
  Result := V;
end;

function TZprJson.GetAsString: UTF8String;
begin
  Result := TakeString(zpr_json_as_string(FHandle));
end;

function TZprJson.GetArrayLength: NativeInt;
begin
  Result := zpr_json_array_len(FHandle);
end;

function TZprJson.GetItem(AIndex: NativeUInt): TZprJson;
var
  H: PJsonValue;
begin
  H := zpr_json_array_get(FHandle, AIndex);
  if H = nil then
    raise EError.CreateFmt('zpr: no array element at index %d', [AIndex]);
  Result := TZprJson.CreateFromHandle(H, True);
end;

function TZprJson.GetField(const AKey: UTF8String): TZprJson;
var
  H: PJsonValue;
begin
  H := zpr_json_object_get(FHandle, Utf8Ptr(AKey));
  if H = nil then
    raise EError.CreateFmt('zpr: no object field "%s"', [AKey]);
  Result := TZprJson.CreateFromHandle(H, True);
end;

procedure TZprJson.Push(AValue: TZprJson);
begin
  Check(zpr_json_array_push(FHandle, AValue.FHandle));
  AValue.FOwnsHandle := False; // ownership transferred into the array
end;

procedure TZprJson.SetField(const AKey: UTF8String; AValue: TZprJson);
begin
  Check(zpr_json_object_set(FHandle, Utf8Ptr(AKey), AValue.FHandle));
  AValue.FOwnsHandle := False; // ownership transferred into the object
end;

function TZprJson.Keys: TArray<UTF8String>;
var
  KeysJson: UTF8String;
  Arr, Item: TZprJson;
  I: NativeInt;
begin
  KeysJson := TakeString(zpr_json_object_keys(FHandle));
  if KeysJson = '' then
    Exit(nil);
  Arr := TZprJson.Parse(KeysJson);
  try
    SetLength(Result, Arr.ArrayLength);
    for I := 0 to Arr.ArrayLength - 1 do
    begin
      Item := Arr.GetItem(I);
      try
        Result[I] := Item.AsString;
      finally
        Item.Free;
      end;
    end;
  finally
    Arr.Free;
  end;
end;

function TZprJson.ToJson(APretty: Boolean): UTF8String;
begin
  Result := TakeString(zpr_json_stringify(FHandle, Ord(APretty)));
end;

// =======================================================================
// HTTP client
// =======================================================================

class function TZprHttpClient.Request(const AMethod, AUrl: UTF8String;
  const AHeadersJson: UTF8String; const ABody: TBytes; ATimeoutMs: Cardinal;
  out AStatus: Word; out AResponseHeadersJson: UTF8String): TBytes;
var
  HeadersPtr: PAnsiChar;
  RespBody: TZprBuffer;
  RespHeaders: PAnsiChar;
  Rc: Int32;
begin
  if AHeadersJson = '' then
    HeadersPtr := nil
  else
    HeadersPtr := Utf8Ptr(AHeadersJson);

  Rc := zpr_http_request(Utf8Ptr(AMethod), Utf8Ptr(AUrl), HeadersPtr,
    BytesPtr(ABody), Length(ABody), ATimeoutMs, AStatus, RespHeaders, RespBody);
  Check(Rc);

  AResponseHeadersJson := TakeString(RespHeaders);
  Result := BufferToBytes(RespBody);
end;

class procedure TZprHttpClient.SetProxy(const AProxyUrl: UTF8String);
begin
  Check(zpr_http_set_proxy(Utf8Ptr(AProxyUrl)));
end;

class procedure TZprHttpClient.DisableProxy;
begin
  Check(zpr_http_set_proxy(nil));
end;

// =======================================================================
// Protobuf <-> JSON
// =======================================================================

destructor TZprProtobufPool.Destroy;
begin
  if FHandle <> nil then
    zpr_protobuf_pool_free(FHandle);
  inherited Destroy;
end;

class function TZprProtobufPool.LoadFromBytes(const ADescriptorSet: TBytes): TZprProtobufPool;
var
  H: PDescriptorPool;
begin
  H := zpr_protobuf_pool_new(BytesPtr(ADescriptorSet), Length(ADescriptorSet));
  if H = nil then
    raise EError.Create(string(LastError));
  Result := TZprProtobufPool.Create;
  Result.FHandle := H;
end;

class function TZprProtobufPool.LoadFromFile(const AFileName: string): TZprProtobufPool;
var
  Stream: TFileStream;
  Bytes: TBytes;
begin
  Stream := TFileStream.Create(AFileName, fmOpenRead or fmShareDenyWrite);
  try
    SetLength(Bytes, Stream.Size);
    if Stream.Size > 0 then
      Stream.ReadBuffer(Bytes[0], Stream.Size);
  finally
    Stream.Free;
  end;
  Result := TZprProtobufPool.LoadFromBytes(Bytes);
end;

function TZprProtobufPool.JsonToBinary(const AMessageType, AJson: UTF8String): TBytes;
var
  Buf: TZprBuffer;
begin
  Check(zpr_protobuf_json_to_binary(FHandle, Utf8Ptr(AMessageType), Utf8Ptr(AJson), Buf));
  Result := BufferToBytes(Buf);
end;

function TZprProtobufPool.BinaryToJson(const AMessageType: UTF8String; const AData: TBytes): UTF8String;
var
  P: PAnsiChar;
begin
  P := zpr_protobuf_binary_to_json(FHandle, Utf8Ptr(AMessageType), BytesPtr(AData), Length(AData));
  if P = nil then
    raise EError.Create(string(LastError));
  Result := TakeString(P);
end;

// =======================================================================
// gRPC client
// =======================================================================

class function TZprGrpcClient.Call(const AEndpoint, AMethodPath: UTF8String;
  const ARequest: TBytes; ATimeoutMs: Cardinal; out AGrpcStatus: Integer;
  const AMetadataJson: UTF8String): TBytes;
var
  MetaPtr: PAnsiChar;
  Resp: TZprBuffer;
  Rc: Int32;
begin
  if AMetadataJson = '' then
    MetaPtr := nil
  else
    MetaPtr := Utf8Ptr(AMetadataJson);
  Rc := zpr_grpc_call(Utf8Ptr(AEndpoint), Utf8Ptr(AMethodPath), MetaPtr, BytesPtr(ARequest),
    Length(ARequest), ATimeoutMs, Resp, AGrpcStatus);
  if Rc = GRPC_CALL_TRANSPORT_ERR then
    raise EError.Create(string(LastError));
  Result := BufferToBytes(Resp);
end;

class function TZprGrpcClient.OpenStream(const AEndpoint, AMethodPath: UTF8String;
  const ARequest: TBytes; ATimeoutMs: Cardinal; const AMetadataJson: UTF8String): TZprGrpcClientStream;
var
  MetaPtr: PAnsiChar;
  H: PGrpcClientStream;
  GrpcStatus: Int32;
begin
  if AMetadataJson = '' then
    MetaPtr := nil
  else
    MetaPtr := Utf8Ptr(AMetadataJson);
  Check(zpr_grpc_client_stream_open(Utf8Ptr(AEndpoint), Utf8Ptr(AMethodPath), MetaPtr,
    BytesPtr(ARequest), Length(ARequest), ATimeoutMs, H, GrpcStatus));
  Result := TZprGrpcClientStream.Create(H);
end;

constructor TZprGrpcClientStream.Create(AHandle: PGrpcClientStream);
begin
  inherited Create;
  FHandle := AHandle;
end;

destructor TZprGrpcClientStream.Destroy;
begin
  if FHandle <> nil then
    zpr_grpc_client_stream_cancel(FHandle);
  inherited Destroy;
end;

function TZprGrpcClientStream.Read(out AData: TBytes): Boolean;
var
  Buf: TZprBuffer;
  Rc: Int32;
begin
  Rc := zpr_grpc_client_stream_read(FHandle, Buf);
  case Rc of
    1: begin
         AData := BufferToBytes(Buf);
         Result := True;
       end;
    0: begin
         AData := nil;
         Result := False;
       end;
  else
    raise EError.Create(string(LastError));
  end;
end;

class procedure TZprGrpcClient.SetProxy(const AProxyUrl: UTF8String);
begin
  Check(zpr_grpc_set_proxy(Utf8Ptr(AProxyUrl)));
end;

class procedure TZprGrpcClient.DisableProxy;
begin
  Check(zpr_grpc_set_proxy(nil));
end;

// =======================================================================
// gRPC server
// =======================================================================

constructor TZprGrpcStream.Create(AHandle: PGrpcStream);
begin
  inherited Create;
  FHandle := AHandle;
end;

function TZprGrpcStream.Read(out AData: TBytes): Boolean;
var
  Buf: TZprBuffer;
  Rc: Int32;
begin
  Rc := zpr_grpc_stream_read(FHandle, Buf);
  case Rc of
    1: begin
         AData := BufferToBytes(Buf);
         Result := True;
       end;
    0: begin
         AData := nil;
         Result := False;
       end;
  else
    raise EError.Create(string(LastError));
  end;
end;

procedure TZprGrpcStream.Write(const AData: TBytes);
begin
  Check(zpr_grpc_stream_write(FHandle, BytesPtr(AData), Length(AData)));
end;

// The bridge between zpr's plain-C GrpcHandler callback and a Pascal
// method: user_data is always the TZprGrpcServer instance itself, so this
// standalone (non-method, therefore cdecl-compatible) routine just casts it
// back and forwards to FOnCall. A Pascal exception must never propagate
// back across the FFI boundary into Rust — the try/except turns any escape
// into a plain INTERNAL gRPC status instead.
procedure GrpcTrampoline(MethodPath: PAnsiChar; Stream: PGrpcStream; UserData: Pointer;
  out OutGrpcStatus: Int32; out OutMessage: PByte; out OutMessageLen: NativeUInt); cdecl;
var
  Server: TZprGrpcServer;
  StreamObj: TZprGrpcStream;
  Status: Integer;
  Msg: UTF8String;
begin
  OutMessage := nil;
  OutMessageLen := 0;
  Status := 13; // INTERNAL, overwritten below on the normal path
  Msg := '';
  try
    Server := TZprGrpcServer(UserData);
    StreamObj := TZprGrpcStream.Create(Stream);
    try
      if Assigned(Server.FOnCall) then
        Server.FOnCall(Server, UTF8String(AnsiString(MethodPath)), StreamObj, Status, Msg)
      else
        Status := 12; // UNIMPLEMENTED: no handler registered
    finally
      StreamObj.Free;
    end;
  except
    on E: Exception do
    begin
      Status := 13; // INTERNAL
      Msg := UTF8String(E.Message);
    end;
  end;

  OutGrpcStatus := Status;
  if Length(Msg) > 0 then
  begin
    OutMessageLen := Length(Msg);
    OutMessage := zpr_alloc(OutMessageLen);
    Move(Msg[1], OutMessage^, OutMessageLen);
  end;
end;

function TZprGrpcServer.GetIsRunning: Boolean;
begin
  Result := FHandle <> nil;
end;

destructor TZprGrpcServer.Destroy;
begin
  Stop;
  inherited Destroy;
end;

class function TZprGrpcServer.Start(const ABindAddr: UTF8String;
  AOnCall: TZprGrpcCallEvent): TZprGrpcServer;
var
  H: PGrpcServerHandle;
begin
  Result := TZprGrpcServer.Create;
  Result.FOnCall := AOnCall;
  Check(zpr_grpc_server_start(Utf8Ptr(ABindAddr), @GrpcTrampoline, Result, H));
  Result.FHandle := H;
end;

procedure TZprGrpcServer.Stop;
begin
  if FHandle = nil then
    Exit;
  zpr_grpc_server_stop(FHandle);
  FHandle := nil;
end;

function TZprProtobufPool.BinaryToJsonValue(const AMessageType: UTF8String;
  const AData: TBytes): TZprJson;
var
  H: PJsonValue;
begin
  H := zpr_protobuf_binary_to_json_value(FHandle, Utf8Ptr(AMessageType),
         BytesPtr(AData), Length(AData));
  if H = nil then
    raise EError.CreateFmt('zpr: %s', [string(LastError)]);
  Result := TZprJson.CreateFromHandle(H, True);
end;

function TZprProtobufPool.JsonValueToBinary(const AMessageType: UTF8String;
  AValue: TZprJson): TBytes;
var
  Buf: TZprBuffer;
begin
  if AValue = nil then
    raise EError.Create('zpr: nil JSON value passed to JsonValueToBinary');
  // The value is BORROWED — AValue still owns its handle and still frees it.
  Check(zpr_protobuf_json_value_to_binary(FHandle, Utf8Ptr(AMessageType), AValue.FHandle, Buf));
  Result := BufferToBytes(Buf);
end;

{ TZprGrpcClientStream — the buffered half }

function TZprGrpcClientStream.ReadInto(ABuffer: PByte; ACapacity: NativeUInt;
  out ALength: NativeUInt): TZprReadResult;
var
  Rc: Integer;
begin
  Rc := zpr_grpc_client_stream_read_into(FHandle, ABuffer, ACapacity, ALength);
  case Rc of
    1: Result := zrMessage;
    0: Result := zrNone;
    -2: Result := zrShortBuffer;
  else
    Result := zrError;
  end;
end;

procedure TZprGrpcClientStream.Buffer(ACapacity: NativeUInt);
begin
  Check(zpr_grpc_client_stream_buffer(FHandle, ACapacity));
  FBuffered := True;
end;

function TZprGrpcClientStream.GetDepth: NativeUInt;
var
  Dropped: UInt64;
begin
  Result := 0;
  Dropped := 0;
  zpr_grpc_client_stream_stats(FHandle, Result, Dropped);
end;

function TZprGrpcClientStream.GetDropped: UInt64;
var
  Depth: NativeUInt;
begin
  Result := 0;
  Depth := 0;
  zpr_grpc_client_stream_stats(FHandle, Depth, Result);
end;

{ TZprGrpcBidiStream }

class function TZprGrpcBidiStream.Open(const AEndpoint, AMethodPath: UTF8String;
  ASendCapacity: NativeUInt; ATimeoutMs: Cardinal; const AMetadataJson: UTF8String): TZprGrpcBidiStream;
var
  H: PGrpcBidiStream;
  Status: Integer;
begin
  H := nil;
  Status := 0;
  if zpr_grpc_bidi_open(Utf8Ptr(AEndpoint), Utf8Ptr(AMethodPath), Utf8PtrOrNil(AMetadataJson),
       ASendCapacity, ATimeoutMs, H, Status) <> 0 then
    raise EError.CreateFmt('zpr: could not open %s (grpc-status %d): %s',
      [string(AMethodPath), Status, string(LastError)]);
  Result := TZprGrpcBidiStream.Create;
  Result.FHandle := H;
end;

destructor TZprGrpcBidiStream.Destroy;
begin
  if FHandle <> nil then
    zpr_grpc_bidi_cancel(FHandle);
  FHandle := nil;
  inherited Destroy;
end;

function TZprGrpcBidiStream.Send(const AData: TBytes): Boolean;
var
  Rc: Integer;
begin
  Rc := zpr_grpc_bidi_send(FHandle, BytesPtr(AData), Length(AData));
  if Rc < 0 then
    raise EError.CreateFmt('zpr: send failed: %s', [string(LastError)]);
  Result := Rc = 1;
end;

procedure TZprGrpcBidiStream.CloseSend;
begin
  if FSendClosed then
    Exit;
  Check(zpr_grpc_bidi_close_send(FHandle));
  FSendClosed := True;
end;

function TZprGrpcBidiStream.ReadInto(ABuffer: PByte; ACapacity: NativeUInt;
  out ALength: NativeUInt): TZprReadResult;
var
  Rc: Integer;
begin
  Rc := zpr_grpc_bidi_read_into(FHandle, ABuffer, ACapacity, ALength);
  case Rc of
    1: Result := zrMessage;
    0: Result := zrNone;
    -2: Result := zrShortBuffer;
  else
    Result := zrError;
  end;
end;

procedure TZprGrpcBidiStream.Buffer(ACapacity: NativeUInt);
begin
  Check(zpr_grpc_bidi_buffer(FHandle, ACapacity));
end;

{ TZprGrpcCall }

constructor TZprGrpcCall.Create(AQueue: PCallQueue; AId: UInt64; const AMethodPath: UTF8String);
begin
  inherited Create;
  FQueue := AQueue;
  FId := AId;
  FMethodPath := AMethodPath;
end;

destructor TZprGrpcCall.Destroy;
begin
  // 14 = UNAVAILABLE. See the note on the declaration: this is a backstop, not
  // a licence to skip Complete.
  if not FCompleted then
    Complete(14, 'the handler ended without completing this call');
  inherited Destroy;
end;

function TZprGrpcCall.Read(out AData: TBytes): TZprReadResult;
var
  Buf: TBytes;
  Got: NativeUInt;
  Rc: Integer;
begin
  SetLength(AData, 0);
  // 64 KiB covers any ordinary request; a short buffer HOLDS the message, so the
  // grow-and-retry below cannot lose one.
  SetLength(Buf, 65536);
  Got := 0;
  repeat
    Rc := zpr_grpc_call_read_into(FQueue, FId, BytesPtr(Buf), Length(Buf), Got);
    if Rc = -2 then
      SetLength(Buf, Got); // exactly what it asked for, then read again
  until Rc <> -2;
  case Rc of
    1:
      begin
        SetLength(AData, Got);
        if Got > 0 then
          Move(Buf[0], AData[0], Got);
        Result := zrMessage;
      end;
    0: Result := zrNone;
    2: Result := zrClientDone;
  else
    Result := zrError;
  end;
end;

function TZprGrpcCall.Write(const AData: TBytes): Boolean;
var
  Rc: Integer;
begin
  Rc := zpr_grpc_call_write(FQueue, FId, BytesPtr(AData), Length(AData));
  if Rc < 0 then
    raise EError.CreateFmt('zpr: write failed: %s', [string(LastError)]);
  Result := Rc = 1;
end;

procedure TZprGrpcCall.Complete(AStatus: Integer; const AMessage: UTF8String);
begin
  if FCompleted then
    Exit;
  FCompleted := True;
  zpr_grpc_call_complete(FQueue, FId, AStatus, Utf8PtrOrNil(AMessage));
end;

{ TZprGrpcQueuedServer }

class function TZprGrpcQueuedServer.Start(const ABindAddr: UTF8String): TZprGrpcQueuedServer;
var
  H: PGrpcServerHandle;
  Q: PCallQueue;
begin
  H := nil;
  Q := nil;
  Check(zpr_grpc_server_start_queued(Utf8Ptr(ABindAddr), H, Q));
  Result := TZprGrpcQueuedServer.Create;
  Result.FHandle := H;
  Result.FQueue := Q;
end;

destructor TZprGrpcQueuedServer.Destroy;
begin
  Stop;
  inherited Destroy;
end;

function TZprGrpcQueuedServer.Accept: TZprGrpcCall;
var
  CallId: UInt64;
  Method: TBytes;
  MethodLen: NativeUInt;
  Rc: Integer;
begin
  Result := nil;
  if FQueue = nil then
    Exit;
  CallId := 0;
  MethodLen := 0;
  SetLength(Method, 512);
  repeat
    Rc := zpr_grpc_accept(FQueue, CallId, BytesPtr(Method), Length(Method), MethodLen);
    if Rc = -2 then
      SetLength(Method, MethodLen); // the RPC stays queued; retry at the right size
  until Rc <> -2;
  if Rc <> 1 then
    Exit;
  SetLength(Method, MethodLen);
  Result := TZprGrpcCall.Create(FQueue, CallId, UTF8String(PAnsiChar(BytesPtr(Method))));
end;

procedure TZprGrpcQueuedServer.Stop;
begin
  if FHandle <> nil then
  begin
    zpr_grpc_server_stop(FHandle);
    FHandle := nil;
  end;
  if FQueue <> nil then
  begin
    zpr_grpc_queue_free(FQueue);
    FQueue := nil;
  end;
end;

function TZprGrpcQueuedServer.GetPending: NativeUInt;
var
  Live: NativeUInt;
begin
  Result := 0;
  Live := 0;
  if FQueue <> nil then
    zpr_grpc_queue_stats(FQueue, Result, Live);
end;

function TZprGrpcQueuedServer.GetLive: NativeUInt;
var
  Pending: NativeUInt;
begin
  Result := 0;
  Pending := 0;
  if FQueue <> nil then
    zpr_grpc_queue_stats(FQueue, Pending, Result);
end;

procedure TZprGrpcQueuedServer.Configure(AMaxPending: UInt64; ADeadlineMs: UInt64);
begin
  if FQueue = nil then
    Exit;
  Check(zpr_grpc_queue_configure(FQueue, AMaxPending, ADeadlineMs));
end;

procedure TZprGrpcQueuedServer.Counters(out AAccepted, ACompleted, ARefused, AReaped: UInt64);
begin
  AAccepted := 0; ACompleted := 0; ARefused := 0; AReaped := 0;
  if FQueue = nil then
    Exit;
  zpr_grpc_queue_counters(FQueue, AAccepted, ACompleted, ARefused, AReaped);
end;

{ TZprTranscoder }

class function TZprTranscoder.Start(const ABindAddr, AUpstream: UTF8String;
  APool: TZprProtobufPool; ATimeoutMs: Cardinal): TZprTranscoder;
var
  H: PTranscodeHandle;
begin
  if APool = nil then
    raise EError.Create('zpr: the transcoder routes by descriptor and needs a pool');
  H := nil;
  Check(zpr_transcode_start(Utf8Ptr(ABindAddr), Utf8Ptr(AUpstream), APool.Handle, ATimeoutMs, H));
  Result := TZprTranscoder.Create;
  Result.FHandle := H;
end;

destructor TZprTranscoder.Destroy;
begin
  Stop;
  inherited Destroy;
end;

procedure TZprTranscoder.Stop;
begin
  if FHandle = nil then
    Exit;
  zpr_transcode_stop(FHandle);
  FHandle := nil;
end;


initialization

finalization
  Unload;

end.
