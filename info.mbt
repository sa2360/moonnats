///|
/// The server information advertised in the `INFO` handshake line and on
/// every cluster topology change. Fields the client acts on are decoded;
/// the rest is available through `raw`.
pub(all) struct ServerInfo {
  server_id : String
  server_name : String
  version : String
  host : String
  port : Int
  /// True when the server requires authentication.
  auth_required : Bool
  /// True when the server supports HMSG headers.
  headers : Bool
  /// Maximum protocol payload size in bytes; 0 means unlimited.
  max_payload : Int
  /// Minimum server version supporting headers, as advertised.
  proto : Int
} derive(Eq, Debug)

///|
/// Decode the JSON argument of an `INFO` operation. Returns an error when
/// the JSON is malformed or missing the fields the client cannot proceed
/// without (`server_id`, `version`, `host`, `port`).
pub fn parse_server_info(json : String) -> Result[ServerInfo, String] {
  let value = @json.parse(json) catch {
    e => return Err("invalid INFO json: \u{22}\{e}\u{22}")
  }
  guard value is Object(fields) else {
    return Err("INFO json is not an object")
  }
  let server_id = str_field(fields, "server_id")
  let version = str_field(fields, "version")
  let host = str_field(fields, "host")
  let port = int_field(fields, "port")
  if server_id.is_empty() || version.is_empty() {
    return Err("INFO json missing server_id or version")
  }
  let port = match port {
    Ok(p) => p
    Err(msg) => return Err(msg)
  }
  Ok({
    server_id,
    server_name: str_field(fields, "server_name"),
    version,
    host,
    port,
    auth_required: bool_field(fields, "auth_required"),
    headers: bool_field(fields, "headers"),
    max_payload: match int_field(fields, "max_payload") {
      Ok(n) => n
      Err(_) => 0
    },
    proto: match int_field(fields, "proto") {
      Ok(n) => n
      Err(_) => 1
    },
  })
}

///|
/// Field readers tolerate absent keys with typed defaults; only fields with
/// the wrong JSON type are errors.
///
fn str_field(fields : Map[String, Json], key : String) -> String {
  match fields.get(key) {
    Some(Json::String(s)) => s
    _ => ""
  }
}

///|
fn bool_field(fields : Map[String, Json], key : String) -> Bool {
  match fields.get(key) {
    Some(Json::True) => true
    Some(Json::False) => false
    _ => false
  }
}

///|
fn int_field(fields : Map[String, Json], key : String) -> Result[Int, String] {
  match fields.get(key) {
    None => Ok(0)
    Some(Json::Number(n, ..)) => Ok(n.to_int())
    Some(_) => Err("INFO field \u{22}" + key + "\u{22} is not a number")
  }
}

///|
/// Decode the `INFO` carried by a parsed `ServerOp::Info`.
pub fn server_info_from_op(op : ServerOp) -> Result[ServerInfo, String] {
  guard op is Info(json) else { return Err("not an INFO operation") }
  parse_server_info(json)
}
