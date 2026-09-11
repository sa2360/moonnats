///|
/// The JSON document sent as the `CONNECT` argument after receiving the
/// server's `INFO` line. Field names and defaults follow the NATS protocol
/// spec; unknown-to-the-server fields are ignored by it, and the server
/// echoes the options it will honor in its next `INFO`.
pub(all) struct ConnectConfig {
  /// Set by the client when a reply subject is expected on every message.
  echo : Bool
  /// Advertise a subject this client listens on for no_responders tracking.
  headers : Bool
  /// Protocol version the client speaks; 1 is the only version today.
  protocol : Int
  /// Request `+OK` for every accepted command.
  verbose : Bool
  /// Request delivery of the client's own published messages back to it.
  no_echo : Bool
  /// Username / password auth (PLAIN).
  user : String?
  pass : String?
  /// Token auth.
  auth_token : String?
  /// Client name shown in server monitoring.
  name : String?
  /// Client language and version, e.g. ("moonbit", "0.1.0").
  lang : String
  version : String
} derive(Eq, Show)

///|
/// Defaults matching nats.go's behavior: echo on, headers on, protocol 1,
/// verbose off, no_echo off.
pub fn ConnectConfig::default(name? : String? = None) -> ConnectConfig {
  {
    echo: true,
    headers: true,
    protocol: 1,
    verbose: false,
    no_echo: false,
    user: None,
    pass: None,
    auth_token: None,
    name,
    lang: "moonbit",
    version: "0.1.0",
  }
}

///|
/// Serialize to the JSON object expected after the CONNECT verb. Optional
/// credentials are omitted entirely rather than sent as empty strings, as
/// nats.go does.
pub fn ConnectConfig::to_json(self : ConnectConfig) -> Json {
  let m : Map[String, Json] = Map::new()
  m["echo"] = Json::boolean(self.echo)
  m["headers"] = Json::boolean(self.headers)
  m["protocol"] = Json::number(Double::from_int(self.protocol))
  m["verbose"] = Json::boolean(self.verbose)
  m["no_echo"] = Json::boolean(self.no_echo)
  match self.user {
    Some(u) => m["user"] = Json::string(u)
    None => ()
  }
  match self.pass {
    Some(p) => m["pass"] = Json::string(p)
    None => ()
  }
  match self.auth_token {
    Some(t) => m["auth_token"] = Json::string(t)
    None => ()
  }
  match self.name {
    Some(n) => m["name"] = Json::string(n)
    None => ()
  }
  m["lang"] = Json::string(self.lang)
  m["version"] = Json::string(self.version)
  Json::object(m)
}

///|
/// The full CONNECT command bytes for this config.
pub fn ConnectConfig::encode(self : ConnectConfig) -> Bytes {
  encode_command(Connect(self.to_json().stringify()))
}
