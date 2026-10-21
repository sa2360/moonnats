///|
struct Endpoint {
  host : String
  port : Int
}

///|
struct SubscriptionSpec {
  subject : String
  queue_group : String?
  handler : async (@moonnats.DeliveredMsg) -> Unit
}

///|
/// A supervised client that reconnects after transport failures and restores
/// subscriptions registered through this value. Call `run` from the task
/// group coordinator, then use publish/subscribe/unsubscribe concurrently.
pub struct ReconnectingClient {
  priv options : Options
  priv lock : @async.Mutex
  priv servers : Array[Endpoint]
  priv subscriptions : Map[String, SubscriptionSpec]
  priv mut session_sids : Map[String, String]
  priv mut next_sid : Int
  priv mut current : Conn?
  priv stop_signal : @async.Queue[Unit]
}

///|
pub fn ReconnectingClient::new(
  options? : Options = Options::default(),
) -> ReconnectingClient {
  {
    options,
    lock: @async.Mutex(),
    servers: [{ host: options.host, port: options.port, }],
    subscriptions: Map([]),
    session_sids: Map([]),
    next_sid: 0,
    current: None,
    stop_signal: @async.Queue(kind=Unbounded),
  }
}

///|
/// Supervise the connection until `close` is called. This method is the task
/// group coordinator because it installs each connection's service loops.
pub async fn[G] ReconnectingClient::run(
  self : ReconnectingClient,
  group : @async.TaskGroup[G],
) -> Unit {
  let mut next_server = 0
  let mut should_stop = false
  while !should_stop {
    if self.take_stop_signal() {
      return
    }
    let endpoint = self.next_endpoint(next_server)
    next_server += 1
    let dial_options = {
      ..self.options,
      host: endpoint.host,
      port: endpoint.port,
    }
    match connect_socket(dial_options) {
      Err(_) => should_stop = self.wait_retry()
      Ok(conn) =>
        match handshake_with_deadline(group, conn) {
          Err(e) => {
            let _ = conn.fail(e.message())
            should_stop = self.wait_retry()
          }
          Ok(_) => {
            let restored = self.restore_subscriptions(conn)
            match restored {
              Err(e) => {
                let _ = conn.fail(e.message())
                self.clear_current(conn)
                should_stop = self.wait_retry()
              }
              Ok(_) => {
                self.add_discovered(conn.server_info())
                conn.spawn_loops(group)
                should_stop = self.wait_connection(conn)
                self.clear_current(conn)
                if !should_stop {
                  should_stop = self.wait_retry()
                }
              }
            }
          }
        }
    }
  }
}

///|
async fn ReconnectingClient::next_endpoint(
  self : ReconnectingClient,
  index : Int,
) -> Endpoint {
  self.lock.acquire()
  if self.servers.is_empty() {
    self.servers.push({ host: self.options.host, port: self.options.port, })
  }
  let endpoint = self.servers[index % self.servers.length()]
  self.lock.release()
  endpoint
}

///|
async fn ReconnectingClient::wait_retry(self : ReconnectingClient) -> Bool {
  let delay = self.options.reconnect_delay_ms
  let rounds = if delay <= 0 { 1 } else { delay / 20 + 1 }
  for _ in 0..<rounds {
    if self.take_stop_signal() {
      return true
    }
    @async.sleep(20)
  }
  self.take_stop_signal()
}

///|
async fn ReconnectingClient::restore_subscriptions(
  self : ReconnectingClient,
  conn : Conn,
) -> Result[Unit, NatsError] {
  self.lock.acquire()
  self.current = Some(conn)
  self.session_sids = Map([])
  for stable_sid, spec in self.subscriptions {
    let subscribed = match spec.queue_group {
      Some(queue_group) =>
        conn.subscribe(spec.subject, spec.handler, queue_group~)
      None => conn.subscribe(spec.subject, spec.handler)
    }
    match subscribed {
      Err(e) => {
        self.current = None
        self.session_sids = Map([])
        self.lock.release()
        return Err(e)
      }
      Ok(session_sid) => self.session_sids[stable_sid] = session_sid
    }
  }
  self.lock.release()
  Ok(())
}

///|
async fn ReconnectingClient::clear_current(
  self : ReconnectingClient,
  conn : Conn,
) -> Unit {
  self.lock.acquire()
  match self.current {
    Some(active) =>
      if physical_equal(active, conn) {
        self.current = None
        self.session_sids = Map([])
      }
    None => ()
  }
  self.lock.release()
}

///|
async fn ReconnectingClient::wait_connection(
  self : ReconnectingClient,
  conn : Conn,
) -> Bool {
  while conn.is_open() {
    if self.take_stop_signal() {
      return true
    }
    let update = conn.info_updates.try_get() catch { _ => None }
    match update {
      Some(info) => self.add_discovered(Some(info))
      None => ()
    }
    @async.sleep(20)
  }
  self.take_stop_signal()
}

///|
fn ReconnectingClient::take_stop_signal(self : ReconnectingClient) -> Bool {
  try (self.stop_signal.try_get() is Some(_)) catch {
    _ => false
  }
}

///|
async fn ReconnectingClient::add_discovered(
  self : ReconnectingClient,
  info : @moonnats.ServerInfo?,
) -> Unit {
  match info {
    None => ()
    Some(info) => {
      self.lock.acquire()
      for url in info.connect_urls {
        match parse_endpoint(url) {
          Some(endpoint) => {
            let mut exists = false
            for known in self.servers {
              if known.host == endpoint.host && known.port == endpoint.port {
                exists = true
              }
            }
            if !exists {
              self.servers.push(endpoint)
            }
          }
          None => ()
        }
      }
      self.lock.release()
    }
  }
}

///|
fn parse_endpoint(value : String) -> Endpoint? {
  let address = match value.split_once("://") {
    Some((_, rest)) => rest
    None => value
  }
  let address = match address.split_once("/") {
    Some((host_port, _)) => host_port
    None => address
  }
  if address.has_prefix("[") {
    match address.split_once("]:") {
      Some((host, port_text)) =>
        endpoint_from_parts(host[1:].to_owned(), port_text.to_owned())
      None => None
    }
  } else {
    match address.split_once(":") {
      Some((host, port_text)) =>
        endpoint_from_parts(host.to_owned(), port_text.to_owned())
      None => None
    }
  }
}

///|
fn endpoint_from_parts(host : String, port_text : String) -> Endpoint? {
  let json = "{\"server_id\":\"discovery\",\"version\":\"0\",\"host\":\"" +
    host +
    "\",\"port\":" +
    port_text +
    "}"
  match @moonnats.parse_server_info(json) {
    Ok(info) => Some({ host: info.host, port: info.port, })
    Err(_) => None
  }
}

///|
/// Register a subscription that will be restored on every new connection.
pub async fn ReconnectingClient::subscribe(
  self : ReconnectingClient,
  subject : String,
  handler : async (@moonnats.DeliveredMsg) -> Unit,
  queue_group? : String,
) -> Result[String, NatsError] {
  guard @moonnats.is_valid_subject(subject, wildcards=true) else {
    return Err(BadRequest("invalid subscription subject: \{subject}"))
  }
  self.lock.acquire()
  self.next_sid += 1
  let sid = "client-" + self.next_sid.to_string()
  let spec = SubscriptionSpec::{ subject, queue_group, handler, }
  self.subscriptions[sid] = spec
  let result = match self.current {
    Some(conn) => {
      let subscribed = match queue_group {
        Some(group) => conn.subscribe(subject, handler, queue_group=group)
        None => conn.subscribe(subject, handler)
      }
      match subscribed {
        Ok(session_sid) => {
          self.session_sids[sid] = session_sid
          Ok(sid)
        }
        Err(e) => {
          let _ = self.subscriptions.remove(sid)
          Err(e)
        }
      }
    }
    None => Ok(sid)
  }
  self.lock.release()
  result
}

///|
/// Remove a subscription from the recovery set and the current connection.
pub async fn ReconnectingClient::unsubscribe(
  self : ReconnectingClient,
  sid : String,
) -> Result[Unit, NatsError] {
  self.lock.acquire()
  let _ = self.subscriptions.remove(sid)
  let session_sid = self.session_sids.get(sid)
  let _ = self.session_sids.remove(sid)
  let current = self.current
  self.lock.release()
  match (current, session_sid) {
    (Some(conn), Some(active_sid)) => conn.unsubscribe(active_sid)
    _ => Ok(())
  }
}

///|
/// Publish using the current connection; returns ConnectionClosed while the
/// supervisor is reconnecting. Publishes are not silently buffered.
pub async fn ReconnectingClient::publish(
  self : ReconnectingClient,
  subject : String,
  payload : Bytes,
) -> Result[Unit, NatsError] {
  self.lock.acquire()
  let current = self.current
  self.lock.release()
  match current {
    Some(conn) => conn.publish(subject, payload)
    None => Err(ConnectionClosed("reconnecting"))
  }
}

///|
/// Wait for all commands sent on the current connection to reach the server.
pub async fn ReconnectingClient::flush(
  self : ReconnectingClient,
  timeout_ms? : Int,
) -> Result[Unit, NatsError] {
  self.lock.acquire()
  let current = self.current
  self.lock.release()
  match current {
    Some(conn) =>
      match timeout_ms {
        Some(timeout) => conn.flush(timeout_ms=timeout)
        None => conn.flush()
      }
    None => Err(ConnectionClosed("reconnecting"))
  }
}

///|
/// Send a request on the current connection. Requests cannot be resumed if
/// the connection drops while waiting for the reply.
pub async fn ReconnectingClient::request(
  self : ReconnectingClient,
  subject : String,
  payload : Bytes,
  timeout_ms? : Int,
) -> Result[Bytes, NatsError] {
  self.lock.acquire()
  let current = self.current
  self.lock.release()
  match current {
    Some(conn) =>
      match timeout_ms {
        Some(timeout) => conn.request(subject, payload, timeout_ms=timeout)
        None => conn.request(subject, payload)
      }
    None => Err(ConnectionClosed("reconnecting"))
  }
}

///|
/// Stop reconnecting and close the active connection.
pub async fn ReconnectingClient::close(self : ReconnectingClient) -> Unit {
  self.stop_signal.put(()) catch {
    _ => ()
  }
  self.lock.acquire()
  let conn = self.current
  self.current = None
  self.lock.release()
  match conn {
    Some(active) => {
      let _ = active.close()
    }
    None => ()
  }
}

///|
/// True when the currently supervised transport is ready.
pub fn ReconnectingClient::is_connected(self : ReconnectingClient) -> Bool {
  match self.current {
    Some(conn) => conn.is_open()
    None => false
  }
}
