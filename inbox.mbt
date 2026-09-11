///|
/// Generates unique reply subjects for request-reply calls, following the
/// `_INBOX.<id>.<counter>` convention used by nats.go. One generator per
/// connection: the prefix identifies the connection (the client supplies a
/// random id at startup) and the counter disambiguates in-flight requests.
pub struct InboxGen {
  priv prefix : String
  priv mut counter : Int
} derive(Eq, Debug)

///|
pub fn InboxGen::new(prefix? : String = "0") -> InboxGen {
  { prefix, counter: 0, }
}

///|
/// Next inbox subject; the counter restarts at 1.
pub fn InboxGen::next(self : InboxGen) -> String {
  self.counter += 1
  "_INBOX." + self.prefix + "." + self.counter.to_string()
}
