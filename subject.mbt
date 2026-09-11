///|
/// Convert a character to the UTF-16 code unit `String` indexing yields.
fn u16(ch : Char) -> UInt16 {
  ch.to_int().to_uint16()
}

///|
/// Validate a NATS subject.
///
/// A subject is dot-separated tokens of printable ASCII characters without
/// spaces. Tokens must be non-empty, so leading/trailing dots and `..` are
/// invalid.
///
/// When `wildcards` is false (publish subjects) neither `*` nor `>` may
/// appear. When true, `*` must form an entire token and `>` must be the
/// final token.
pub fn is_valid_subject(subject : String, wildcards? : Bool = false) -> Bool {
  if subject.is_empty() {
    return false
  }
  let len = subject.length()
  let mut i = 0
  let mut token_len = 0
  while i < len {
    let ch = subject[i]
    if ch == u16('.') {
      if token_len == 0 {
        return false
      }
      token_len = 0
    } else if ch == u16('*') {
      if !wildcards || token_len != 0 {
        return false
      }
      token_len = 1
    } else if ch == u16('>') {
      if !wildcards || token_len != 0 || i + 1 != len {
        return false
      }
      token_len = 1
    } else if ch < u16('!') || ch > u16('~') {
      return false
    } else {
      token_len += 1
    }
    i += 1
  }
  token_len > 0
}

///|
/// Report whether a subscription subject (wildcards allowed) matches a
/// concrete publish subject (wildcards forbidden).
///
/// `*` matches exactly one token; a trailing `>` matches one or more
/// remaining tokens, so `foo.>` does not match `foo`. Neither input is
/// validated here; pair with `is_valid_subject` at the API boundary.
pub fn subject_matches(subscription : String, subject : String) -> Bool {
  match_tokens(subscription, 0, subject, 0)
}

///|
fn match_tokens(
  subscription : String,
  sub_start : Int,
  subject : String,
  subj_start : Int,
) -> Bool {
  let mut s = sub_start
  let mut j = subj_start
  while true {
    let sub_end = token_end(subscription, s)
    let sub_len = subscription.length()
    if is_wildcard_token(subscription, s, sub_end, '>') {
      // A valid '>' is the last subscription token and must find at least
      // one remaining subject token to absorb.
      return sub_end == sub_len && j < subject.length()
    }
    if j >= subject.length() {
      // Subject exhausted while the subscription still has a concrete token.
      return false
    }
    let subj_end = token_end(subject, j)
    let matched = if is_wildcard_token(subscription, s, sub_end, '*') {
      true
    } else {
      sub_end - s == subj_end - j &&
      token_equal(subscription, s, sub_end, subject, j)
    }
    if !matched {
      return false
    }
    s = sub_end + 1
    j = subj_end + 1
    if sub_end >= sub_len {
      // Subscription exhausted: match only if the subject has no token left.
      return j > subject.length()
    }
  }
  false
}

///|
fn is_wildcard_token(
  s : String,
  start : Int,
  end : Int,
  wildcard : Char,
) -> Bool {
  end - start == 1 && s[start] == u16(wildcard)
}

///|
/// End index (exclusive) of the token starting at `start`, i.e. the position
/// of the next '.' or the string length.
fn token_end(s : String, start : Int) -> Int {
  let mut i = start
  while i < s.length() && s[i] != u16('.') {
    i += 1
  }
  i
}

///|
fn token_equal(
  a : String,
  a_start : Int,
  a_end : Int,
  b : String,
  b_start : Int,
) -> Bool {
  let mut i = a_start
  let mut k = b_start
  while i < a_end {
    if a[i] != b[k] {
      return false
    }
    i += 1
    k += 1
  }
  true
}
