///|
test "inbox generator produces unique ascending subjects" {
  let gen = @moonnats.InboxGen::new(prefix="k3Ab2")
  let first = gen.next()
  let second = gen.next()
  assert_eq(first, "_INBOX.k3Ab2.1")
  assert_eq(second, "_INBOX.k3Ab2.2")
  assert_not_eq(first, second)
  // Generated inboxes must be valid subscription subjects.
  assert_true(@moonnats.is_valid_subject(first, wildcards=true))
}

///|
test "inbox generator is isolated per instance" {
  let a = @moonnats.InboxGen::new(prefix="a")
  let b = @moonnats.InboxGen::new(prefix="b")
  assert_eq(a.next(), "_INBOX.a.1")
  assert_eq(b.next(), "_INBOX.b.1")
}
