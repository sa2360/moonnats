///|
test "is_valid_subject accepts plain dotted subjects" {
  assert_eq(@moonnats.is_valid_subject("foo"), true)
  assert_eq(@moonnats.is_valid_subject("foo.bar"), true)
  assert_eq(@moonnats.is_valid_subject("a.b.c.d.e"), true)
  assert_eq(@moonnats.is_valid_subject("ORDERS.new.us-east_1"), true)
}

///|
test "is_valid_subject rejects malformed subjects" {
  assert_eq(@moonnats.is_valid_subject(""), false)
  assert_eq(@moonnats.is_valid_subject(".foo"), false)
  assert_eq(@moonnats.is_valid_subject("foo."), false)
  assert_eq(@moonnats.is_valid_subject("foo..bar"), false)
  assert_eq(@moonnats.is_valid_subject("foo bar"), false)
  assert_eq(@moonnats.is_valid_subject("foo\nbar"), false)
  assert_eq(@moonnats.is_valid_subject("héllo"), false)
}

///|
test "is_valid_subject forbids wildcards for publishing" {
  assert_eq(@moonnats.is_valid_subject("foo.*"), false)
  assert_eq(@moonnats.is_valid_subject("foo.>"), false)
  assert_eq(@moonnats.is_valid_subject("*"), false)
  assert_eq(@moonnats.is_valid_subject(">"), false)
}

///|
test "is_valid_subject enforces wildcard placement" {
  assert_eq(@moonnats.is_valid_subject("foo.*", wildcards=true), true)
  assert_eq(@moonnats.is_valid_subject("foo.>", wildcards=true), true)
  assert_eq(@moonnats.is_valid_subject("*", wildcards=true), true)
  assert_eq(@moonnats.is_valid_subject(">", wildcards=true), true)
  // '*' fused with other characters, '>' not last, and embedded '>' are invalid.
  assert_eq(@moonnats.is_valid_subject("fo*o", wildcards=true), false)
  assert_eq(@moonnats.is_valid_subject("foo.>.bar", wildcards=true), false)
  assert_eq(@moonnats.is_valid_subject("**", wildcards=true), false)
}

///|
test "subject_matches exact and wildcard tokens" {
  assert_eq(@moonnats.subject_matches("foo", "foo"), true)
  assert_eq(@moonnats.subject_matches("foo", "bar"), false)
  assert_eq(@moonnats.subject_matches("foo", "foo.bar"), false)
  assert_eq(@moonnats.subject_matches("foo.bar", "foo"), false)
  assert_eq(@moonnats.subject_matches("foo.*", "foo.bar"), true)
  assert_eq(@moonnats.subject_matches("foo.*", "foo.bar.baz"), false)
  assert_eq(@moonnats.subject_matches("foo.*.baz", "foo.bar.baz"), true)
  assert_eq(@moonnats.subject_matches("foo.>", "foo.bar.baz"), true)
  assert_eq(@moonnats.subject_matches("foo.>", "foo"), false)
  assert_eq(@moonnats.subject_matches(">", "anything.at.all"), true)
  assert_eq(@moonnats.subject_matches("*.*", "a.b"), true)
  assert_eq(@moonnats.subject_matches("a.*", "a"), false)
}
