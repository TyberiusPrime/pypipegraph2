//! Canonical JSON (WP2-Rust, ppg3/CONTRACT.md "Canonical JSON").
//!
//! The validator IS the spec (PPG3_DESIGN.md §5): UTF-8, object keys
//! strictly byte-sorted, zero insignificant whitespace, no floats anywhere
//! (a JSON number containing `.`, `e`, or `E` is rejected; integers must fit
//! in i64 or u64), no duplicate object keys. Strings are NOT NFC-normalized
//! — two Unicode-equivalent-but-differently-encoded strings hash
//! differently; this is documented behavior, not a bug.

use serde_json::Value;

use crate::error::Error;
use crate::hash::blake3_hex;

/// Validate that `bytes` is exactly a canonical-JSON document: UTF-8,
/// zero insignificant whitespace, strictly sorted+unique object keys, no
/// floats, integers within i64/u64 range.
pub fn validate(bytes: &[u8]) -> Result<(), Error> {
    let s = std::str::from_utf8(bytes).map_err(|e| Error::Canon(format!("invalid utf-8: {e}")))?;
    let mut p = Parser {
        b: s.as_bytes(),
        pos: 0,
    };
    p.parse_value(0)?;
    if p.pos != p.b.len() {
        return Err(Error::Canon(format!(
            "trailing bytes after top-level value at offset {}",
            p.pos
        )));
    }
    Ok(())
}

/// Canonicalize a `serde_json::Value`: recursively sort object keys by
/// UTF-8 byte order, reject any float (a `Number` that is not exactly
/// representable as i64/u64), and serialize compactly (no whitespace).
pub fn canonicalize(v: &Value) -> Result<Vec<u8>, Error> {
    let sorted = canon_value(v, 0)?;
    serde_json::to_vec(&sorted).map_err(|e| Error::Canon(format!("serialize: {e}")))
}

/// `validate(canonical_key_doc)` then `blake3_hex` of it. Returns the
/// lowercase hex input key.
pub fn input_key(canonical_key_doc: &[u8]) -> Result<String, Error> {
    validate(canonical_key_doc)?;
    Ok(blake3_hex(canonical_key_doc))
}

const MAX_DEPTH: u32 = 256;

fn canon_value(v: &Value, depth: u32) -> Result<Value, Error> {
    if depth > MAX_DEPTH {
        return Err(Error::Canon("max nesting depth exceeded".to_string()));
    }
    match v {
        Value::Null | Value::Bool(_) | Value::String(_) => Ok(v.clone()),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                Ok(Value::Number(n.clone()))
            } else {
                Err(Error::Canon(format!(
                    "floating point numbers are not allowed in canonical json: {n}"
                )))
            }
        }
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(canon_value(item, depth + 1)?);
            }
            Ok(Value::Array(out))
        }
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            let mut out = serde_json::Map::new();
            for (k, val) in entries {
                out.insert(k.clone(), canon_value(val, depth + 1)?);
            }
            Ok(Value::Object(out))
        }
    }
}

/// Hand-rolled recursive-descent validator. serde_json cannot be reused
/// here: it silently accepts whitespace, silently drops duplicate keys
/// (last-wins) instead of rejecting them, and does not distinguish "integer
/// literal" from "float that happens to be whole" the way the contract
/// requires (rejection is by lexical form: presence of `.`/`e`/`E`).
struct Parser<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn err(&self, msg: impl Into<String>) -> Error {
        Error::Canon(format!("{} at byte offset {}", msg.into(), self.pos))
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.pos).copied()
    }

    fn expect(&mut self, c: u8) -> Result<(), Error> {
        if self.peek() == Some(c) {
            self.pos += 1;
            Ok(())
        } else {
            Err(self.err(format!("expected '{}'", c as char)))
        }
    }

    fn parse_value(&mut self, depth: u32) -> Result<(), Error> {
        if depth > MAX_DEPTH {
            return Err(self.err("max nesting depth exceeded"));
        }
        match self.peek() {
            Some(b'{') => self.parse_object(depth),
            Some(b'[') => self.parse_array(depth),
            Some(b'"') => self.parse_string().map(|_| ()),
            Some(b't') => self.parse_lit("true"),
            Some(b'f') => self.parse_lit("false"),
            Some(b'n') => self.parse_lit("null"),
            Some(c) if c == b'-' || c.is_ascii_digit() => self.parse_number(),
            Some(c) if (c as char).is_whitespace() => Err(self.err("unexpected whitespace")),
            Some(_) => Err(self.err("unexpected byte")),
            None => Err(self.err("unexpected end of input")),
        }
    }

    fn parse_lit(&mut self, lit: &str) -> Result<(), Error> {
        let bytes = lit.as_bytes();
        if self.b[self.pos..].starts_with(bytes) {
            self.pos += bytes.len();
            Ok(())
        } else {
            Err(self.err(format!("expected literal `{lit}`")))
        }
    }

    fn parse_object(&mut self, depth: u32) -> Result<(), Error> {
        self.expect(b'{')?;
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(());
        }
        let mut prev_key: Option<String> = None;
        loop {
            if self.peek() != Some(b'"') {
                return Err(self.err("expected string key"));
            }
            let key = self.parse_string()?;
            if let Some(p) = &prev_key {
                if key.as_bytes() <= p.as_bytes() {
                    return Err(self.err(format!(
                        "object keys not strictly sorted or duplicated: {key:?} after {p:?}"
                    )));
                }
            }
            self.expect(b':')?;
            self.parse_value(depth + 1)?;
            prev_key = Some(key);
            match self.peek() {
                Some(b',') => {
                    self.pos += 1;
                }
                Some(b'}') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(self.err("expected ',' or '}'")),
            }
        }
        Ok(())
    }

    fn parse_array(&mut self, depth: u32) -> Result<(), Error> {
        self.expect(b'[')?;
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(());
        }
        loop {
            self.parse_value(depth + 1)?;
            match self.peek() {
                Some(b',') => {
                    self.pos += 1;
                }
                Some(b']') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(self.err("expected ',' or ']'")),
            }
        }
        Ok(())
    }

    fn parse_string(&mut self) -> Result<String, Error> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            match self.peek() {
                None => return Err(self.err("unterminated string")),
                Some(b'"') => {
                    self.pos += 1;
                    break;
                }
                Some(b'\\') => {
                    self.pos += 1;
                    match self.peek() {
                        Some(b'"') => {
                            out.push('"');
                            self.pos += 1;
                        }
                        Some(b'\\') => {
                            out.push('\\');
                            self.pos += 1;
                        }
                        Some(b'/') => {
                            out.push('/');
                            self.pos += 1;
                        }
                        Some(b'b') => {
                            out.push('\u{0008}');
                            self.pos += 1;
                        }
                        Some(b'f') => {
                            out.push('\u{000C}');
                            self.pos += 1;
                        }
                        Some(b'n') => {
                            out.push('\n');
                            self.pos += 1;
                        }
                        Some(b'r') => {
                            out.push('\r');
                            self.pos += 1;
                        }
                        Some(b't') => {
                            out.push('\t');
                            self.pos += 1;
                        }
                        Some(b'u') => {
                            self.pos += 1;
                            let cp = self.parse_hex4()?;
                            if (0xD800..=0xDBFF).contains(&cp) {
                                if self.peek() != Some(b'\\') {
                                    return Err(self.err("expected low surrogate"));
                                }
                                self.pos += 1;
                                if self.peek() != Some(b'u') {
                                    return Err(self.err("expected low surrogate"));
                                }
                                self.pos += 1;
                                let low = self.parse_hex4()?;
                                if !(0xDC00..=0xDFFF).contains(&low) {
                                    return Err(self.err("invalid low surrogate"));
                                }
                                let c = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
                                let ch = char::from_u32(c)
                                    .ok_or_else(|| self.err("invalid surrogate pair"))?;
                                out.push(ch);
                            } else if (0xDC00..=0xDFFF).contains(&cp) {
                                return Err(self.err("unpaired low surrogate"));
                            } else {
                                let ch = char::from_u32(cp)
                                    .ok_or_else(|| self.err("invalid unicode escape"))?;
                                out.push(ch);
                            }
                        }
                        _ => return Err(self.err("invalid escape sequence")),
                    }
                }
                Some(c) if c < 0x20 => {
                    return Err(self.err("unescaped control character in string"))
                }
                Some(_) => {
                    let start = self.pos;
                    let rest = std::str::from_utf8(&self.b[start..])
                        .map_err(|e| self.err(format!("utf8: {e}")))?;
                    let ch = rest.chars().next().unwrap();
                    out.push(ch);
                    self.pos += ch.len_utf8();
                }
            }
        }
        Ok(out)
    }

    fn parse_hex4(&mut self) -> Result<u32, Error> {
        if self.pos + 4 > self.b.len() {
            return Err(self.err("truncated unicode escape"));
        }
        let s = std::str::from_utf8(&self.b[self.pos..self.pos + 4])
            .map_err(|_| self.err("bad unicode escape"))?;
        let v = u32::from_str_radix(s, 16).map_err(|_| self.err("bad hex digits"))?;
        self.pos += 4;
        Ok(v)
    }

    fn parse_number(&mut self) -> Result<(), Error> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        match self.peek() {
            Some(b'0') => {
                self.pos += 1;
            }
            Some(c) if c.is_ascii_digit() => {
                while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                    self.pos += 1;
                }
            }
            _ => return Err(self.err("invalid number")),
        }
        if matches!(self.peek(), Some(b'.') | Some(b'e') | Some(b'E')) {
            return Err(self.err(
                "floating point numbers are not allowed in canonical json (found '.'/'e'/'E')",
            ));
        }
        let text = std::str::from_utf8(&self.b[start..self.pos]).expect("ascii digits");
        if text.starts_with('-') {
            text.parse::<i64>()
                .map_err(|_| self.err("integer out of i64 range"))?;
        } else {
            text.parse::<u64>()
                .map_err(|_| self.err("integer out of u64 range"))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn validate_accepts_minimal_object() {
        assert!(validate(br#"{"a":1}"#).is_ok());
    }

    #[test]
    fn validate_rejects_whitespace() {
        assert!(validate(br#"{"a": 1}"#).is_err());
        assert!(validate(b" {}").is_err());
        assert!(validate(b"{}\n").is_err());
    }

    #[test]
    fn validate_rejects_unsorted_keys() {
        assert!(validate(br#"{"b":1,"a":2}"#).is_err());
    }

    #[test]
    fn validate_rejects_duplicate_keys() {
        assert!(validate(br#"{"a":1,"a":2}"#).is_err());
    }

    #[test]
    fn validate_accepts_sorted_keys() {
        assert!(validate(br#"{"a":1,"b":2,"c":3}"#).is_ok());
    }

    #[test]
    fn validate_rejects_float_with_dot() {
        assert!(validate(br#"{"a":1.0}"#).is_err());
    }

    #[test]
    fn validate_rejects_float_with_exponent() {
        assert!(validate(br#"{"a":1e10}"#).is_err());
        assert!(validate(br#"{"a":1E10}"#).is_err());
    }

    #[test]
    fn validate_accepts_negative_integer() {
        assert!(validate(br#"{"a":-42}"#).is_ok());
    }

    #[test]
    fn validate_rejects_leading_zero() {
        assert!(validate(br#"{"a":01}"#).is_err());
    }

    #[test]
    fn validate_accepts_unicode_string() {
        assert!(validate("{\"a\":\"héllo wörld ☃\"}".as_bytes()).is_ok());
    }

    #[test]
    fn validate_rejects_unescaped_control_char() {
        let bytes = b"{\"a\":\"x\ny\"}";
        assert!(validate(bytes).is_err());
    }

    #[test]
    fn validate_rejects_trailing_bytes() {
        assert!(validate(br#"{}{}"#).is_err());
    }

    #[test]
    fn canonicalize_sorts_keys() {
        let v = json!({"b": 1, "a": 2, "c": {"z": 1, "y": 2}});
        let out = canonicalize(&v).unwrap();
        assert_eq!(out, br#"{"a":2,"b":1,"c":{"y":2,"z":1}}"#);
    }

    #[test]
    fn canonicalize_rejects_float() {
        let v = json!({"a": 1.5});
        assert!(canonicalize(&v).is_err());
    }

    #[test]
    fn canonicalize_rejects_whole_number_float() {
        // 1.0 parses into serde_json::Number as f64, not i64/u64 - must be
        // rejected even though it "looks" like an integer value.
        let v: Value = serde_json::from_str(r#"{"a": 1.0}"#).unwrap();
        assert!(canonicalize(&v).is_err());
    }

    #[test]
    fn canonicalize_keeps_arrays_in_order() {
        let v = json!({"a": [3, 1, 2]});
        let out = canonicalize(&v).unwrap();
        assert_eq!(out, br#"{"a":[3,1,2]}"#);
    }

    #[test]
    fn canonicalize_roundtrips_through_validate() {
        let v = json!({
            "ppg3_key_version": 1,
            "inputs": {"b": "hash2", "a": "hash1"},
            "env": {},
            "outputs_declared": ["x/y.txt"]
        });
        let out = canonicalize(&v).unwrap();
        validate(&out).expect("canonicalize output must itself validate");
    }

    #[test]
    fn input_key_is_64_char_hex_and_stable() {
        let canon = canonicalize(&json!({"a": 1})).unwrap();
        let ik = input_key(&canon).unwrap();
        assert_eq!(ik.len(), 64);
        assert!(ik
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        let ik2 = input_key(&canon).unwrap();
        assert_eq!(ik, ik2);
    }

    #[test]
    fn input_key_rejects_non_canonical_input() {
        assert!(input_key(br#"{"a": 1}"#).is_err());
    }

    #[test]
    fn canonicalize_handles_unicode_and_escapes() {
        let v = json!({"s": "tab\there\nline\"quote\\back"});
        let out = canonicalize(&v).unwrap();
        validate(&out).unwrap();
        // round-trip through serde_json to confirm content preserved
        let back: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(back, v);
    }
}
