//! Memcached wire adapters for the shared single-connection `brz-net` driver.

#![allow(dead_code)]

use std::{collections::VecDeque, ops::Range};

use brz_net::{
    Correlation, DecodedResponse, EphemeralBytes, EphemeralBytesMut, RequestToken, RxBuffer,
    RxFrame, SessionError, SessionProtocol, global_request_arena,
};
use bytes::Bytes;

#[cfg(feature = "metrics")]
use crate::profile_metrics::ProfileAttempt;
use crate::{Error, Expiration, Protocol, Result, value::CasValue, value::Value};

const REQUEST_MAGIC: u8 = 0x80;
const RESPONSE_MAGIC: u8 = 0x81;
const BINARY_HEADER_LEN: usize = 24;
const MAX_KEY_LEN: usize = 250;

const OP_GET: u8 = 0x00;
const OP_SET: u8 = 0x01;
const OP_ADD: u8 = 0x02;
const OP_DELETE: u8 = 0x04;

const STATUS_OK: u16 = 0x0000;
const STATUS_NOT_FOUND: u16 = 0x0001;
const STATUS_KEY_EXISTS: u16 = 0x0002;
const STATUS_VALUE_TOO_LARGE: u16 = 0x0003;
const STATUS_INVALID_ARGUMENTS: u16 = 0x0004;
const STATUS_NOT_STORED: u16 = 0x0005;
const STATUS_NON_NUMERIC: u16 = 0x0006;
const STATUS_UNKNOWN_COMMAND: u16 = 0x0081;
const STATUS_OUT_OF_MEMORY: u16 = 0x0082;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Get,
    Gets,
    Set,
    Add,
    Cas,
    Delete,
}

impl Operation {
    fn binary_opcode(self) -> u8 {
        match self {
            Self::Get | Self::Gets => OP_GET,
            Self::Set | Self::Cas => OP_SET,
            Self::Add => OP_ADD,
            Self::Delete => OP_DELETE,
        }
    }

    fn is_get(self) -> bool {
        matches!(self, Self::Get | Self::Gets)
    }

    fn retry_on_not_ok(self) -> bool {
        matches!(self, Self::Set)
    }
}

/// Unserialized request retained until the node has admitted it.
#[derive(Debug)]
pub(crate) struct MemcacheRequest {
    operation: Operation,
    key: Bytes,
    value: Option<Value>,
    expiration: Expiration,
    cas: u64,
    #[cfg(feature = "metrics")]
    profile_attempt: Option<ProfileAttempt>,
}

impl Clone for MemcacheRequest {
    fn clone(&self) -> Self {
        Self {
            operation: self.operation,
            key: self.key.clone(),
            value: self.value.clone(),
            expiration: self.expiration,
            cas: self.cas,
            #[cfg(feature = "metrics")]
            profile_attempt: None,
        }
    }
}

impl MemcacheRequest {
    pub(crate) fn get(key: &str) -> Self {
        Self::key_only(Operation::Get, key)
    }

    pub(crate) fn gets(key: &str) -> Self {
        Self::key_only(Operation::Gets, key)
    }

    pub(crate) fn set(key: &str, value: Value, expiration: Expiration) -> Self {
        Self::store(Operation::Set, key, value, expiration, 0)
    }

    pub(crate) fn add(key: &str, value: Value, expiration: Expiration) -> Self {
        Self::store(Operation::Add, key, value, expiration, 0)
    }

    pub(crate) fn cas(key: &str, value: Value, expiration: Expiration, cas: u64) -> Self {
        Self::store(Operation::Cas, key, value, expiration, cas)
    }

    pub(crate) fn delete(key: &str) -> Self {
        Self::key_only(Operation::Delete, key)
    }

    fn key_only(operation: Operation, key: &str) -> Self {
        Self {
            operation,
            key: Bytes::copy_from_slice(key.as_bytes()),
            value: None,
            expiration: Expiration::Never,
            cas: 0,
            #[cfg(feature = "metrics")]
            profile_attempt: None,
        }
    }

    fn store(
        operation: Operation,
        key: &str,
        value: Value,
        expiration: Expiration,
        cas: u64,
    ) -> Self {
        Self {
            operation,
            key: Bytes::copy_from_slice(key.as_bytes()),
            value: Some(value),
            expiration,
            cas,
            #[cfg(feature = "metrics")]
            profile_attempt: None,
        }
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn is_read(&self) -> bool {
        self.operation.is_get()
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn with_profile_attempt(mut self, attempt: ProfileAttempt) -> Self {
        self.profile_attempt = Some(attempt);
        self
    }

    /// reference client converts store propagation to unconditional SET and clears
    /// CAS; delete propagation remains delete.
    pub(crate) fn fanout(&self) -> Self {
        let mut request = self.clone();
        match request.operation {
            Operation::Set | Operation::Add | Operation::Cas => {
                request.operation = Operation::Set;
                request.cas = 0;
            }
            Operation::Delete => {}
            Operation::Get | Operation::Gets => {
                unreachable!("retrieval uses a response-derived SET for writeback")
            }
        }
        request
    }
}

#[derive(Debug)]
pub(crate) struct WriteResponse {
    /// The exact result returned to the caller.
    pub(crate) result: Result<bool>,
    /// Whether reference client considers this response successful for write fanout.
    pub(crate) fanout: bool,
    /// Whether a non-OK response may use the one configured foreground retry.
    pub(crate) retryable: bool,
}

#[derive(Debug)]
pub(crate) enum MemcacheResponse {
    Get(Result<Option<CasValue>>),
    Write(WriteResponse),
}

impl MemcacheResponse {
    pub(crate) fn into_get(self) -> Result<Option<CasValue>> {
        match self {
            Self::Get(result) => result,
            Self::Write(_) => Err(protocol_error("expected get response")),
        }
    }

    pub(crate) fn into_write(self) -> Result<WriteResponse> {
        match self {
            Self::Write(response) => Ok(response),
            Self::Get(_) => Err(protocol_error("expected write response")),
        }
    }
}

pub(crate) enum MemcacheProtocol {
    Text(TextCodec),
    Binary(BinaryCodec),
}

impl MemcacheProtocol {
    pub(crate) fn new(protocol: Protocol) -> Self {
        match protocol {
            Protocol::Text => Self::Text(TextCodec::default()),
            Protocol::Binary => Self::Binary(BinaryCodec::default()),
        }
    }
}

impl SessionProtocol for MemcacheProtocol {
    type Request = MemcacheRequest;
    type Frame = EphemeralBytes;
    type Response = MemcacheResponse;
    type Error = Error;

    fn reset(&mut self) {
        match self {
            Self::Text(codec) => codec.reset(),
            Self::Binary(codec) => codec.reset(),
        }
    }

    fn encode(&mut self, request: MemcacheRequest, token: RequestToken) -> Result<EphemeralBytes> {
        match self {
            Self::Text(codec) => codec.encode(request),
            Self::Binary(codec) => codec.encode(request, token),
        }
    }

    fn decode(
        &mut self,
        source: &mut RxBuffer,
    ) -> Result<Option<DecodedResponse<MemcacheResponse>>> {
        match self {
            Self::Text(codec) => codec.decode(source),
            Self::Binary(codec) => codec.decode(source),
        }
    }
}

struct PendingRequest {
    operation: Operation,
    #[cfg(feature = "metrics")]
    profile_attempt: Option<ProfileAttempt>,
}

impl PendingRequest {
    fn take_from(request: &mut MemcacheRequest) -> Self {
        Self {
            operation: request.operation,
            #[cfg(feature = "metrics")]
            profile_attempt: request.profile_attempt.take(),
        }
    }

    #[inline]
    fn finish(&mut self) {
        #[cfg(feature = "metrics")]
        if let Some(attempt) = &mut self.profile_attempt {
            attempt.finish(true);
        }
    }
}

#[derive(Default)]
pub(crate) struct TextCodec {
    pending: VecDeque<PendingRequest>,
}

impl TextCodec {
    fn reset(&mut self) {
        self.pending.clear();
    }

    fn encode(&mut self, mut request: MemcacheRequest) -> Result<EphemeralBytes> {
        validate_key(&request.key)?;
        let frame = encode_text(&request)?;
        self.pending
            .push_back(PendingRequest::take_from(&mut request));
        Ok(frame)
    }

    fn decode(
        &mut self,
        source: &mut RxBuffer,
    ) -> Result<Option<DecodedResponse<MemcacheResponse>>> {
        let Some(operation) = self.pending.front().map(|pending| pending.operation) else {
            return if source.is_empty() {
                Ok(None)
            } else {
                Err(protocol_error("text response has no pending request"))
            };
        };
        let Some((layout, consumed)) = scan_text(source, operation)? else {
            return Ok(None);
        };
        let mut pending = self
            .pending
            .pop_front()
            .expect("front pending request must still exist");
        let frame = source.take(consumed);
        let response = materialize_text(frame, layout, operation)?;
        pending.finish();
        Ok(Some(DecodedResponse::fifo(response)))
    }
}

struct BinaryPending {
    token: RequestToken,
    request: PendingRequest,
}

pub(crate) struct BinaryCodec {
    // One allocation per physical connection keeps the runtime protocol enum
    // compact while retaining O(1) low-byte opaque correlation.
    pending: Box<[Option<BinaryPending>; 256]>,
}

impl Default for BinaryCodec {
    fn default() -> Self {
        Self {
            pending: Box::new(std::array::from_fn(|_| None)),
        }
    }
}

impl BinaryCodec {
    fn reset(&mut self) {
        for pending in self.pending.iter_mut() {
            *pending = None;
        }
    }

    fn encode(
        &mut self,
        mut request: MemcacheRequest,
        token: RequestToken,
    ) -> Result<EphemeralBytes> {
        validate_key(&request.key)?;
        let index = token.index();
        if self.pending[index].is_some() {
            return Err(protocol_error("binary opaque slot is already occupied"));
        }
        let frame = encode_binary(&request, index as u32)?;
        self.pending[index] = Some(BinaryPending {
            token,
            request: PendingRequest::take_from(&mut request),
        });
        Ok(frame)
    }

    fn decode(
        &mut self,
        source: &mut RxBuffer,
    ) -> Result<Option<DecodedResponse<MemcacheResponse>>> {
        if source.len() < BINARY_HEADER_LEN {
            return Ok(None);
        }
        if source.byte(0) != Some(RESPONSE_MAGIC) {
            return Err(protocol_error("invalid binary response magic"));
        }
        let body_len = read_u32(source, 8)? as usize;
        let consumed = BINARY_HEADER_LEN
            .checked_add(body_len)
            .ok_or_else(|| protocol_error("binary response length overflow"))?;
        if source.len() < consumed {
            source
                .reserve(consumed - source.len())
                .map_err(|error| protocol_detail("binary response is too large", error))?;
            return Ok(None);
        }

        let opaque = read_u32(source, 12)? as usize;
        let pending = self
            .pending
            .get_mut(opaque)
            .and_then(Option::take)
            .ok_or_else(|| protocol_error("binary response opaque has no pending request"))?;
        let opcode = source.byte(1).expect("complete binary header");
        if opcode != pending.request.operation.binary_opcode() {
            return Err(protocol_error(
                "binary response opcode does not match request",
            ));
        }

        let status = read_u16(source, 6)?;
        let key_len = read_u16(source, 2)? as usize;
        let extras_len = source.byte(4).expect("complete binary header") as usize;
        if extras_len + key_len > body_len {
            return Err(protocol_error("binary response extras/key exceed body"));
        }
        let cas = read_u64(source, 16)?;
        let frame = frame_bytes(source.take(consumed));
        let body_start = BINARY_HEADER_LEN;
        let value_start = body_start + extras_len + key_len;
        let value = frame.slice(value_start..consumed);
        let response = materialize_binary(
            pending.request.operation,
            status,
            cas,
            &frame[body_start..body_start + extras_len],
            value,
        )?;
        let mut request = pending.request;
        request.finish();
        Ok(Some(DecodedResponse {
            correlation: Correlation::Tagged(pending.token),
            response,
        }))
    }
}

pub(crate) fn validate_key(key: &[u8]) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidKey("key is empty"));
    }
    if key.len() > MAX_KEY_LEN {
        return Err(Error::InvalidKey("key exceeds 250 bytes"));
    }
    if key
        .iter()
        .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
    {
        return Err(Error::InvalidKey(
            "key contains whitespace or control bytes",
        ));
    }
    Ok(())
}

fn encode_text(request: &MemcacheRequest) -> Result<EphemeralBytes> {
    let value_len = request
        .value
        .as_ref()
        .map_or(0, |value| value.as_bytes().len());
    let frame_len = text_frame_len(request, value_len)
        .ok_or_else(|| protocol_error("text request length overflow"))?;
    let mut frame = global_request_arena().alloc(frame_len);
    match request.operation {
        Operation::Get => {
            frame.extend_from_slice(b"get ");
            frame.extend_from_slice(&request.key);
            frame.extend_from_slice(b"\r\n");
        }
        Operation::Gets => {
            frame.extend_from_slice(b"gets ");
            frame.extend_from_slice(&request.key);
            frame.extend_from_slice(b"\r\n");
        }
        Operation::Set | Operation::Add | Operation::Cas => {
            let value = request.value.as_ref().expect("store request has value");
            frame.extend_from_slice(match request.operation {
                Operation::Set => b"set ",
                Operation::Add => b"add ",
                Operation::Cas => b"cas ",
                _ => unreachable!(),
            });
            frame.extend_from_slice(&request.key);
            frame.extend_from_slice(b" ");
            write_u64(value.flags() as u64, &mut frame);
            frame.extend_from_slice(b" ");
            write_u64(request.expiration.to_wire() as u64, &mut frame);
            frame.extend_from_slice(b" ");
            write_u64(value.as_bytes().len() as u64, &mut frame);
            if request.operation == Operation::Cas {
                frame.extend_from_slice(b" ");
                write_u64(request.cas, &mut frame);
            }
            frame.extend_from_slice(b"\r\n");
            frame.extend_from_slice(value.as_bytes());
            frame.extend_from_slice(b"\r\n");
        }
        Operation::Delete => {
            frame.extend_from_slice(b"delete ");
            frame.extend_from_slice(&request.key);
            frame.extend_from_slice(b"\r\n");
        }
    }
    debug_assert_eq!(frame.len(), frame_len);
    Ok(frame.freeze())
}

fn text_frame_len(request: &MemcacheRequest, value_len: usize) -> Option<usize> {
    let key_len = request.key.len();
    match request.operation {
        Operation::Get => key_len.checked_add(6),
        Operation::Gets => key_len.checked_add(7),
        Operation::Delete => key_len.checked_add(9),
        Operation::Set | Operation::Add | Operation::Cas => {
            let value = request.value.as_ref().expect("store request has value");
            let mut length = 11_usize
                .checked_add(key_len)?
                .checked_add(value_len)?
                .checked_add(decimal_len(u64::from(value.flags())))?
                .checked_add(decimal_len(u64::from(request.expiration.to_wire())))?
                .checked_add(decimal_len(value_len as u64))?;
            if request.operation == Operation::Cas {
                length = length
                    .checked_add(1)?
                    .checked_add(decimal_len(request.cas))?;
            }
            Some(length)
        }
    }
}

#[inline]
fn decimal_len(value: u64) -> usize {
    if value == 0 {
        1
    } else {
        value.ilog10() as usize + 1
    }
}

fn encode_binary(request: &MemcacheRequest, opaque: u32) -> Result<EphemeralBytes> {
    let value = request.value.as_ref();
    let extras_len: usize = if matches!(
        request.operation,
        Operation::Set | Operation::Add | Operation::Cas
    ) {
        8
    } else {
        0
    };
    let value_len = value.map_or(0, |value| value.as_bytes().len());
    let body_len = extras_len
        .checked_add(request.key.len())
        .and_then(|length| length.checked_add(value_len))
        .ok_or_else(|| protocol_error("binary request length overflow"))?;
    let frame_len = BINARY_HEADER_LEN
        .checked_add(body_len)
        .ok_or_else(|| protocol_error("binary request length overflow"))?;
    let mut frame = global_request_arena().alloc(frame_len);
    frame.extend_from_slice(&[REQUEST_MAGIC, request.operation.binary_opcode()]);
    frame.extend_from_slice(&(request.key.len() as u16).to_be_bytes());
    frame.extend_from_slice(&[extras_len as u8, 0, 0, 0]);
    frame.extend_from_slice(&(body_len as u32).to_be_bytes());
    frame.extend_from_slice(&opaque.to_be_bytes());
    frame.extend_from_slice(&request.cas.to_be_bytes());
    if let Some(value) = value {
        frame.extend_from_slice(&value.flags().to_be_bytes());
        frame.extend_from_slice(&request.expiration.to_wire().to_be_bytes());
    }
    frame.extend_from_slice(&request.key);
    if let Some(value) = value {
        frame.extend_from_slice(value.as_bytes());
    }
    debug_assert_eq!(frame.len(), frame_len);
    Ok(frame.freeze())
}

fn write_u64(value: u64, frame: &mut EphemeralBytesMut) {
    let mut buffer = itoa::Buffer::new();
    frame.extend_from_slice(buffer.format(value).as_bytes());
}

enum TextLayout {
    Miss,
    Value {
        body: Range<usize>,
        flags: u32,
        cas: u64,
    },
    Line(Range<usize>),
}

fn scan_text(source: &mut RxBuffer, operation: Operation) -> Result<Option<(TextLayout, usize)>> {
    let Some(line_end) = source.find_crlf(0) else {
        return Ok(None);
    };
    let after_line = line_end + 2;
    if operation.is_get() {
        if source.range_eq(0..line_end, b"END") {
            return Ok(Some((TextLayout::Miss, after_line)));
        }
        if !source.range_eq(0..6.min(line_end), b"VALUE ") {
            return Ok(Some((TextLayout::Line(0..line_end), after_line)));
        }
        let mut cursor = 6;
        let _key = next_token(source, &mut cursor, line_end)
            .ok_or_else(|| protocol_error("malformed text VALUE key"))?;
        let flags = parse_decimal(
            source,
            next_token(source, &mut cursor, line_end)
                .ok_or_else(|| protocol_error("malformed text VALUE flags"))?,
        )?;
        let value_len = parse_decimal(
            source,
            next_token(source, &mut cursor, line_end)
                .ok_or_else(|| protocol_error("malformed text VALUE length"))?,
        )? as usize;
        let cas = next_token(source, &mut cursor, line_end)
            .map(|range| parse_decimal(source, range))
            .transpose()?
            .unwrap_or(0);
        let body_end = after_line
            .checked_add(value_len)
            .ok_or_else(|| protocol_error("text VALUE length overflow"))?;
        let consumed = body_end
            .checked_add(7)
            .ok_or_else(|| protocol_error("text VALUE length overflow"))?;
        if source.len() < consumed {
            source
                .reserve(consumed - source.len())
                .map_err(|error| protocol_detail("text response is too large", error))?;
            return Ok(None);
        }
        if source.byte(body_end) != Some(b'\r')
            || source.byte(body_end + 1) != Some(b'\n')
            || !source.range_eq(body_end + 2..consumed, b"END\r\n")
        {
            return Err(protocol_error("text VALUE is not terminated by END"));
        }
        return Ok(Some((
            TextLayout::Value {
                body: after_line..body_end,
                flags: flags as u32,
                cas,
            },
            consumed,
        )));
    }
    Ok(Some((TextLayout::Line(0..line_end), after_line)))
}

fn next_token(source: &RxBuffer, cursor: &mut usize, end: usize) -> Option<Range<usize>> {
    while *cursor < end && source.byte(*cursor) == Some(b' ') {
        *cursor += 1;
    }
    let start = *cursor;
    while *cursor < end && source.byte(*cursor) != Some(b' ') {
        *cursor += 1;
    }
    (start < *cursor).then_some(start..*cursor)
}

fn parse_decimal(source: &RxBuffer, range: Range<usize>) -> Result<u64> {
    let mut value = 0_u64;
    for index in range {
        let digit = source
            .byte(index)
            .filter(u8::is_ascii_digit)
            .ok_or_else(|| protocol_error("invalid decimal in text response"))?;
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(digit - b'0')))
            .ok_or_else(|| protocol_error("decimal overflow in text response"))?;
    }
    Ok(value)
}

fn materialize_text(
    frame: RxFrame,
    layout: TextLayout,
    operation: Operation,
) -> Result<MemcacheResponse> {
    Ok(match layout {
        TextLayout::Miss => MemcacheResponse::Get(Ok(None)),
        TextLayout::Value { body, flags, cas } => {
            let bytes = frame_bytes(frame);
            MemcacheResponse::Get(Ok(Some(CasValue::new(
                Value::new(bytes.slice(body), flags),
                cas,
            ))))
        }
        TextLayout::Line(range) => {
            let bytes = frame.copy_range(range);
            let line = String::from_utf8_lossy(&bytes);
            if operation.is_get() {
                return match classify_text_error(&line) {
                    Some(error) => Ok(MemcacheResponse::Get(Err(error))),
                    None => Err(Error::Protocol(format!(
                        "unexpected text get response: {line}"
                    ))),
                };
            }
            let (result, fanout, retryable) = match operation {
                Operation::Set => match line.as_ref() {
                    "STORED" => (Ok(true), true, false),
                    "NOT_STORED" | "EXISTS" | "NOT_FOUND" => (Ok(false), false, true),
                    _ => match classify_text_error(&line) {
                        Some(error) => (Err(error), false, false),
                        None => {
                            return Err(Error::Protocol(format!(
                                "unexpected text set response: {line}"
                            )));
                        }
                    },
                },
                Operation::Add | Operation::Cas => match line.as_ref() {
                    "STORED" => (Ok(true), true, false),
                    "NOT_STORED" | "EXISTS" | "NOT_FOUND" => (Ok(false), false, false),
                    _ => match classify_text_error(&line) {
                        Some(error) => (Err(error), false, false),
                        None => {
                            return Err(Error::Protocol(format!(
                                "unexpected text store response: {line}"
                            )));
                        }
                    },
                },
                Operation::Delete => match line.as_ref() {
                    "DELETED" => (Ok(true), true, false),
                    "NOT_FOUND" => (Ok(false), true, false),
                    _ => match classify_text_error(&line) {
                        Some(error) => (Err(error), false, false),
                        None => {
                            return Err(Error::Protocol(format!(
                                "unexpected text delete response: {line}"
                            )));
                        }
                    },
                },
                Operation::Get | Operation::Gets => unreachable!(),
            };
            MemcacheResponse::Write(WriteResponse {
                result,
                fanout,
                retryable,
            })
        }
    })
}

fn materialize_binary(
    operation: Operation,
    status: u16,
    cas: u64,
    extras: &[u8],
    value: Bytes,
) -> Result<MemcacheResponse> {
    if operation.is_get() {
        let result = match status {
            STATUS_OK if extras.len() >= 4 => Ok(Some(CasValue::new(
                Value::new(
                    value,
                    u32::from_be_bytes(extras[..4].try_into().expect("four-byte flags")),
                ),
                cas,
            ))),
            STATUS_OK => return Err(protocol_error("binary get response is missing flags")),
            STATUS_NOT_FOUND => Ok(None),
            _ => Err(binary_status_error(status, &value)),
        };
        return Ok(MemcacheResponse::Get(result));
    }

    let result = match status {
        STATUS_OK => Ok(true),
        STATUS_KEY_EXISTS | STATUS_NOT_STORED | STATUS_NOT_FOUND => Ok(false),
        _ => Err(binary_status_error(status, &value)),
    };
    let fanout = match operation {
        Operation::Set | Operation::Add | Operation::Cas => status != STATUS_KEY_EXISTS,
        Operation::Delete => true,
        Operation::Get | Operation::Gets => unreachable!(),
    };
    let retryable = !fanout && operation.retry_on_not_ok();
    Ok(MemcacheResponse::Write(WriteResponse {
        result,
        fanout,
        retryable,
    }))
}

fn classify_text_error(line: &str) -> Option<Error> {
    if line == "ERROR" {
        return Some(Error::Client("unknown command".into()));
    }
    if let Some(message) = line.strip_prefix("CLIENT_ERROR ") {
        return Some(Error::Client(message.to_owned()));
    }
    if let Some(message) = line.strip_prefix("SERVER_ERROR ") {
        return Some(Error::Server(message.to_owned()));
    }
    None
}

fn binary_status_error(status: u16, value: &[u8]) -> Error {
    let message = String::from_utf8_lossy(value);
    match status {
        STATUS_UNKNOWN_COMMAND => Error::Client(format!("unknown command: {message}")),
        STATUS_INVALID_ARGUMENTS => Error::Client(format!("invalid arguments: {message}")),
        STATUS_NON_NUMERIC => Error::Client(format!("non-numeric value: {message}")),
        STATUS_VALUE_TOO_LARGE => Error::Server(format!("value too large: {message}")),
        STATUS_OUT_OF_MEMORY => Error::Server(format!("out of memory: {message}")),
        other => Error::Server(format!("status 0x{other:04x}: {message}")),
    }
}

fn read_u16(source: &RxBuffer, offset: usize) -> Result<u16> {
    Ok(u16::from_be_bytes([
        source
            .byte(offset)
            .ok_or_else(|| protocol_error("short binary header"))?,
        source
            .byte(offset + 1)
            .ok_or_else(|| protocol_error("short binary header"))?,
    ]))
}

fn read_u32(source: &RxBuffer, offset: usize) -> Result<u32> {
    Ok(u32::from_be_bytes(std::array::from_fn(|index| {
        source.byte(offset + index).expect("complete binary header")
    })))
}

fn read_u64(source: &RxBuffer, offset: usize) -> Result<u64> {
    Ok(u64::from_be_bytes(std::array::from_fn(|index| {
        source.byte(offset + index).expect("complete binary header")
    })))
}

fn frame_bytes(frame: RxFrame) -> Bytes {
    match frame.into_contiguous() {
        Ok(frame) => Bytes::from_owner(frame),
        Err(frame) => frame.copy_range(0..frame.len()),
    }
}

fn protocol_error(message: &'static str) -> Error {
    Error::Protocol(message.into())
}

fn protocol_detail(message: &'static str, detail: impl std::fmt::Display) -> Error {
    Error::Protocol(format!("{message}: {detail}"))
}

pub(crate) fn map_session_error(error: SessionError<Error>) -> Error {
    match error {
        SessionError::Busy => Error::Overloaded,
        SessionError::Unavailable | SessionError::Closed => Error::Unavailable,
        SessionError::Timeout { .. } => Error::Timeout,
        SessionError::Io(error) => Error::Io(std::io::Error::new(error.kind(), error.to_string())),
        SessionError::Protocol(error) => Error::Protocol(error.to_string()),
        SessionError::UnexpectedResponse => Error::Desynced("unexpected response".into()),
        SessionError::EmptyRequestFrame => Error::Protocol("empty request frame".into()),
        SessionError::Routing(message) => Error::Client(message.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use brz_net::{RequestToken, RxBuffer, SessionProtocol};

    use super::*;

    fn feed(source: &mut RxBuffer, bytes: &[u8]) {
        source.extend_from_slice(bytes).unwrap();
    }

    #[test]
    fn binary_opaque_uses_slot_and_restores_full_request_token() {
        let mut codec = BinaryCodec::default();
        let token = RequestToken::from_raw((123_u64 << 8) | 17);
        let frame = codec.encode(MemcacheRequest::get("key"), token).unwrap();
        assert_eq!(
            u32::from_be_bytes(frame.as_ref()[12..16].try_into().unwrap()),
            17
        );

        let mut response = vec![0_u8; 24 + 4 + 5];
        response[0] = RESPONSE_MAGIC;
        response[1] = OP_GET;
        response[4] = 4;
        response[8..12].copy_from_slice(&9_u32.to_be_bytes());
        response[12..16].copy_from_slice(&17_u32.to_be_bytes());
        response[24..28].copy_from_slice(&32_u32.to_be_bytes());
        response[28..].copy_from_slice(b"value");
        let mut source = RxBuffer::with_capacity(16);
        feed(&mut source, &response);

        let decoded = codec.decode(&mut source).unwrap().unwrap();
        assert_eq!(decoded.correlation, Correlation::Tagged(token));
        assert!(
            matches!(decoded.response.into_get().unwrap(), Some(value) if value.value.as_bytes() == b"value")
        );
    }

    #[test]
    fn fragmented_text_value_is_zero_copy_when_contiguous() {
        let mut codec = TextCodec::default();
        codec.encode(MemcacheRequest::get("key")).unwrap();
        let mut source = RxBuffer::with_capacity(8);
        feed(&mut source, b"VALUE key 7 5\r\nhe");
        assert!(codec.decode(&mut source).unwrap().is_none());
        feed(&mut source, b"llo\r\nEND\r\n");
        let response = codec.decode(&mut source).unwrap().unwrap().response;
        let value = response.into_get().unwrap().unwrap();
        assert_eq!(value.value.as_bytes(), b"hello");
        assert_eq!(value.value.flags(), 7);
    }

    #[test]
    fn runtime_protocol_delegates_binary_tagging() {
        let mut protocol = MemcacheProtocol::new(Protocol::Binary);
        let token = RequestToken::from_raw(3);
        let frame = protocol
            .encode(MemcacheRequest::delete("key"), token)
            .unwrap();
        assert_eq!(frame.as_ref()[0], REQUEST_MAGIC);
        assert_eq!(frame.as_ref()[1], OP_DELETE);
    }
}
