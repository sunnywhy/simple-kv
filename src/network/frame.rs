use std::io::{Read, Write};

use bytes::{Buf, BufMut, BytesMut};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::debug;

use crate::{CommandRequest, CommandResponse, KvError};

// the length took 4 bytes
pub const LENGTH_BYTES: usize = 4;
// the length will be 31 bit, so biggest frame is 2GB
const MAX_FRAME: usize = 2 * 1024 * 1024 * 1024;
// if payload > 1436 bytes, then gzip it
// because internet MTU is 1500 bytes, ip header is 20 bytes, tcp header is 20 bytes, so 1500 - 20 - 20 = 1460
// we reserve another 20 bytes, but we need to add 4 bytes for length, so 1460 - 20 - 4 = 1436
// if payload > 1436 bytes, there is a high chance it will be split into multiple packets, so we gzip it
const COMPRESSION_THRESHOLD: usize = 1436;
// compression flag bit (the 4 bytes length's highest bit)
const COMPRESSION_BIT: usize = 1 << 31;

// handle Frame's encode and decode
pub trait FrameCoder
    where
        Self: Message + Sized + Default,
{
    // convert a Message to a frame
    fn encode_frame(&self, buf: &mut BytesMut) -> Result<(), KvError> {
        let size = self.encoded_len();
        if size > MAX_FRAME {
            return Err(KvError::FrameError);
        }

        // write length first, if need compression, set the new length later
        buf.put_u32(size as u32);

        if size > COMPRESSION_THRESHOLD {
            let mut compressed_buf = Vec::with_capacity(size);
            self.encode(&mut compressed_buf)?;

            // BytesMut support logic split
            // so we remove the 4 bytes length first
            let payload = buf.split_off(LENGTH_BYTES);
            buf.clear();

            // handle gzip
            let mut encoder = GzEncoder::new(payload.writer(), Compression::default());
            encoder.write_all(&compressed_buf)?;

            // after compression, get the BytesMut from the gzip encoder
            let payload = encoder.finish()?.into_inner();
            debug!("Encode a frame with compression, original size: {}, compressed size: {}", size, payload.len());

            // set the new length
            buf.put_u32(payload.len() as u32 | COMPRESSION_BIT as u32);

            buf.unsplit(payload);
        } else {
            self.encode(buf)?;
        }

        Ok(())
    }

    // convert a frame to a Message
    fn decode_frame(buf: &mut BytesMut) -> Result<Self, KvError> {
        // get 4 bytes, read length and compression flag
        let header = buf.get_u32() as usize;
        let (len, compressed) = decode_header(header);
        debug!("Got a frame, length: {}, compressed: {}", len, compressed);

        if compressed {
            // unzip
            let mut decoder = GzDecoder::new(&buf[..len]);
            let mut decompressed_buf = Vec::with_capacity(len * 2);
            decoder.read_to_end(&mut decompressed_buf)?;
            buf.advance(len);

            // decode
            Ok(Self::decode(&decompressed_buf[..])?)
        } else {
            // decode
            let message = Self::decode(&buf[..len])?;
            buf.advance(len);
            Ok(message)
        }
    }
}

impl FrameCoder for CommandRequest {}

impl FrameCoder for CommandResponse {}

fn decode_header(header: usize) -> (usize, bool) {
    let len = header & !COMPRESSION_BIT;
    let compressed = header & COMPRESSION_BIT == COMPRESSION_BIT;
    (len, compressed)
}

// read a frame from a stream
pub async fn read_frame<S>(stream: &mut S, buf: &mut BytesMut) -> Result<(), KvError>
    where
        S: AsyncRead + Unpin + Send,
{
    // read 4 bytes length
    let mut header = [0; LENGTH_BYTES];
    stream.read_exact(&mut header).await?;
    let header = u32::from_be_bytes(header) as usize;
    let (len, _compressed) = decode_header(header);

    buf.reserve(LENGTH_BYTES + len);
    buf.put_u32(header as u32);
    // unsafe is because from current position too position + len is not initialized
    // but we have reserved enough space, and after reading from the stream, the space will be initialized
    // so it is safe
    unsafe {
        buf.advance_mut(len);
    }
    stream.read_exact(&mut buf[LENGTH_BYTES..]).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::utils::DummyStream;
    use crate::Value;

    use super::*;

    #[tokio::test]
    async fn read_frame_should_work() {
        let mut buf = BytesMut::new();
        let request = CommandRequest::new_hdel("table", "key");
        request.encode_frame(&mut buf).unwrap();
        let mut stream = DummyStream { buf };

        let mut data = BytesMut::new();
        read_frame(&mut stream, &mut data).await.unwrap();

        let request2 = CommandRequest::decode_frame(&mut data).unwrap();
        assert_eq!(request, request2);
    }

    #[test]
    fn command_request_encode_decode_should_work() {
        let mut buf = BytesMut::new();

        let request = CommandRequest::new_hdel("table", "key");
        request.encode_frame(&mut buf).unwrap();

        assert_eq!(is_compressed(&buf), false);

        let request2 = CommandRequest::decode_frame(&mut buf).unwrap();
        assert_eq!(request, request2);
    }

    #[test]
    fn command_response_encode_decode_should_work() {
        let mut buf = BytesMut::new();

        let values: Vec<Value> = vec![1.into(), "hello".into(), b"data".into()];
        let response: CommandResponse = values.into();
        response.encode_frame(&mut buf).unwrap();

        assert_eq!(is_compressed(&buf), false);

        let response2 = CommandResponse::decode_frame(&mut buf).unwrap();
        assert_eq!(response, response2);
    }

    #[test]
    fn command_response_compressed_encode_decode_should_work() {
        let mut buf = BytesMut::new();

        let value: Value = Bytes::from(vec![0u8; COMPRESSION_THRESHOLD + 1]).into();
        let response: CommandResponse = value.into();
        response.encode_frame(&mut buf).unwrap();

        assert_eq!(is_compressed(&buf), true);

        let response2 = CommandResponse::decode_frame(&mut buf).unwrap();
        assert_eq!(response, response2);
    }

    fn is_compressed(buf: &BytesMut) -> bool {
        if let &[v] = &buf[..1] {
            v >> 7 == 1
        } else {
            false
        }
    }
}