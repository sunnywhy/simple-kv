use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::BytesMut;
use futures::{ready, Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite};
use crate::{FrameCoder, KvError};
use crate::network::frame::read_frame;

/// stream that handles KV server prost frame
pub struct ProstStream<S, In, Out> {
    // inner stream
    stream: S,
    // write buffer
    write_buf: BytesMut,
    // how many bytes have been written
    written: usize,
    // read buffer
    read_buf: BytesMut,

    _in: PhantomData<In>,
    _out: PhantomData<Out>,
}

impl<S, In, Out> Stream for ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    In: FrameCoder + Unpin + Send,
    Out: Unpin + Send,
{
    // when calling next(), return Result<In, KvError>
    type Item = Result<In, KvError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // after last time calling poll_next(), the read_buf should be empty
        assert!(self.read_buf.is_empty());

        // get rest from the read_buf, separate the buffer from self
        let mut rest = self.read_buf.split_off(0);

        // read a frame from the stream
        let fut = read_frame(&mut self.stream, &mut rest);
        ready!(fut.poll_unpin(cx))?;

        // get data, merge the buffer
        self.read_buf.unsplit(rest);

        Poll::Ready(Some(In::decode_frame(&mut self.read_buf)))
    }
}

// when calling send(), will send Out to the stream
impl<S, In, Out> Sink<Out> for ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    In: Unpin + Send,
    Out: FrameCoder + Unpin + Send,
{
    // if send() failed, return KvError
    type Error = KvError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Out) -> Result<(), Self::Error> {
        let this = self.get_mut();
        item.encode_frame(&mut this.write_buf)?;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();

        while this.written != this.write_buf.len() {
            let n = ready!(Pin::new(&mut this.stream).poll_write(cx, &this.write_buf[this.written..]))?;
            this.written += n;
        }

        // after flush, reset written to 0
        this.write_buf.clear();
        this.written = 0;

        ready!(Pin::new(&mut this.stream).poll_flush(cx))?;
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        ready!(this.poll_flush(cx))?;

        ready!(Pin::new(&mut this.stream).poll_shutdown(cx))?;
        Poll::Ready(Ok(()))
    }
}

impl<S, In, Out> ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            write_buf: BytesMut::new(),
            written: 0,
            read_buf: BytesMut::new(),
            _in: PhantomData::default(),
            _out: PhantomData::default(),
        }
    }
}

// in general, our ProstStream is Unpin
impl<S, In, Out> Unpin for ProstStream<S, In, Out> where S: Unpin {}