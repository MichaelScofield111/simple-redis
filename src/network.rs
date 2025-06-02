use crate::{
    Backend, RespDecode, RespEncode, RespError, RespFrame,
    cmd::{Command, CommandExecutor},
};
use anyhow::Result;
use bytes::BytesMut;
use futures::SinkExt;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Encoder, Framed};
use tracing::info;

#[derive(Debug)]
struct RespFrameCodec;

#[derive(Debug)]
struct RedisRequset {
    frame: RespFrame,
    backend: Backend,
}

#[derive(Debug)]
struct RedisResponse {
    frame: RespFrame,
}

pub async fn stream_handle(stream: TcpStream, backend: Backend) -> Result<()> {
    // how to oget a frame from stream
    let mut framed = Framed::new(stream, RespFrameCodec);
    loop {
        match framed.next().await {
            Some(Ok(frame)) => {
                info!("Received frame: {:?}", frame);
                let request = RedisRequset {
                    frame,
                    backend: backend.clone(),
                };
                let response = request_handle(request).await?;
                info!("Send response: {:?}", response);
                let _ = framed.send(response.frame).await;
            }
            Some(Err(e)) => return Err(e),
            None => return Ok(()),
            // parse request
            // call request_handle with the frame
            // send the response back to the stream
        }
    }
}
async fn request_handle(request: RedisRequset) -> Result<RedisResponse> {
    let (frame, backend) = (request.frame, request.backend);
    let cmd = Command::try_from(frame)?;
    info!("execute command: {:?}", cmd);
    let response = cmd.execute(&backend);
    Ok(RedisResponse { frame: response })
}

impl Encoder<RespFrame> for RespFrameCodec {
    type Error = anyhow::Error;

    fn encode(&mut self, item: RespFrame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded = item.encode();
        dst.extend_from_slice(&encoded);
        Ok(())
    }
}

impl Decoder for RespFrameCodec {
    type Item = RespFrame;
    type Error = anyhow::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<RespFrame>> {
        match RespFrame::decode(src) {
            Ok(frame) => Ok(Some(frame)),
            Err(RespError::NotComplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}
