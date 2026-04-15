use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use lz4::block;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader as TokioBufReader};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;

const LZ4_FRAME_MAGIC: [u8; 4] = [0x04, 0x22, 0x4D, 0x18];

#[derive(Clone, Copy, Debug)]
pub(crate) struct ParallelLz4ReaderConfig {
    pub workers: usize,
    pub io_buffer_bytes: usize,
}

#[derive(Debug)]
struct DecodedBlock {
    seq: u64,
    bytes: Vec<u8>,
}

pub(crate) struct ParallelLz4LineReader {
    rx: mpsc::Receiver<Result<DecodedBlock>>,
    producer: JoinHandle<()>,
    pending: BTreeMap<u64, Vec<u8>>,
    next_seq: u64,
    current_block: Option<Vec<u8>>,
    current_offset: usize,
    stream_exhausted: bool,
}

impl ParallelLz4LineReader {
    pub(crate) fn new<R>(reader: R, config: ParallelLz4ReaderConfig) -> Self
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        let channel_capacity = config.workers.max(1).saturating_mul(2).max(2);
        let (tx, rx) = mpsc::channel(channel_capacity);
        let producer = tokio::spawn(async move {
            if let Err(err) = produce_decoded_blocks(reader, config, tx.clone()).await {
                drop(tx.send(Err(err)).await);
            }
        });

        Self {
            rx,
            producer,
            pending: BTreeMap::new(),
            next_seq: 0,
            current_block: None,
            current_offset: 0,
            stream_exhausted: false,
        }
    }

    pub(crate) async fn next_line_into(&mut self, line: &mut Vec<u8>) -> Result<bool> {
        line.clear();
        loop {
            if let Some(block) = self.current_block.as_ref() {
                if self.current_offset >= block.len() {
                    self.current_block = None;
                    self.current_offset = 0;
                    continue;
                }

                if let Some(rel_newline_idx) = block[self.current_offset..].iter().position(|&byte| byte == b'\n') {
                    let end = self.current_offset + rel_newline_idx + 1;
                    line.extend_from_slice(&block[self.current_offset..end]);
                    self.current_offset = end;
                    if self.current_offset >= block.len() {
                        self.current_block = None;
                        self.current_offset = 0;
                    }
                    return Ok(true);
                }

                line.extend_from_slice(&block[self.current_offset..]);
                self.current_block = None;
                self.current_offset = 0;
                continue;
            }

            if let Some(next_block) = self.pending.remove(&self.next_seq) {
                self.next_seq = self.next_seq.saturating_add(1);
                self.current_block = Some(next_block);
                self.current_offset = 0;
                continue;
            }

            if self.stream_exhausted {
                return Ok(!line.is_empty());
            }

            match self.rx.recv().await {
                Some(Ok(decoded)) => {
                    self.pending.insert(decoded.seq, decoded.bytes);
                }
                Some(Err(err)) => return Err(err),
                None => self.stream_exhausted = true,
            }
        }
    }
}

impl Drop for ParallelLz4LineReader {
    fn drop(&mut self) {
        self.producer.abort();
    }
}

async fn produce_decoded_blocks<R>(
    reader: R,
    config: ParallelLz4ReaderConfig,
    tx: mpsc::Sender<Result<DecodedBlock>>,
) -> Result<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let mut reader = TokioBufReader::with_capacity(config.io_buffer_bytes, reader);
    let workers = config.workers.max(1);
    let semaphore = Arc::new(Semaphore::new(workers));
    let mut joins: Vec<JoinHandle<()>> = Vec::new();
    let mut seq = 0_u64;

    loop {
        let mut magic = [0_u8; 4];
        if !read_exact_or_eof(&mut reader, &mut magic).await? {
            break;
        }
        if magic != LZ4_FRAME_MAGIC {
            bail!("unsupported lz4 frame magic: {:02x?}", magic);
        }

        let mut descriptor = [0_u8; 2];
        read_exact_required(&mut reader, &mut descriptor, "reading lz4 FLG/BD").await?;
        let flg = descriptor[0];
        let bd = descriptor[1];

        let version = (flg >> 6) & 0x03;
        if version != 0x01 {
            bail!("unsupported lz4 frame version bits={version}");
        }

        let block_independent = ((flg >> 5) & 1) == 1;
        let block_checksum = ((flg >> 4) & 1) == 1;
        let content_size_present = ((flg >> 3) & 1) == 1;
        let content_checksum_present = ((flg >> 2) & 1) == 1;
        let dict_id_present = (flg & 1) == 1;
        let max_block_size = decode_block_max_size(bd)?;

        if !block_independent {
            bail!("lz4 frame has dependent blocks; parallel block decode requires independent blocks");
        }

        let mut descriptor_tail =
            vec![0_u8; 1 + if content_size_present { 8 } else { 0 } + if dict_id_present { 4 } else { 0 }];
        read_exact_required(&mut reader, &mut descriptor_tail, "reading lz4 descriptor tail").await?;

        loop {
            let mut block_header = [0_u8; 4];
            read_exact_required(&mut reader, &mut block_header, "reading lz4 block header").await?;
            let block_descriptor = u32::from_le_bytes(block_header);
            if block_descriptor == 0 {
                if content_checksum_present {
                    let mut checksum = [0_u8; 4];
                    read_exact_required(&mut reader, &mut checksum, "reading lz4 content checksum").await?;
                }
                break;
            }

            let is_raw = (block_descriptor & 0x8000_0000) != 0;
            let payload_size = (block_descriptor & 0x7FFF_FFFF) as usize;
            if payload_size > max_block_size {
                bail!("lz4 block payload size {} exceeds max block size {}", payload_size, max_block_size);
            }

            let mut payload = vec![0_u8; payload_size];
            if payload_size > 0 {
                read_exact_required(&mut reader, &mut payload, "reading lz4 block payload").await?;
            }

            if block_checksum {
                let mut checksum = [0_u8; 4];
                read_exact_required(&mut reader, &mut checksum, "reading lz4 block checksum").await?;
            }

            let current_seq = seq;
            seq = seq.saturating_add(1);
            let permit = semaphore.clone().acquire_owned().await.context("acquiring lz4 decode worker permit")?;
            let tx_clone = tx.clone();

            joins.push(tokio::spawn(async move {
                let decoded =
                    tokio::task::spawn_blocking(move || decode_lz4_block(payload, is_raw, max_block_size)).await;

                let send_result = match decoded {
                    Ok(Ok(bytes)) => tx_clone.send(Ok(DecodedBlock { seq: current_seq, bytes })).await,
                    Ok(Err(err)) => tx_clone.send(Err(err)).await,
                    Err(join_err) => {
                        tx_clone.send(Err(anyhow!("parallel lz4 decode worker join failed: {join_err}"))).await
                    }
                };

                drop(permit);
                drop(send_result);
            }));
        }
    }

    for join in joins {
        if let Err(err) = join.await {
            bail!("parallel lz4 decode task failed: {err}");
        }
    }

    Ok(())
}

fn decode_lz4_block(payload: Vec<u8>, is_raw: bool, max_block_size: usize) -> Result<Vec<u8>> {
    if is_raw {
        return Ok(payload);
    }

    let mut out = vec![0_u8; max_block_size];
    let decoded_size = block::decompress_to_buffer(&payload, Some(max_block_size as i32), &mut out)
        .context("lz4 block decompression failed")?;
    out.truncate(decoded_size);
    Ok(out)
}

fn decode_block_max_size(bd: u8) -> Result<usize> {
    match (bd >> 4) & 0x07 {
        4 => Ok(64 * 1024),
        5 => Ok(256 * 1024),
        6 => Ok(1024 * 1024),
        7 => Ok(4 * 1024 * 1024),
        code => bail!("unsupported lz4 block max size code: {code}"),
    }
}

async fn read_exact_or_eof<R>(reader: &mut R, buf: &mut [u8]) -> Result<bool>
where
    R: AsyncRead + Unpin,
{
    let mut offset = 0_usize;
    while offset < buf.len() {
        let read = reader.read(&mut buf[offset..]).await?;
        if read == 0 {
            if offset == 0 {
                return Ok(false);
            }
            bail!("unexpected EOF while reading {} bytes", buf.len());
        }
        offset += read;
    }
    Ok(true)
}

async fn read_exact_required<R>(reader: &mut R, buf: &mut [u8], context: &str) -> Result<()>
where
    R: AsyncRead + Unpin,
{
    if read_exact_or_eof(reader, buf).await? { Ok(()) } else { bail!("unexpected EOF while {context}") }
}
