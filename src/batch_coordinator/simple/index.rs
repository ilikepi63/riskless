use bytes::{BufMut, BytesMut};
use uuid::Uuid;

use crate::error::RisklessError;

#[derive(Debug, Clone)]
pub struct Index {
    pub object_key: Uuid,
    pub offset: u64,
    pub size: u32,
}

impl Index {
    pub fn new(object_key: uuid::Uuid, offset: u64, size: u32) -> Self {
        Self {
            object_key,
            offset,
            size,
        }
    }

    #[inline]
    pub const fn packed_size() -> usize {
        16 + std::mem::size_of::<u64>() + std::mem::size_of::<u32>()
    }

    pub fn write(self, buf: &mut BytesMut) {
        buf.put_slice(self.object_key.as_bytes());

        buf.put_u64(self.offset);
        buf.put_u32(self.size);
    }
}

impl TryFrom<&[u8]> for Index {
    type Error = RisklessError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() < Self::packed_size() {
            return Err(RisklessError::Unknown); // TODO make an error for this.; 
        }

        let object_key = Uuid::from_slice(&value[0..16])?;

        let offset = u64::from_be_bytes(value[16..24].try_into()?);

        let size = u32::from_be_bytes(value[24..28].try_into()?);

        Ok(Self {
            object_key,
            offset,
            size,
        })
    }
}

impl From<Index> for BytesMut {
    fn from(val: Index) -> Self {
        let mut bytes = BytesMut::with_capacity(Index::packed_size());

        val.write(&mut bytes);

        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use uuid::Uuid;

    #[test]
    fn test_index_new() {
        let uuid = Uuid::new_v4();
        let offset = 12345;
        let size = 67890;

        let index = Index::new(uuid, offset, size);

        assert_eq!(index.object_key, uuid);
        assert_eq!(index.offset, offset);
        assert_eq!(index.size, size);
    }

    #[test]
    fn test_index_write() {
        let uuid = Uuid::new_v4();
        let offset = 12345;
        let size = 67890;

        let index = Index::new(uuid, offset, size);
        let mut buf = BytesMut::new();
        index.write(&mut buf);

        // Check the written bytes
        assert_eq!(buf.len(), 16 + 8 + 4); // UUID + u64 + u32
        assert_eq!(&buf[0..16], uuid.as_bytes());
        assert_eq!(
            u64::from_be_bytes(buf[16..24].try_into().expect("")),
            offset
        );
        assert_eq!(u32::from_be_bytes(buf[24..28].try_into().expect("")), size);
    }

    #[test]
    fn test_try_from_slice_success() {
        let uuid: Uuid = Uuid::new_v4();
        let offset: u64 = 12345;
        let size: u32 = 67890;

        let mut bytes = Vec::new();
        bytes.extend_from_slice(uuid.as_bytes());
        bytes.extend_from_slice(&offset.to_be_bytes());
        bytes.extend_from_slice(&size.to_be_bytes());

        let index = Index::try_from(bytes.as_slice()).expect("");

        assert_eq!(index.object_key, uuid);
        assert_eq!(index.offset, offset);
        assert_eq!(index.size, size);
    }

    #[test]
    fn test_try_from_slice_too_short() {
        let bytes = [0u8; 15]; // Less than needed (16 + 8 + 4 = 28 bytes)

        let result = Index::try_from(bytes.as_slice());

        assert!(result.is_err());
    }

    #[test]
    fn test_round_trip() {
        let uuid: Uuid = Uuid::new_v4();
        let offset: u64 = 12345;
        let size: u32 = 67890;

        let original = Index::new(uuid, offset, size);

        let original_clone = original.clone();

        let mut buf = BytesMut::new();
        original_clone.write(&mut buf);

        let reconstructed = Index::try_from(buf.as_ref()).expect("");

        assert_eq!(reconstructed.object_key, original.object_key);
        assert_eq!(reconstructed.offset, original.offset);
        assert_eq!(reconstructed.size, original.size);
    }
}
